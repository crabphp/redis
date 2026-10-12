<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Engine\Async\Async;
use Crab\Engine\EventLoop;
use Crab\Engine\Process\Worker;
use Crab\Redis\EngineClient;
use Crab\Redis\RedisException;
use Crab\Redis\Resp;
use Crab\Testing\WorkerHarness;
use PHPUnit\Framework\Attributes\DataProvider;

final class EngineClientTest extends RedisTestCase
{
    public function testCloseCancelsTheOpenerAndItsWaitersWithoutDisturbingANewConnection(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $redis = new EngineClient($this->url);
            $before = $worker->loop->getIdentifiers();
            $first = $worker->spawn(fn () => $this->timedCommand($redis, 'PING'));
            $second = $worker->spawn(fn () => $this->timedCommand($redis, 'PING'));
            $replacement = $worker->spawn(static function () use ($redis): mixed {
                $redis->close();
                // Begin again before the cancelled opener has resumed its cleanup.
                return $redis->command('PING');
            });
            try {
                self::assertInstanceOf(RedisException::class, $first->await()[0]);
                self::assertInstanceOf(RedisException::class, $second->await()[0]);
                self::assertSame('PONG', $replacement->await());
                $redis->close();
                $redis->close();
                Async::sleep($worker->loop, 0);
                self::assertSame($before, $worker->loop->getIdentifiers());
                // A reader left on a closed stream used to crash this loop turn.
                Async::sleep($worker->loop, 0.01);
            } finally {
                $redis->close();
            }
        });
    }

    public function testASilentServerFailsTheCommandAfterItsReplyTimeout(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            [$url, $close] = $this->server($worker->loop, static fn () => null);
            try {
                [$reply, $elapsed] = $this->timedCommand(new EngineClient($url, replyTimeout: 0.2), 'PING');

                self::assertInstanceOf(RedisException::class, $reply);
                self::assertSame('Redis did not answer PING within 0.2 seconds', $reply->getMessage());
                self::assertGreaterThanOrEqual(0.18, $elapsed);
                self::assertLessThan(5.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    public function testOneReplyInAPipelineSucceedsAndTheOthersFailTogether(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            [$url, $close] = $this->server($worker->loop, static fn (int $connection, int $command): ?array => $command === 1 ? [0.0, "+PONG\r\n"] : null);
            $redis = new EngineClient($url, replyTimeout: 0.2);
            try {
                $tasks = [
                    $worker->spawn(fn () => $this->timedCommand($redis, 'PING')),
                    $worker->spawn(fn () => $this->timedCommand($redis, 'GET', 'missing')),
                    $worker->spawn(fn () => $this->timedCommand($redis, 'INCR', 'counter')),
                ];
                $replies = array_map(static fn ($task): array => $task->await(), $tasks);

                self::assertSame('PONG', $replies[0][0]);
                foreach ([$replies[1], $replies[2]] as [$error, $elapsed]) {
                    self::assertInstanceOf(RedisException::class, $error);
                    self::assertSame('Redis did not answer GET within 0.2 seconds', $error->getMessage());
                    self::assertGreaterThanOrEqual(0.18, $elapsed);
                    self::assertLessThan(5.0, $elapsed);
                }
                self::assertSame($replies[1][0]->getMessage(), $replies[2][0]->getMessage());
            } finally {
                $close();
            }
        });
    }

    public function testACommandAfterATimeoutConnectsAgain(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $connections = [];
            [$url, $close] = $this->server($worker->loop, static function (int $connection) use (&$connections): ?array {
                $connections[] = $connection;
                return $connection === 1 ? null : [0.0, "+PONG\r\n"];
            });
            $redis = new EngineClient($url, replyTimeout: 0.2);
            try {
                self::assertInstanceOf(RedisException::class, $this->timedCommand($redis, 'PING')[0]);
                self::assertSame('PONG', $redis->command('PING'));
                // A timer left by either command must not close the new connection.
                Async::sleep($worker->loop, 0.4);
                self::assertSame('PONG', $redis->command('PING'));
                self::assertSame([1, 2, 2], $connections);
            } finally {
                $close();
            }
        });
    }

    public function testASlowReplyWithinTheDeadlineSucceeds(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $received = $worker->loop->getSuspension();
            $stream = null;
            [$url, $close] = $this->server($worker->loop, static function (int $connection, int $command, array $words, mixed $socket) use ($received, &$stream): ?array {
                $stream = $socket;
                $received->resume();
                return null;
            });
            try {
                $task = $worker->spawn(fn () => $this->timedCommand(new EngineClient($url, replyTimeout: 5.0), 'PING'));
                $received->suspend();
                self::assertFalse($task->isComplete());
                // The pause begins after the server has received the command.
                Async::sleep($worker->loop, 0.1);
                fwrite($stream, "+PONG\r\n");
                [$reply, $elapsed] = $task->await();

                self::assertSame('PONG', $reply);
                self::assertLessThan(10.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    public function testASecondCommandsDeadlineStartsAtItsWriteRatherThanTheFirstReply(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $received = $worker->loop->getSuspension();
            $stream = null;
            [$url, $close] = $this->server($worker->loop, static function (int $connection, int $command, array $words, mixed $socket) use ($received, &$stream): ?array {
                $stream = $socket;
                $received->resume($words[0]);
                return null;
            });
            $redis = new EngineClient($url, replyTimeout: 5.0);
            try {
                $first = $worker->spawn(static fn () => $redis->command('PING'));
                self::assertSame('PING', $received->suspend());
                $second = $worker->spawn(fn () => $this->timedCommand($redis, 'GET', 'missing'));
                self::assertSame('GET', $received->suspend());
                // Neither reply exists yet, but both fully written commands have deadlines.
                $waiting = (new \ReflectionProperty($redis, 'waiting'))->getValue($redis);
                self::assertSame(['PING', 'GET'], array_column($waiting, 'name'));
                self::assertNotNull($waiting[0]['deadline']);
                self::assertNotNull($waiting[1]['deadline']);
                fwrite($stream, "+PONG\r\n");

                self::assertSame('PONG', $first->await());
                [$reply, $elapsed] = $second->await();
                self::assertInstanceOf(RedisException::class, $reply);
                self::assertSame('Redis did not answer GET within 5 seconds', $reply->getMessage());
                self::assertLessThan(10.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    public function testAnEqualDeadlineNamesTheGetStillWaitingAfterTheLargeSetWasAnswered(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $received = $worker->loop->getSuspension();
            $stream = null;
            $commands = [];
            [$url, $close] = $this->server($worker->loop, static function (int $connection, int $command, array $words, mixed $socket) use ($received, &$stream, &$commands): ?array {
                $commands[] = $words[0];
                if ($words[0] === 'GET') {
                    $stream = $socket;
                    $received->resume();
                }
                return null;
            });
            $redis = new EngineClient($url, replyTimeout: 5.0);
            try {
                // Backpressure keeps GET queued behind SET until a write finishes both.
                $set = $worker->spawn(fn () => $this->timedCommand($redis, 'SET', 'large', str_repeat('x', 32 * 1024 * 1024)));
                $get = $worker->spawn(fn () => $this->timedCommand($redis, 'GET', 'large'));
                $received->suspend();
                self::assertSame(['SET', 'GET'], $commands);
                $waiting = (new \ReflectionProperty($redis, 'waiting'))->getValue($redis);
                self::assertSame(['SET', 'GET'], array_column($waiting, 'name'));
                self::assertNotNull($waiting[0]['deadline']);
                self::assertSame($waiting[0]['deadline'], $waiting[1]['deadline']);
                fwrite($stream, "+OK\r\n");

                self::assertSame('OK', $set->await()[0]);
                [$reply, $elapsed] = $get->await();
                self::assertInstanceOf(RedisException::class, $reply);
                self::assertSame('Redis did not answer GET within 5 seconds', $reply->getMessage());
                self::assertLessThan(10.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    /**
     * A word that looks like a block of zero must not disable the
     * ordinary deadline unless it stands where Redis reads the time and
     * is written the way Redis accepts.
     *
     * @return iterable<string, array{list<string|int|float>}>
     */
    public static function malformedBlockingCommands(): iterable
    {
        yield 'BLPOP with only a numeric key' => [['BLPOP', '0']];
        yield 'BRPOP with only a numeric key' => [['BRPOP', '0']];
        yield 'BZPOPMIN with only a numeric key' => [['BZPOPMIN', '0']];
        yield 'BZPOPMAX with only a numeric key' => [['BZPOPMAX', '0']];
        yield 'BRPOPLPUSH missing a timeout' => [['BRPOPLPUSH', 'source', '0']];
        yield 'BRPOPLPUSH with an extra argument' => [['BRPOPLPUSH', 'source', 'destination', 1, 0]];
        yield 'BLMOVE missing a timeout' => [['BLMOVE', 'source', 'destination', 'LEFT', 0]];
        yield 'BLPOP with a padded zero' => [['BLPOP', 'empty', ' 0 ']];
        yield 'BLPOP with a negative time' => [['BLPOP', 'empty', '-1']];
        yield 'XREAD with a padded zero' => [['XREAD', 'BLOCK', ' 0 ', 'STREAMS', 'empty', '$']];
        yield 'XREAD with a signed zero' => [['XREAD', 'BLOCK', '+0', 'STREAMS', 'empty', '$']];
        yield 'XREAD with a fraction of a millisecond' => [['XREAD', 'BLOCK', '0.0', 'STREAMS', 'empty', '$']];
        yield 'XREAD of a stream named BLOCK' => [['XREAD', 'STREAMS', 'BLOCK', '0']];
        yield 'XREADGROUP of a group named BLOCK' => [['XREADGROUP', 'GROUP', 'BLOCK', '0', 'STREAMS', 'empty', '>']];
        yield 'XREADGROUP of a group named BLOCK after COUNT' => [['XREADGROUP', 'COUNT', 1, 'GROUP', 'BLOCK', '0', 'STREAMS', 'empty', '>']];
        yield 'XREAD of a count named BLOCK' => [['XREAD', 'COUNT', 'BLOCK', '0', 'STREAMS', 'empty', '$']];
    }

    /**
     * @param list<string|int|float> $words
     */
    #[DataProvider('malformedBlockingCommands')]
    public function testAMalformedBlockingCommandKeepsTheOrdinaryReplyDeadline(array $words): void
    {
        WorkerHarness::run([], function (Worker $worker) use ($words): void {
            $received = $worker->loop->getSuspension();
            [$url, $close] = $this->server($worker->loop, static function () use ($received): ?array {
                $received->resume();
                return null;
            });
            $name = (string) array_shift($words);
            // Bounds the regression too: an erroneously indefinite command cannot hang the suite.
            $failsafe = $worker->loop->delay(5.0, $close);
            try {
                $task = $worker->spawn(fn () => $this->timedCommand(new EngineClient($url, replyTimeout: 0.1), $name, ...$words));
                $received->suspend();
                [$reply, $elapsed] = $task->await();

                self::assertInstanceOf(RedisException::class, $reply);
                self::assertSame("Redis did not answer {$name} within 0.1 seconds", $reply->getMessage());
                self::assertLessThan(10.0, $elapsed);
            } finally {
                $worker->loop->cancel($failsafe);
                $close();
            }
        });
    }

    public function testABlockingCommandAddsItsOwnTimeoutToTheReplyTimeout(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            [$url, $close] = $this->server($worker->loop, static fn () => null);
            try {
                [$reply, $elapsed] = $this->timedCommand(new EngineClient($url, replyTimeout: 0.2), 'BLPOP', 'empty', 1);

                self::assertInstanceOf(RedisException::class, $reply);
                self::assertSame('Redis did not answer BLPOP within 1.2 seconds', $reply->getMessage());
                self::assertGreaterThanOrEqual(1.18, $elapsed);
                self::assertLessThan(5.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    /**
     * The list commands give seconds; stream commands give milliseconds.
     *
     * @return iterable<string, array{list<string|int|float>}>
     */
    public static function blockingCommands(): iterable
    {
        yield 'BLPOP' => [['BLPOP', 'empty', 0.1]];
        yield 'BRPOP' => [['BRPOP', 'empty', 0.1]];
        yield 'BRPOPLPUSH' => [['BRPOPLPUSH', 'source', 'destination', 0.1]];
        yield 'BLMOVE' => [['BLMOVE', 'source', 'destination', 'LEFT', 'RIGHT', 0.1]];
        yield 'BZPOPMIN' => [['BZPOPMIN', 'empty', 0.1]];
        yield 'BZPOPMAX' => [['BZPOPMAX', 'empty', 0.1]];
        yield 'XREAD' => [['xread', 'COUNT', 1, 'block', 100, 'STREAMS', 'empty', '$']];
        yield 'XREADGROUP' => [['XREADGROUP', 'GROUP', 'group', 'consumer', 'COUNT', 1, 'BLOCK', 100, 'NOACK', 'STREAMS', 'empty', '>']];
    }

    /**
     * @param list<string|int|float> $words
     */
    #[DataProvider('blockingCommands')]
    public function testEachSupportedBlockingCommandAddsItsBlockingTime(array $words): void
    {
        WorkerHarness::run([], function (Worker $worker) use ($words): void {
            [$url, $close] = $this->server($worker->loop, static fn () => null);
            $name = (string) array_shift($words);
            try {
                [$reply, $elapsed] = $this->timedCommand(new EngineClient($url, replyTimeout: 0.1), $name, ...$words);

                self::assertInstanceOf(RedisException::class, $reply);
                self::assertSame('Redis did not answer ' . strtoupper($name) . ' within 0.2 seconds', $reply->getMessage());
                self::assertGreaterThanOrEqual(0.18, $elapsed);
                self::assertLessThan(5.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    /**
     * @param list<string|int|float> $words
     */
    #[DataProvider('blockingCommands')]
    public function testABlockOfZeroKeepsWaitingBeyondTheReplyTimeout(array $words): void
    {
        WorkerHarness::run([], function (Worker $worker) use ($words): void {
            $received = $worker->loop->getSuspension();
            [$url, $close] = $this->server($worker->loop, static function () use ($received): ?array {
                $received->resume();
                return null;
            });
            $redis = new EngineClient($url, replyTimeout: 0.1);
            $name = (string) array_shift($words);
            foreach ($words as &$word) {
                if ($word === 0.1 || $word === 100) {
                    $word = 0;
                }
            }
            unset($word);
            try {
                $task = $worker->spawn(fn () => $this->timedCommand($redis, $name, ...$words));
                $received->suspend();
                Async::sleep($worker->loop, 0.25);

                self::assertFalse($task->isComplete());
                $close();
                [$reply] = $task->await();
                self::assertInstanceOf(RedisException::class, $reply);
                self::assertStringContainsString('the server closed the connection', $reply->getMessage());
            } finally {
                $close();
            }
        });
    }

    public function testAFiniteCommandBehindAnIndefiniteBlockStillHasADeadline(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $received = $worker->loop->getSuspension();
            [$url, $close] = $this->server($worker->loop, static function (int $connection, int $command) use ($received): ?array {
                if ($command === 1) {
                    $received->resume();
                }
                return null;
            });
            $redis = new EngineClient($url, replyTimeout: 0.2);
            try {
                $blocked = $worker->spawn(fn () => $this->timedCommand($redis, 'BLPOP', 'empty', 0));
                $received->suspend();
                [$reply, $elapsed] = $this->timedCommand($redis, 'PING');
                [$blockingReply] = $blocked->await();

                self::assertInstanceOf(RedisException::class, $reply);
                self::assertInstanceOf(RedisException::class, $blockingReply);
                self::assertSame('Redis did not answer PING within 0.2 seconds', $reply->getMessage());
                self::assertSame($reply->getMessage(), $blockingReply->getMessage());
                self::assertGreaterThanOrEqual(0.18, $elapsed);
                self::assertLessThan(5.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    /**
     * These names are data, not a BLOCK option.
     *
     * @return iterable<string, array{list<string|int|float>}>
     */
    public static function nonBlockingStreamCommands(): iterable
    {
        yield 'stream names' => [['XREAD', 'STREAMS', 'BLOCK', '0', '$', '$']];
        yield 'group and consumer names' => [['XREADGROUP', 'GROUP', 'BLOCK', 0, 'STREAMS', 'empty', '>']];
        yield 'ordinary stream read' => [['XREAD', 'COUNT', 1, 'STREAMS', 'empty', '$']];
    }

    /**
     * @param list<string|int|float> $words
     */
    #[DataProvider('nonBlockingStreamCommands')]
    public function testAStreamReadWithoutABlockKeepsTheOrdinaryReplyTimeout(array $words): void
    {
        WorkerHarness::run([], function (Worker $worker) use ($words): void {
            [$url, $close] = $this->server($worker->loop, static fn () => null);
            $name = (string) array_shift($words);
            try {
                [$reply, $elapsed] = $this->timedCommand(new EngineClient($url, replyTimeout: 0.2), $name, ...$words);

                self::assertInstanceOf(RedisException::class, $reply);
                self::assertSame("Redis did not answer {$name} within 0.2 seconds", $reply->getMessage());
                self::assertGreaterThanOrEqual(0.18, $elapsed);
                self::assertLessThan(5.0, $elapsed);
            } finally {
                $close();
            }
        });
    }

    public function testACommandOutsideAWorkerHasTheSameReplyDeadline(): void
    {
        $loop = EventLoop::current();
        [$url, $close] = $this->server($loop, static fn () => null);
        try {
            [$reply, $elapsed] = $this->timedCommand(new EngineClient($url, replyTimeout: 0.2), 'PING');

            self::assertInstanceOf(RedisException::class, $reply);
            self::assertSame('Redis did not answer PING within 0.2 seconds', $reply->getMessage());
            self::assertGreaterThanOrEqual(0.18, $elapsed);
            self::assertLessThan(5.0, $elapsed);
        } finally {
            $close();
        }
    }

    /**
     * @return iterable<string, array{float}>
     */
    public static function invalidReplyTimeouts(): iterable
    {
        yield 'zero' => [0.0];
        yield 'negative' => [-0.1];
        yield 'infinite' => [INF];
        yield 'not a number' => [NAN];
    }

    #[DataProvider('invalidReplyTimeouts')]
    public function testAReplyTimeoutMustBePositiveAndFinite(float $timeout): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Set replyTimeout to a positive, finite number');
        new EngineClient($this->url, replyTimeout: $timeout);
    }

    public function testAReplyIsAStringAnIntegerNullOrAListOfThose(): void
    {
        $key = $this->prefix . 'k';

        self::assertSame('OK', $this->redis->command('SET', $key, 'hello'));
        self::assertSame('hello', $this->redis->command('GET', $key));
        self::assertNull($this->redis->command('GET', $this->prefix . 'missing'));
        self::assertSame(1, $this->redis->command('INCR', $this->prefix . 'n'));
        self::assertSame(3, $this->redis->command('RPUSH', $this->prefix . 'list', 'a', 2, 3.5));
        self::assertSame(['a', '2', '3.5'], $this->redis->command('LRANGE', $this->prefix . 'list', 0, -1));
        self::assertSame([], $this->redis->command('LRANGE', $this->prefix . 'missing', 0, -1));
        self::assertSame(['hello', null], $this->redis->command('MGET', $key, $this->prefix . 'missing'));
        self::assertSame([1, ['two', [3, null]], 'four'], $this->redis->command('EVAL', "return {1, {'two', {3, false}}, 'four'}", 0));
        self::assertNull($this->redis->command('EVAL', 'return false', 0));
    }

    public function testAValueIsStoredByteForByteHoweverLarge(): void
    {
        // Line ends, a zero byte and bytes that are not text, well past what one read brings.
        $value = str_repeat("line\r\n\0\xff\xfe*3\r\n\$5\r\n", 20_000);

        $this->redis->command('SET', $this->prefix . 'big', $value);

        self::assertSame(strlen($value), $this->redis->command('STRLEN', $this->prefix . 'big'));
        self::assertSame($value, $this->redis->command('GET', $this->prefix . 'big'));
        self::assertSame('', $this->redis->command('GETRANGE', $this->prefix . 'big', 5, 4));
    }

    public function testCommandsOfOneWorkerShareAConnectionAndEachGetsItsOwnReply(): void
    {
        $redis = $this->redis;
        $prefix = $this->prefix;
        $seen = [];
        $clients = 0;
        $ticks = 0;

        WorkerHarness::run([], static function (Worker $worker) use ($redis, $prefix, &$seen, &$clients, &$ticks): void {
            $tasks = [];
            for ($i = 0; $i < 50; ++$i) {
                $tasks[] = $worker->spawn(static fn (): array => [$redis->command('INCR', $prefix . 'counter'), $redis->command('CLIENT', 'ID')]);
            }
            $replies = array_map(static fn ($task): array => $task->await(), $tasks);
            $seen = array_column($replies, 0);
            $clients = count(array_unique(array_column($replies, 1)));

            // A second and a half of waiting on Redis, during which the worker's own timer goes on.
            $timer = $worker->loop->repeat(0.05, static function () use (&$ticks): void {
                ++$ticks;
            });
            $redis->command('BLPOP', $prefix . 'empty', 1);
            $worker->loop->cancel($timer);
        });

        sort($seen);
        self::assertSame(range(1, 50), $seen);
        self::assertSame(1, $clients);
        self::assertGreaterThan(10, $ticks);
    }

    public function testAnErrorIsThrownToWhoSentTheCommandAndTheOnesAfterItStillGetTheirReplies(): void
    {
        $redis = $this->redis;
        $prefix = $this->prefix;
        $results = [];

        WorkerHarness::run([], static function (Worker $worker) use ($redis, $prefix, &$results): void {
            $redis->command('SET', $prefix . 'text', 'not a number');
            $tasks = [
                $worker->spawn(static fn () => $redis->command('GET', $prefix . 'text')),
                $worker->spawn(static function () use ($redis, $prefix): string {
                    try {
                        $redis->command('INCR', $prefix . 'text');
                        return 'no error';
                    } catch (RedisException $error) {
                        return $error->getMessage();
                    }
                }),
                $worker->spawn(static fn () => $redis->command('STRLEN', $prefix . 'text')),
            ];
            $results = array_map(static fn ($task) => $task->await(), $tasks);
        });

        self::assertSame('not a number', $results[0]);
        self::assertStringContainsString('Redis refused INCR: ERR value is not an integer', $results[1]);
        self::assertSame(12, $results[2]);
    }

    public function testAConnectionTheServerClosedIsMadeAgain(): void
    {
        $redis = $this->redis;
        $other = new EngineClient($this->url);
        $ids = [];

        WorkerHarness::run([], static function (Worker $worker) use ($redis, $other, &$ids): void {
            $ids[] = $redis->command('CLIENT', 'ID');
            $worker->spawn(static fn () => $other->command('CLIENT', 'KILL', 'ID', $ids[0]))->await();
            // A turn of the loop, in which the worker sees that the connection has gone.
            Async::sleep($worker->loop, 0.02);
            $ids[] = $redis->command('CLIENT', 'ID');
        });

        self::assertNotSame($ids[0], $ids[1]);
    }

    public function testAWriteTheSocketRefusesFailsWithThePackagesOwnException(): void
    {
        $redis = $this->redis;
        $ended = [];

        WorkerHarness::run([], static function (Worker $worker) use ($redis, &$ended): void {
            $redis->command('PING');
            // The connection is cut under the client, which has not had a turn of the loop to notice.
            $stream = (new \ReflectionProperty($redis, 'stream'))->getValue($redis);
            stream_socket_shutdown($stream, STREAM_SHUT_RDWR);
            try {
                $ended[] = $redis->command('PING');
            } catch (\Throwable $error) {
                $ended[] = $error::class . ': ' . $error->getMessage();
            }
            // And the next command connects afresh.
            $ended[] = $redis->command('PING');
        });

        self::assertStringStartsWith(RedisException::class . ': Redis command PING failed', $ended[0]);
        self::assertSame('PONG', $ended[1]);
    }

    public function testTheAddressSaysWhoAsksAndWhichDatabase(): void
    {
        $user = 'crab-test-' . bin2hex(random_bytes(4));
        $this->redis->command('ACL', 'SETUSER', $user, 'on', '>s3cret', '~*', '+@all');
        $address = "redis://{$user}:s3cret@" . parse_url($this->url, PHP_URL_HOST) . ':' . (parse_url($this->url, PHP_URL_PORT) ?? 6379);
        try {
            $named = new EngineClient($address . '/2');
            self::assertSame($user, $named->command('ACL', 'WHOAMI'));
            $named->command('SET', $this->prefix . 'in-two', 'yes');
            self::assertSame('yes', (new EngineClient($address . '?database=2'))->command('GET', $this->prefix . 'in-two'));
            self::assertNull($this->redis->command('GET', $this->prefix . 'in-two'));
            $named->command('DEL', $this->prefix . 'in-two');

            try {
                (new EngineClient(str_replace('s3cret', 'wrong', $address)))->command('PING');
                self::fail('A wrong password was accepted.');
            } catch (RedisException $error) {
                self::assertStringContainsString('Redis refused AUTH', $error->getMessage());
            }
        } finally {
            $this->redis->command('ACL', 'DELUSER', $user);
        }

        // A password with a percent sign in it, written encoded once, both ways it can be given.
        $this->redis->command('ACL', 'SETUSER', $user, 'on', '>a%2Fb c', '~*', '+@all');
        $host = parse_url($this->url, PHP_URL_HOST) . ':' . (parse_url($this->url, PHP_URL_PORT) ?? 6379);
        try {
            self::assertSame($user, (new EngineClient("redis://{$user}:a%252Fb%20c@{$host}"))->command('ACL', 'WHOAMI'));
            // The same password in the query string, which PHP has already decoded once.
            self::assertSame($user, (new EngineClient("redis://{$user}@{$host}?password=a%252Fb%20c"))->command('ACL', 'WHOAMI'));
        } finally {
            $this->redis->command('ACL', 'DELUSER', $user);
        }

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('redis://127.0.0.1:6379');
        new EngineClient('127.0.0.1:6379');
    }

    /**
     * Records the actual command result and time, including expected
     * failures, so timeout tests can distinguish a late reply from EOF.
     *
     * @return array{mixed, float}
     */
    private function timedCommand(EngineClient $redis, string $name, string|int|float ...$arguments): array
    {
        $start = hrtime(true);
        try {
            $reply = $redis->command($name, ...$arguments);
        } catch (RedisException $error) {
            $reply = $error;
        }
        return [$reply, (hrtime(true) - $start) / 1_000_000_000];
    }

    /**
     * A plain TCP server on a free port, with a chosen response to each
     * command. Null stays silent; a pair supplies a delay and wire reply.
     * Unreferenced server watchers let commands outside workers finish
     * their loop, and the returned closure releases every test resource.
     *
     * The callback also receives the accepted socket, so a test can
     * wait for a command before deciding when to send its reply.
     *
     * @param \Closure(int, int, list<string>, resource): ?array{float, string} $reply
     * @return array{string, \Closure(): void}
     */
    private function server(EventLoop $loop, \Closure $reply): array
    {
        $listener = stream_socket_server('tcp://127.0.0.1:0');
        self::assertIsResource($listener);
        stream_set_blocking($listener, false);
        $address = stream_socket_get_name($listener, false);
        $streams = [$listener];
        $watchers = [];
        $connections = 0;
        $accept = $loop->onReadable($listener, static function () use ($loop, $listener, $reply, &$streams, &$watchers, &$connections): void {
            $stream = stream_socket_accept($listener, 0);
            if ($stream === false) {
                return;
            }
            stream_set_blocking($stream, false);
            $streams[] = $stream;
            $connection = ++$connections;
            $received = '';
            $commands = 0;
            $reader = $loop->onReadable($stream, static function (string $reader) use ($loop, $stream, $reply, $connection, &$received, &$commands, &$watchers): void {
                $bytes = fread($stream, 65_536);
                if ($bytes === '' && feof($stream)) {
                    $loop->cancel($reader);
                    return;
                }
                $received .= $bytes;
                $offset = 0;
                while (true) {
                    $words = Resp::reply($received, $offset, $complete);
                    if (!$complete) {
                        break;
                    }
                    $response = $reply($connection, ++$commands, $words, $stream);
                    if ($response !== null) {
                        [$delay, $wire] = $response;
                        $timer = $loop->delay($delay, static function () use ($stream, $wire): void {
                            fwrite($stream, $wire);
                        });
                        $loop->unreference($timer);
                        $watchers[] = $timer;
                    }
                }
                $received = substr($received, $offset);
            });
            $loop->unreference($reader);
            $watchers[] = $reader;
        });
        $loop->unreference($accept);
        $watchers[] = $accept;
        $close = static function () use ($loop, &$streams, &$watchers): void {
            foreach ($watchers as $watcher) {
                $loop->cancel($watcher);
            }
            foreach ($streams as $stream) {
                fclose($stream);
            }
            $streams = $watchers = [];
        };
        return ['redis://' . $address, $close];
    }
}
