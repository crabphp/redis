<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Engine\Async\Async;
use Crab\Engine\Process\Worker;
use Crab\Redis\EngineClient;
use Crab\Redis\RedisException;
use Crab\Testing\WorkerHarness;

final class EngineClientTest extends RedisTestCase
{
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
}
