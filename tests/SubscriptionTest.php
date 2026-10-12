<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Engine\Async\Async;
use Crab\Engine\EventLoop;
use Crab\Engine\FailureReporter;
use Crab\Engine\Process\Worker;
use Crab\Redis\EngineClient;
use Crab\Redis\RedisException;
use Crab\Redis\Resp;
use Crab\Redis\Subscription;
use Crab\Testing\WorkerHarness;
use PHPUnit\Framework\Attributes\DataProvider;

final class SubscriptionTest extends RedisTestCase
{
    public function testPublishReachesSubscribersAndReturnsTheReceiverCount(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $first = $this->redis->subscribe($this->prefix . 'orders');
            $second = $this->redis->subscribe($this->prefix . 'orders');
            try {
                $waiting = $worker->spawn(fn () => $this->take($first, 1));
                Async::sleep($worker->loop, 0);
                self::assertFalse($waiting->isComplete());
                self::assertSame(2, $this->redis->publish($this->prefix . 'orders', 'ready'));
                self::assertSame([[$this->prefix . 'orders', 'ready']], $waiting->await());
                self::assertSame([[$this->prefix . 'orders', 'ready']], $this->take($second, 1));
                self::assertSame('PONG', $this->redis->command('PING'));
                self::assertSame(0, $this->redis->publish($this->prefix . 'missing', 'nobody'));
            } finally {
                $this->redis->close();
            }
        });
    }

    public function testChannelsCanBeAddedAndRemovedWhileTheConsumerWaits(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $a = $this->prefix . 'a';
            $b = $this->prefix . 'b';
            $subscription = $this->redis->subscribe($a, $a);
            try {
                $waiting = $worker->spawn(fn () => $this->take($subscription, 2));
                Async::sleep($worker->loop, 0);
                $subscription->add($b, $b);
                $subscription->remove($a, $this->prefix . 'unknown');
                self::assertSame(0, $this->redis->publish($a, 'removed'));
                self::assertSame(1, $this->redis->publish($b, 'first'));
                $subscription->remove($b);
                self::assertSame(0, $this->redis->publish($b, 'removed too'));
                $subscription->add($a);
                self::assertSame(1, $this->redis->publish($a, 'second'));
                self::assertSame([[$b, 'first'], [$a, 'second']], $waiting->await());
                self::assertSame(0, $subscription->dropped());
            } finally {
                $this->redis->close();
            }
        });
    }

    public function testAnEmptySubscriptionCanAddItsFirstChannelLater(): void
    {
        WorkerHarness::run([], function (): void {
            $subscription = $this->redis->subscribe();
            try {
                $subscription->add('', '42');
                self::assertSame(1, $this->redis->publish('42', 'numeric'));
                self::assertSame(1, $this->redis->publish('', 'empty'));
                self::assertSame([['42', 'numeric'], ['', 'empty']], $this->take($subscription, 2));
            } finally {
                $this->redis->close();
            }
        });
    }

    public function testABurstKeepsRedisWireOrderAndBinaryPayloads(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $channel = $this->prefix . "\0\xff\r\n";
            $subscription = $this->redis->subscribe($channel);
            $payloads = ["\0\xff\xfe\r\n*3\r\n", '', str_repeat("\0\r\n\xff", 40_000), ...array_map(strval(...), range(1, 100))];
            try {
                $waiting = $worker->spawn(fn () => $this->take($subscription, count($payloads)));
                foreach ($payloads as $payload) {
                    self::assertSame(1, $this->redis->publish($channel, $payload));
                }
                self::assertSame($payloads, array_column($waiting->await(), 1));
                self::assertSame(0, $subscription->dropped());
            } finally {
                $this->redis->close();
            }
        });
    }

    public function testAFullBufferDropsItsOldestMessagesAndCountsThem(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $redis = new EngineClient($this->url, subscriptionBuffer: 3);
            $channel = $this->prefix . 'bounded';
            $subscription = $redis->subscribe($channel);
            try {
                for ($index = 0; $index < 50; ++$index) {
                    $this->redis->publish($channel, (string) $index);
                }
                $this->until($worker, static fn (): bool => $subscription->dropped() === 47);
                self::assertSame(['47', '48', '49'], array_column($this->take($subscription, 3), 1));
                self::assertSame(47, $subscription->dropped());
                self::assertSame(0, $subscription->gaps());
            } finally {
                $redis->close();
                $this->redis->close();
            }
        });
    }

    public function testASecondConcurrentConsumerIsRefusedWithAUsefulMessage(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $subscription = $this->redis->subscribe($this->prefix . 'single');
            try {
                $first = $worker->spawn(fn () => $this->take($subscription, 1));
                Async::sleep($worker->loop, 0);
                try {
                    $this->take($subscription, 1);
                    self::fail('Two consumers shared one subscription.');
                } catch (\LogicException $error) {
                    self::assertStringContainsString('already has a consumer', $error->getMessage());
                    self::assertStringContainsString('create a separate subscription', $error->getMessage());
                }
                $this->redis->publish($this->prefix . 'single', 'first');
                self::assertSame('first', $first->await()[0][1]);
                $this->redis->publish($this->prefix . 'single', 'next consumer');
                self::assertSame('next consumer', $this->take($subscription, 1)[0][1]);
            } finally {
                $this->redis->close();
            }
        });
    }

    public function testAConsumerOwnsTheSubscriptionWhileProcessingAYieldedMessage(): void
    {
        WorkerHarness::run([], function (): void {
            $subscription = $this->redis->subscribe($this->prefix . 'processing');
            try {
                $this->redis->publish($this->prefix . 'processing', 'first');
                $iterator = $subscription->getIterator();
                $iterator->rewind();
                self::assertSame('first', $iterator->current());
                try {
                    $subscription->getIterator()->rewind();
                    self::fail('A paused iterator lost ownership of its consumer.');
                } catch (\LogicException $error) {
                    self::assertStringContainsString('Finish its iterator', $error->getMessage());
                }
                unset($iterator);
            } finally {
                $this->redis->close();
            }
        });
    }

    public function testCloseEndsAWaitingIteratorAndIsHarmlessTwice(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $subscription = $this->redis->subscribe($this->prefix . 'close');
            $waiting = $worker->spawn(fn () => $this->take($subscription, 1));
            Async::sleep($worker->loop, 0);
            $subscription->close();
            $subscription->close();
            self::assertSame([], $waiting->await());
            self::assertSame([], iterator_to_array($subscription));
            self::assertSame(0, $this->redis->publish($this->prefix . 'close', 'gone'));
            try {
                $subscription->add('later');
                self::fail('A closed subscription was changed.');
            } catch (RedisException $error) {
                self::assertStringContainsString('Create a new subscription', $error->getMessage());
            }
            $this->redis->close();
        });
    }

    public function testKillingTheDedicatedConnectionRestoresTheCurrentSetAndReportsOneGap(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $reports = [];
            $previous = FailureReporter::handleWith(static function (string $what, ?\Throwable $error) use (&$reports): void {
                $reports[] = [$what, $error];
            });
            $a = $this->prefix . 'old';
            $b = $this->prefix . 'current';
            $subscription = $this->redis->subscribe($a);
            try {
                $subscription->add($b);
                $subscription->remove($a);
                $id = $this->subscriberId($b);
                $waiting = $worker->spawn(fn () => $this->take($subscription, 1));
                self::assertSame(1, $this->redis->command('CLIENT', 'KILL', 'ID', $id));
                $this->until($worker, fn (): bool => $this->redis->command('PUBSUB', 'NUMSUB', $b)[1] === 1);
                self::assertSame(0, $this->redis->publish($a, 'removed'));
                self::assertSame(1, $this->redis->publish($b, 'after gap'));
                self::assertSame([[$b, 'after gap']], $waiting->await());
                self::assertNotSame($id, $this->subscriberId($b));
                self::assertSame(1, $subscription->gaps());
                self::assertCount(1, $reports);
                self::assertStringContainsString('resynchronise', $reports[0][0]);
            } finally {
                $this->redis->close();
                FailureReporter::handleWith($previous);
            }
        });
    }

    public function testAGapDiscardsBufferedUpdatesBeforeTheCallerResynchronises(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $previous = FailureReporter::handleWith(static function (): void {});
            $channel = $this->prefix . 'stale';
            $subscription = $this->redis->subscribe($channel);
            try {
                foreach (['1', '2', '3'] as $payload) {
                    $this->redis->publish($channel, $payload);
                }
                $buffer = (new \ReflectionProperty($subscription, 'messages'))->getValue($subscription);
                $this->until($worker, static fn (): bool => $buffer->count() === 3);
                $this->redis->command('CLIENT', 'KILL', 'ID', $this->subscriberId($channel));
                $this->until($worker, static fn (): bool => $subscription->gaps() === 1);
                self::assertSame(0, $buffer->count());
                self::assertSame(3, $subscription->dropped());
                $state = 4; // Authoritative state reloaded after noticing the gap.
                $this->until($worker, fn (): bool => $this->redis->command('PUBSUB', 'NUMSUB', $channel)[1] === 1);
                $this->redis->publish($channel, '5');
                foreach ($subscription as $payload) {
                    $state = (int) $payload;
                    break;
                }
                self::assertSame(5, $state);
            } finally {
                $this->redis->close();
                FailureReporter::handleWith($previous);
            }
        });
    }

    public function testAGapCanDiscardAMessageAfterWakeupButBeforeItIsYielded(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $previous = FailureReporter::handleWith(static function (): void {});
            $peer = null;
            // Both frames arrive in one read, before the resumed consumer
            // fiber gets a turn to yield the stale publication.
            [$url, $close] = $this->server($worker->loop, static function (array $words, mixed $stream) use (&$peer): string {
                $peer = $stream;
                return Resp::command(['subscribe', $words[1], 1]);
            });
            $redis = new EngineClient($url);
            try {
                $subscription = $redis->subscribe('in flight');
                $waiting = $worker->spawn(fn () => $this->take($subscription, 1));
                Async::sleep($worker->loop, 0);
                fwrite($peer, Resp::command(['message', 'in flight', 'stale']) . "?broken\r\n");
                $this->until($worker, static fn (): bool => $subscription->gaps() === 1);
                self::assertSame(1, $subscription->dropped());
                self::assertFalse($waiting->isComplete());
                $this->until($worker, static fn (): bool => (new \ReflectionProperty($subscription, 'connected'))->getValue($subscription));
                fwrite($peer, Resp::command(['message', 'in flight', 'new']));
                self::assertSame([['in flight', 'new']], $waiting->await());
            } finally {
                $redis->close();
                $close();
                FailureReporter::handleWith($previous);
            }
        });
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function refusedReconnectCommands(): iterable
    {
        yield 'credentials' => ['AUTH'];
        yield 'database' => ['SELECT'];
        yield 'channel permissions' => ['SUBSCRIBE'];
    }

    #[DataProvider('refusedReconnectCommands')]
    public function testAPermanentReconnectRefusalEndsTheWaitAndRequiresANewClient(string $refused): void
    {
        WorkerHarness::run([], function (Worker $worker) use ($refused): void {
            $reports = [];
            $commands = [];
            $peer = null;
            $previous = FailureReporter::handleWith(static function (string $what, ?\Throwable $error) use (&$reports): void {
                $reports[] = [$what, $error];
            });
            [$url, $close] = $this->server($worker->loop, static function (array $words, mixed $stream, int $connection) use ($refused, &$commands, &$peer): string {
                $commands[] = [$connection, $words[0]];
                if ($connection > 1 && $words[0] === $refused) {
                    return "-ERR credentials or database refused secret-password\r\n";
                }
                if ($words[0] === 'SUBSCRIBE') {
                    $peer = $stream;
                    return Resp::command(['subscribe', $words[1], 1]);
                }
                return "+OK\r\n";
            });
            $redis = new EngineClient($url . '?password=secret-password&database=2');
            try {
                $subscription = $redis->subscribe('refused');
                $waiting = $worker->spawn(function () use ($subscription): mixed {
                    try {
                        return $this->take($subscription, 1);
                    } catch (RedisException $error) {
                        return $error;
                    }
                });
                Async::sleep($worker->loop, 0);
                stream_socket_shutdown($peer, STREAM_SHUT_RDWR);
                Async::sleep($worker->loop, 0.35);
                self::assertTrue($waiting->isComplete(), 'Permanent reconnect refusal left the consumer waiting.');
                $error = $waiting->await();
                self::assertInstanceOf(RedisException::class, $error);
                self::assertStringContainsString('new client', $error->getMessage());
                self::assertStringContainsString('updated settings', $error->getMessage());
                self::assertStringNotContainsString('secret-password', (string) $error);
                foreach (['add', 'remove'] as $method) {
                    try {
                        $subscription->{$method}('refused');
                        self::fail('A permanently refused subscription accepted a channel change.');
                    } catch (RedisException $same) {
                        self::assertSame($error, $same);
                    }
                }
                self::assertCount(2, $reports); // The transport gap and the terminal refusal.
                self::assertSame($error, $reports[1][1]);
                self::assertNull((new \ReflectionProperty($subscription, 'reconnectTimer'))->getValue($subscription));
                $attempts = count($commands);
                Async::sleep($worker->loop, 0.3);
                self::assertCount($attempts, $commands);
            } finally {
                $redis->close();
                $close();
                FailureReporter::handleWith($previous);
            }
        });
    }

    public function testRotatedRedisCredentialsTerminateTheOldSubscriptionAndANewClientWorks(): void
    {
        $user = 'pubsub-rotate-' . bin2hex(random_bytes(4));
        $this->redis->command('ACL', 'SETUSER', $user, 'on', '>old-password', '~*', '&*', '+@all');
        try {
            WorkerHarness::run([], function (Worker $worker) use ($user): void {
                $reports = [];
                $previous = FailureReporter::handleWith(static function (string $what, ?\Throwable $error) use (&$reports): void {
                    $reports[] = $error;
                });
                $host = parse_url($this->url, PHP_URL_HOST) . ':' . parse_url($this->url, PHP_URL_PORT);
                $redis = new EngineClient('redis://' . $user . ':old-password@' . $host);
                $replacement = new EngineClient('redis://' . $user . ':new-password@' . $host);
                $channel = $this->prefix . 'rotated';
                try {
                    $subscription = $redis->subscribe($channel);
                    $waiting = $worker->spawn(function () use ($subscription): mixed {
                        try {
                            return $this->take($subscription, 1);
                        } catch (RedisException $error) {
                            return $error;
                        }
                    });
                    $this->redis->command('ACL', 'SETUSER', $user, 'resetpass', '>new-password');
                    $this->redis->command('CLIENT', 'KILL', 'ID', $this->subscriberId($channel));
                    $this->until($worker, static fn (): bool => $waiting->isComplete());
                    $error = $waiting->await();
                    self::assertInstanceOf(RedisException::class, $error);
                    self::assertStringContainsString('AUTH', $error->getMessage());
                    self::assertStringContainsString('new client', $error->getMessage());
                    self::assertStringNotContainsString('old-password', (string) $error);
                    self::assertCount(2, $reports);
                    self::assertSame($error, $reports[1]);
                    $current = $replacement->subscribe($channel);
                    self::assertSame(1, $this->redis->publish($channel, 'updated settings'));
                    self::assertSame('updated settings', $this->take($current, 1)[0][1]);
                } finally {
                    $redis->close();
                    $replacement->close();
                    FailureReporter::handleWith($previous);
                }
            });
        } finally {
            $this->redis->command('ACL', 'DELUSER', $user);
        }
    }

    public function testAQuietSubscriptionOutlivesTheReplyTimeoutAndAnswersIdlePings(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $redis = new EngineClient($this->url, replyTimeout: 0.05, subscriptionIdleInterval: 0.04, subscriptionPingTimeout: 0.1);
            $subscription = $redis->subscribe($this->prefix . 'quiet');
            try {
                $waiting = $worker->spawn(fn () => $this->take($subscription, 1));
                Async::sleep($worker->loop, 0.25);
                self::assertFalse($waiting->isComplete());
                self::assertSame(0, $subscription->gaps());
                self::assertSame(1, $redis->publish($this->prefix . 'quiet', 'still here'));
                self::assertSame('still here', $waiting->await()[0][1]);
                $subscription->remove($this->prefix . 'quiet');
                // Empty subscriptions receive the ordinary +PONG form.
                Async::sleep($worker->loop, 0.15);
                self::assertSame(0, $subscription->gaps());
            } finally {
                $redis->close();
            }
        });
    }

    public function testRestartingRedisResubscribesWithoutCountingEveryRetryAsAGap(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $server = new SubscriptionRedisServer();
            $redis = new EngineClient($server->url, timeout: 0.1);
            $reports = [];
            $previous = FailureReporter::handleWith(static function (string $what, ?\Throwable $error) use (&$reports): void {
                $reports[] = [$what, $error];
            });
            try {
                $server->start($worker);
                $subscription = $redis->subscribe('restart');
                $waiting = $worker->spawn(fn () => $this->take($subscription, 1));
                $server->stop($worker);
                $this->until($worker, static fn (): bool => $subscription->gaps() === 1);
                Async::sleep($worker->loop, 0.75);
                self::assertCount(1, $reports);
                self::assertFalse($waiting->isComplete());
                $server->start($worker);
                $this->until($worker, static fn (): bool => $redis->command('PUBSUB', 'NUMSUB', 'restart')[1] === 1);
                self::assertSame(1, $redis->publish('restart', 'after restart'));
                self::assertSame([['restart', 'after restart']], $waiting->await());
                self::assertSame(1, $subscription->gaps());
                self::assertCount(1, $reports);
            } finally {
                $redis->close();
                $server->stop($worker);
                FailureReporter::handleWith($previous);
            }
        });
    }

    public function testSeparateOutagesAreReportedSeparately(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $reports = [];
            $previous = FailureReporter::handleWith(static function (string $what) use (&$reports): void {
                $reports[] = $what;
            });
            $channel = $this->prefix . 'twice';
            $subscription = $this->redis->subscribe($channel);
            try {
                for ($outage = 1; $outage <= 2; ++$outage) {
                    $this->redis->command('CLIENT', 'KILL', 'ID', $this->subscriberId($channel));
                    $this->until($worker, fn (): bool => $this->redis->command('PUBSUB', 'NUMSUB', $channel)[1] === 1);
                    self::assertSame($outage, $subscription->gaps());
                    self::assertCount($outage, $reports);
                }
            } finally {
                $this->redis->close();
                FailureReporter::handleWith($previous);
            }
        });
    }

    public function testIdlePingDetectsAPeerThatStoppedAnswering(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $commands = [];
            $reports = [];
            $previous = FailureReporter::handleWith(static function (string $what) use (&$reports): void {
                $reports[] = $what;
            });
            [$url, $close] = $this->server($worker->loop, static function (array $words) use (&$commands): ?string {
                $commands[] = $words[0];
                return $words[0] === 'SUBSCRIBE' ? Resp::command(['subscribe', $words[1], 1]) : null;
            });
            $redis = new EngineClient($url, replyTimeout: 0.02, subscriptionIdleInterval: 0.04, subscriptionPingTimeout: 0.04);
            try {
                $subscription = $redis->subscribe('silent');
                $waiting = $worker->spawn(fn () => $this->take($subscription, 1));
                $this->until($worker, static fn (): bool => $subscription->gaps() === 1);
                self::assertContains('PING', $commands);
                self::assertFalse($waiting->isComplete());
                self::assertCount(1, $reports);
                $subscription->close();
                self::assertSame([], $waiting->await());
            } finally {
                $redis->close();
                $close();
                FailureReporter::handleWith($previous);
            }
        });
    }

    public function testClosingDuringReconnectCancelsRetriesAndEndsTheConsumer(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $connections = 0;
            $reports = [];
            $previous = FailureReporter::handleWith(static function (string $what) use (&$reports): void {
                $reports[] = $what;
            });
            [$url, $close] = $this->server($worker->loop, static function (array $words, mixed $stream, int $connection) use (&$connections): ?string {
                $connections = $connection;
                return Resp::command(['subscribe', $words[1], 1]);
            });
            $redis = new EngineClient($url);
            try {
                $subscription = $redis->subscribe('closing');
                $waiting = $worker->spawn(fn () => $this->take($subscription, 1));
                Async::sleep($worker->loop, 0);
                $close();
                $this->until($worker, static fn (): bool => $subscription->gaps() === 1);
                Async::sleep($worker->loop, 0.4);
                self::assertSame(1, $subscription->gaps());
                self::assertCount(1, $reports);
                $subscription->close();
                self::assertSame([], $waiting->await());
                Async::sleep($worker->loop, 0.25);
                self::assertSame(1, $connections);
                self::assertNull((new \ReflectionProperty($subscription, 'reconnectTimer'))->getValue($subscription));
            } finally {
                $redis->close();
                $close();
                FailureReporter::handleWith($previous);
            }
        });
    }

    public function testSetupUsesUrlCredentialsAndDatabaseWithoutExposingAPassword(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $commands = [];
            [$url, $close] = $this->server($worker->loop, static function (array $words) use (&$commands): string {
                $commands[] = $words;
                return $words[0] === 'SUBSCRIBE' ? Resp::command(['subscribe', $words[1], 1]) : "+OK\r\n";
            });
            $redis = new EngineClient(str_replace('redis://', 'redis://user:a%252Fb%20c@', $url) . '/2');
            try {
                $redis->subscribe('setup');
                self::assertSame([['AUTH', 'user', 'a%2Fb c'], ['SELECT', '2'], ['SUBSCRIBE', 'setup']], $commands);
            } finally {
                $redis->close();
                $close();
            }

            [$url, $close] = $this->server($worker->loop, static fn (): string => "-ERR secret-password was refused\r\n");
            $redis = new EngineClient($url . '?password=secret-password&database=2');
            try {
                $redis->subscribe('refused');
                self::fail('Refused authentication opened a subscription.');
            } catch (RedisException $error) {
                self::assertStringContainsString('Check credentials', $error->getMessage());
                self::assertStringNotContainsString('secret-password', (string) $error);
            } finally {
                $redis->close();
                $close();
            }
        });
    }

    public function testConcurrentChannelChangesWaitForTheirOwnConfirmations(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $channel = $this->prefix . 'concurrent';
            $subscription = $this->redis->subscribe();
            try {
                $changes = [
                    $worker->spawn(static fn () => $subscription->add($channel)),
                    $worker->spawn(static fn () => $subscription->remove($channel)),
                    $worker->spawn(static fn () => $subscription->add($channel)),
                ];
                foreach ($changes as $change) {
                    $change->await();
                }
                self::assertSame(1, $this->redis->publish($channel, 'confirmed'));
                self::assertSame('confirmed', $this->take($subscription, 1)[0][1]);
            } finally {
                $this->redis->close();
            }
        });
    }

    public function testPublicationsAreReadWhileAChannelChangeWaitsForConfirmation(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $pending = $worker->loop->getSuspension();
            $stream = null;
            [$url, $close] = $this->server($worker->loop, static function (array $words, mixed $socket) use ($pending, &$stream): ?string {
                if ($words[0] === 'SUBSCRIBE' && $words[1] === 'later') {
                    $stream = $socket;
                    $pending->resume();
                    return Resp::command(['message', 'original', 'before confirmation']);
                }
                return Resp::command(['subscribe', $words[1], 1]);
            });
            $redis = new EngineClient($url);
            try {
                $subscription = $redis->subscribe('original');
                $adding = $worker->spawn(static fn () => $subscription->add('later'));
                $pending->suspend();
                self::assertFalse($adding->isComplete());
                self::assertSame([['original', 'before confirmation']], $this->take($subscription, 1));
                self::assertFalse($adding->isComplete());
                fwrite($stream, Resp::command(['subscribe', 'later', 2]));
                $adding->await();
                self::assertTrue($adding->isComplete());
            } finally {
                $redis->close();
                $close();
            }
        });
    }

    public function testCloseFailsPendingAndQueuedChannelChangesWithoutSuspending(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $received = $worker->loop->getSuspension();
            [$url, $close] = $this->server($worker->loop, static function (array $words) use ($received): ?string {
                if ($words[1] === 'pending') {
                    $received->resume();
                    return null;
                }
                return Resp::command(['subscribe', $words[1], 1]);
            });
            $redis = new EngineClient($url);
            try {
                $subscription = $redis->subscribe('first');
                $change = static function (string $channel) use ($subscription): ?RedisException {
                    try {
                        $subscription->add($channel);
                        return null;
                    } catch (RedisException $error) {
                        return $error;
                    }
                };
                $pending = $worker->spawn(static fn () => $change('pending'));
                $received->suspend();
                $queued = $worker->spawn(static fn () => $change('queued'));
                Async::sleep($worker->loop, 0);
                $subscription->close();
                self::assertInstanceOf(RedisException::class, $pending->await());
                self::assertInstanceOf(RedisException::class, $queued->await());
            } finally {
                $redis->close();
                $close();
            }
        });
    }

    public function testASetupTimeoutFailsWithoutLeavingAConnectionOrRetry(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            [$url, $close] = $this->server($worker->loop, static fn (): null => null);
            $redis = new EngineClient($url, replyTimeout: 0.05);
            try {
                try {
                    $redis->subscribe('unconfirmed');
                    self::fail('An unconfirmed subscription was returned.');
                } catch (RedisException $error) {
                    self::assertStringContainsString('did not confirm SUBSCRIBE', $error->getMessage());
                    self::assertSame([], (new \ReflectionProperty($redis, 'subscriptions'))->getValue($redis));
                }
            } finally {
                $redis->close();
                $close();
            }
        });
    }

    public function testClientCloseCancelsAnEncryptedConnectionStillBeingOpened(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            $accepted = $worker->loop->getSuspension();
            $listener = stream_socket_server('tcp://127.0.0.1:0', $code, $reason);
            self::assertIsResource($listener, (string) $reason);
            stream_set_blocking($listener, false);
            $address = stream_socket_get_name($listener, false);
            $peer = null;
            $accept = $worker->loop->onReadable($listener, static function () use ($listener, $accepted, &$peer): void {
                $peer = stream_socket_accept($listener, 0);
                $accepted->resume();
            });
            $redis = new EngineClient('rediss://' . $address, timeout: 10.0);
            try {
                $opening = $worker->spawn(static function () use ($redis): ?RedisException {
                    try {
                        $redis->subscribe('never opened');
                        return null;
                    } catch (RedisException $error) {
                        return $error;
                    }
                });
                $accepted->suspend();
                Async::sleep($worker->loop, 0);
                $before = $worker->loop->getIdentifiers();
                $start = hrtime(true);
                $redis->close();
                // Inspect before awaiting the opener, which would supply a
                // loop turn and hide asynchronous watcher/socket cleanup.
                self::assertSame(2, count(array_diff($before, $worker->loop->getIdentifiers())));
                foreach (get_resources('stream') as $candidate) {
                    self::assertNotSame($address, @stream_socket_get_name($candidate, true));
                }
                self::assertInstanceOf(RedisException::class, $opening->await());
                self::assertLessThan(1_000_000_000, hrtime(true) - $start);
                self::assertSame([], (new \ReflectionProperty($redis, 'subscriptions'))->getValue($redis));
                self::assertLessThan(count($before), count($worker->loop->getIdentifiers()));
            } finally {
                $redis->close();
                $worker->loop->cancel($accept);
                if (is_resource($peer)) {
                    fclose($peer);
                }
                fclose($listener);
            }
        });
    }

    public function testAMalformedReplyAfterConfirmationDoesNotReturnABrokenSubscription(): void
    {
        WorkerHarness::run([], function (Worker $worker): void {
            [$url, $close] = $this->server($worker->loop, static fn (array $words): string => Resp::command(['subscribe', $words[1], 1]) . "?broken\r\n");
            $redis = new EngineClient($url);
            try {
                try {
                    $redis->subscribe('broken');
                    self::fail('A broken subscription escaped its initial setup.');
                } catch (RedisException $error) {
                    self::assertStringContainsString('before the subscription was ready', $error->getMessage());
                    self::assertSame([], (new \ReflectionProperty($redis, 'subscriptions'))->getValue($redis));
                }
            } finally {
                $redis->close();
                $close();
            }
        });
    }

    public function testUrlAuthenticationAlsoWorksAgainstRedisItself(): void
    {
        $user = 'pubsub-test-' . bin2hex(random_bytes(4));
        $password = 'a%2Fb c';
        $this->redis->command('ACL', 'SETUSER', $user, 'on', '>' . $password, '~*', '&*', '+@all');
        try {
            WorkerHarness::run([], function () use ($user, $password): void {
                $address = parse_url($this->url, PHP_URL_HOST) . ':' . parse_url($this->url, PHP_URL_PORT);
                $redis = new EngineClient('redis://' . $user . '@' . $address . '?password=' . rawurlencode($password) . '&database=2');
                try {
                    $subscription = $redis->subscribe($this->prefix . 'authenticated');
                    self::assertSame(1, $redis->publish($this->prefix . 'authenticated', 'authenticated'));
                    self::assertSame('authenticated', $this->take($subscription, 1)[0][1]);
                } finally {
                    $redis->close();
                }
            });
        } finally {
            $this->redis->command('ACL', 'DELUSER', $user);
        }
    }

    public function testEncryptedSubscriptionsUseTheSameTrustedTlsSettingsAsCommands(): void
    {
        $config = tempnam(sys_get_temp_dir(), 'crab-redis-tls-config-');
        $certificate = tempnam(sys_get_temp_dir(), 'crab-redis-tls-cert-');
        $keyFile = tempnam(sys_get_temp_dir(), 'crab-redis-tls-key-');
        try {
            file_put_contents($config, "[req]\ndistinguished_name=dn\n[dn]\n[v3]\nsubjectAltName=IP:127.0.0.1\nbasicConstraints=CA:TRUE\nkeyUsage=digitalSignature,keyEncipherment,keyCertSign\n");
            $key = openssl_pkey_new(['private_key_bits' => 2048]);
            self::assertNotFalse($key);
            $request = openssl_csr_new(['commonName' => '127.0.0.1'], $key, ['config' => $config, 'digest_alg' => 'sha256']);
            self::assertNotFalse($request);
            $cert = openssl_csr_sign($request, null, $key, 1, ['config' => $config, 'x509_extensions' => 'v3', 'digest_alg' => 'sha256']);
            self::assertNotFalse($cert);
            self::assertTrue(openssl_x509_export_to_file($cert, $certificate));
            self::assertTrue(openssl_pkey_export_to_file($key, $keyFile));
            WorkerHarness::run([], function (Worker $worker) use ($certificate, $keyFile): void {
                $server = new SubscriptionRedisServer(['cert' => $certificate, 'key' => $keyFile]);
                $redis = new EngineClient($server->url, tls: ['cafile' => $certificate]);
                try {
                    $server->start($worker);
                    $subscription = $redis->subscribe('encrypted');
                    self::assertSame(1, $redis->publish('encrypted', "secret\0payload"));
                    self::assertSame([["encrypted", "secret\0payload"]], $this->take($subscription, 1));
                    self::assertSame(0, $subscription->gaps());
                } finally {
                    $redis->close();
                    $server->stop($worker);
                }
            });
        } finally {
            unlink($config);
            unlink($certificate);
            unlink($keyFile);
        }
    }

    /**
     * @return iterable<string, array{array<string, int|float>, string}>
     */
    public static function invalidLimits(): iterable
    {
        yield 'empty buffer' => [['subscriptionBuffer' => 0], 'subscriptionBuffer'];
        yield 'negative buffer' => [['subscriptionBuffer' => -1], 'subscriptionBuffer'];
        yield 'zero idle interval' => [['subscriptionIdleInterval' => 0.0], 'subscriptionIdleInterval'];
        yield 'infinite idle interval' => [['subscriptionIdleInterval' => INF], 'subscriptionIdleInterval'];
        yield 'zero ping timeout' => [['subscriptionPingTimeout' => 0.0], 'subscriptionPingTimeout'];
        yield 'nan ping timeout' => [['subscriptionPingTimeout' => NAN], 'subscriptionPingTimeout'];
    }

    /**
     * @param array<string, int|float> $options
     */
    #[DataProvider('invalidLimits')]
    public function testSubscriptionLimitsMustBePositiveAndFinite(array $options, string $option): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Set ' . $option);
        new EngineClient($this->url, ...$options);
    }

    public function testAnInvalidUrlDoesNotEchoCredentials(): void
    {
        try {
            new EngineClient('http://user:secret-password@localhost');
            self::fail('An HTTP address was accepted as Redis.');
        } catch (\InvalidArgumentException $error) {
            self::assertStringNotContainsString('secret-password', (string) $error);
            self::assertStringContainsString('redis://127.0.0.1:6379', $error->getMessage());
        }
    }

    /**
     * @return list<array{string, string}>
     */
    private function take(Subscription $subscription, int $count): array
    {
        $messages = [];
        foreach ($subscription as $channel => $payload) {
            $messages[] = [$channel, $payload];
            if (count($messages) === $count) {
                break;
            }
        }
        return $messages;
    }

    private function until(Worker $worker, \Closure $condition): void
    {
        $deadline = hrtime(true) + 5_000_000_000;
        while (!$condition()) {
            self::assertLessThan($deadline, hrtime(true), 'The asynchronous condition did not become true within five seconds.');
            Async::sleep($worker->loop, 0.01);
        }
    }

    private function subscriberId(string $channel): string
    {
        self::assertSame(1, $this->redis->command('PUBSUB', 'NUMSUB', $channel)[1]);
        $lines = explode("\n", trim($this->redis->command('CLIENT', 'LIST', 'TYPE', 'PUBSUB')));
        self::assertCount(1, $lines);
        preg_match('/(?:^| )id=(\d+)/', $lines[0], $match);
        return $match[1];
    }

    /**
     * A local RESP2 peer for stalled PINGs and setup failures. It owns
     * every accepted stream and watcher and closes only those resources.
     *
     * @param \Closure(array, mixed, int): ?string $answer
     * @return array{string, \Closure(): void}
     */
    private function server(EventLoop $loop, \Closure $answer): array
    {
        $listener = stream_socket_server('tcp://127.0.0.1:0', $code, $reason);
        self::assertIsResource($listener, (string) $reason);
        stream_set_blocking($listener, false);
        $address = stream_socket_get_name($listener, false);
        $streams = [$listener];
        $watchers = [];
        $connections = 0;
        $accept = $loop->onReadable($listener, static function () use ($listener, $loop, $answer, &$streams, &$watchers, &$connections): void {
            $stream = stream_socket_accept($listener, 0);
            if ($stream === false) {
                return;
            }
            stream_set_blocking($stream, false);
            $streams[] = $stream;
            $connection = ++$connections;
            $received = '';
            $reader = $loop->onReadable($stream, static function (string $watcher) use ($stream, $loop, $answer, $connection, &$received): void {
                $bytes = fread($stream, 65_536);
                if ($bytes === false || ($bytes === '' && feof($stream))) {
                    $loop->cancel($watcher);
                    return;
                }
                $received .= $bytes;
                $offset = 0;
                while (true) {
                    $words = Resp::reply($received, $offset, $complete);
                    if (!$complete) {
                        break;
                    }
                    $wire = $answer($words, $stream, $connection);
                    if ($wire !== null) {
                        fwrite($stream, $wire);
                    }
                }
                $received = substr($received, $offset);
            });
            $loop->unreference($reader);
            $watchers[] = $reader;
        });
        $loop->unreference($accept);
        $watchers[] = $accept;
        return ['redis://' . $address, static function () use ($loop, &$streams, &$watchers): void {
            foreach ($watchers as $watcher) {
                $loop->cancel($watcher);
            }
            foreach ($streams as $stream) {
                fclose($stream);
            }
            $streams = $watchers = [];
        }];
    }
}
