<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

require_once dirname(__DIR__) . '/vendor/autoload.php';

use Crab\Broadcast\Broadcaster;
use Crab\Broadcast\Channels;
use Crab\Contracts\WebSocket\Recipient;
use Crab\Engine\Async\Async;
use Crab\Engine\Async\Cancellation;
use Crab\Engine\Async\CancelledException;
use Crab\Engine\Async\Task;
use Crab\Engine\EventLoop;
use Crab\Engine\EventLoop\RevoltEventLoop;
use Crab\Engine\FailureReporter;
use Crab\Engine\Network\WriteResult;
use Crab\Redis\BroadcastCarrier;
use Crab\Redis\Client;
use Crab\Redis\EngineClient;
use Crab\Redis\Resp;
use Crab\Redis\Subscription;
use PHPUnit\Framework\TestCase;

/**
 * Production subscription parsing over owned socket pairs, with optional
 * real Redis cases for the orchestrator's isolated integration run.
 */
final class BroadcastCarrierTest extends TestCase
{
    private EventLoop $loop;
    private PairBroker $broker;
    private ?\Closure $reporter;
    private array $reports = [];
    private array $carriers = [];
    private array $clients = [];
    private array $registries = [];
    private array $apis = [];

    protected function setUp(): void
    {
        $this->loop = EventLoop::install(RevoltEventLoop::create());
        $this->broker = new PairBroker($this->loop);
        $this->reporter = FailureReporter::handleWith(function (string $what, ?\Throwable $error): void {
            $this->reports[] = [$what, $error];
        });
    }

    protected function tearDown(): void
    {
        foreach ($this->apis as $api) {
            $api->closeSockets();
        }
        foreach ($this->registries as $registry) {
            $registry->close();
        }
        foreach ($this->carriers as $carrier) {
            $carrier->close();
        }
        foreach ($this->clients as $client) {
            $client->close();
        }
        $this->broker->close();
        $this->loop->defer($this->loop->stop(...));
        $deadline = $this->loop->delay(1.0, static fn() => self::fail(
            'Timed out waiting for the carrier cleanup loop to stop.',
        ));
        try {
            $this->loop->run();
        } finally {
            $this->loop->cancel($deadline);
        }
        self::assertSame([], $this->loop->getIdentifiers(), 'The carrier retained a watcher after stop.');
        FailureReporter::handleWith($this->reporter);
    }

    private function carrier(string $prefix = 'app:'): BroadcastCarrier
    {
        $client = $this->clients[] = new PairClient($this->broker);
        return $this->carriers[] = new BroadcastCarrier($client, $prefix);
    }

    /**
     * Run I/O in a worker-like fiber. A command called from main drains
     * the loop, which cannot finish while a broadcast consumer is active.
     * Bound the caller's wait even if the operation's own deadline fails.
     */
    private function waitFor(string $what, \Closure $body, float $seconds = 3.0): mixed
    {
        $task = Task::spawn($this->loop, $body);
        $cancellation = new Cancellation();
        $deadline = $this->loop->delay($seconds, static fn() => $cancellation->cancel());
        try {
            return $task->await($cancellation);
        } catch (CancelledException $error) {
            if (!$cancellation->isCancelled()) {
                throw $error;
            }
            $task->cancel();
            self::fail("Timed out after {$seconds} seconds waiting for {$what}.");
        } finally {
            $this->loop->cancel($deadline);
        }
    }

    private function waitUntil(string $what, \Closure $ready): void
    {
        $this->waitFor($what, function () use ($ready): void {
            while (!$ready()) {
                Async::sleep($this->loop, 0.001);
            }
        });
    }

    public function testConstructingAndSendingWithoutSubscribersOpensNoSubscription(): void
    {
        $carrier = $this->carrier();
        self::assertSame([], $this->loop->getIdentifiers());
        $carrier->publish(['orders'], 'text');
        self::assertSame([], $this->broker->peers);
        self::assertSame([], $this->loop->getIdentifiers());
    }

    public function testTwoCarriersOnTwoClientsReceiveEachOthersOrderedMessages(): void
    {
        $a = $this->carrier();
        $b = $this->carrier();
        $receivedA = new \ArrayObject();
        $receivedB = new \ArrayObject();
        $gap = static function (): void {};
        $this->waitFor('Redis channel join confirmation', static fn() => $a->subscribe(
            'orders',
            static function (string $channel, string $payload) use ($receivedA): void {
                $receivedA[] = [$channel, $payload];
            },
            $gap,
        ));
        $this->waitFor('Redis channel join confirmation', static fn() => $b->subscribe(
            'orders',
            static function (string $channel, string $payload) use ($receivedB): void {
                $receivedB[] = [$channel, $payload];
            },
            $gap,
        ));
        $a->publish(['orders'], '{"step":1}');
        $a->publish(['orders'], 'step 2');
        $b->publish(['orders'], 'from b');
        $this->waitUntil(
            'three ordered messages on both carriers',
            static fn() => count($receivedA) === 3 && count($receivedB) === 3,
        );
        $expected = [
            ['orders', '{"step":1}'],
            ['orders', 'step 2'],
            ['orders', 'from b'],
        ];
        self::assertSame($expected, $receivedA->getArrayCopy());
        self::assertSame($expected, $receivedB->getArrayCopy());
        self::assertCount(2, $this->broker->peers);
    }

    public function testTheFirstJoinSubscribesAndTheLastLeaveUnsubscribes(): void
    {
        $carrier = $this->carrier();
        $channels = $this->registries[] = new Channels($carrier);
        $a = new BroadcastRecipient();
        $b = new BroadcastRecipient();
        $this->waitFor('Redis channel join confirmation', static fn() => $channels->join($a, 'orders'));
        $this->waitFor('Redis channel join confirmation', static fn() => $channels->join($b, 'orders'));
        self::assertSame([['SUBSCRIBE', 'app:orders']], $this->broker->commands);
        $channels->forget($a);
        Async::sleep($this->loop, 0.001);
        self::assertCount(1, $this->broker->commands);
        $channels->forget($b);
        $this->waitUntil('the last leave to send UNSUBSCRIBE', fn() => count($this->broker->commands) === 2);
        self::assertSame(['UNSUBSCRIBE', 'app:orders'], $this->broker->commands[1]);
        self::assertSame(0, $channels->stats()['connections']);
        $this->waitFor('Redis channel join confirmation', static fn() => $channels->join($a, 'orders'));
        self::assertCount(1, $this->broker->peers);
        new Broadcaster($carrier)->to('orders')->send('back');
        $this->waitUntil('delivery after rejoining orders', static fn() => $a->messages !== []);
        self::assertSame(['back'], $a->messages);
    }

    public function testPrefixesKeepApplicationsApartOnOneRedis(): void
    {
        $a = $this->carrier('a:');
        $b = $this->carrier('b:');
        $received = new \ArrayObject();
        $receive = static function (string $channel, string $message) use ($received): void {
            $received[] = $message;
        };
        $gap = static function (): void {};
        $this->waitFor('Redis channel join confirmation', static fn() => $a->subscribe('orders', $receive, $gap));
        $this->waitFor('Redis channel join confirmation', static fn() => $b->subscribe('orders', $receive, $gap));
        $a->publish(['orders'], 'a only');
        $this->waitUntil('the publication for prefix a', static fn() => count($received) === 1);
        self::assertSame(['a only'], $received->getArrayCopy());
    }

    public function testConcurrentJoinsShareOneSubscriptionAndConfirmBeforeReturning(): void
    {
        $carrier = $this->carrier();
        $receive = static function (): void {};
        $gap = static function (): void {};
        $a = Task::spawn($this->loop, static fn() => $carrier->subscribe('a', $receive, $gap));
        $b = Task::spawn($this->loop, static fn() => $carrier->subscribe('b', $receive, $gap));
        $this->waitFor('completion of the a join task', static fn() => $a->await());
        $this->waitFor('completion of the b join task', static fn() => $b->await());
        self::assertCount(1, $this->broker->peers);
        self::assertSame(
            [
                ['SUBSCRIBE', 'app:a'],
                ['SUBSCRIBE', 'app:b'],
            ],
            $this->broker->commands,
        );
    }

    public function testConcurrentConnectionsJoiningOneChannelShareItsConfirmation(): void
    {
        $carrier = $this->carrier();
        $channels = $this->registries[] = new Channels($carrier);
        $a = new BroadcastRecipient();
        $b = new BroadcastRecipient();
        $first = Task::spawn($this->loop, static fn() => $channels->join($a, 'orders'));
        $second = Task::spawn($this->loop, static fn() => $channels->join($b, 'orders'));
        $this->waitFor('completion of the second join task', static fn() => $second->await());
        $this->waitFor('completion of the first join task', static fn() => $first->await());
        self::assertSame([['SUBSCRIBE', 'app:orders']], $this->broker->commands);
        new Broadcaster($carrier)->to('orders')->send('ready');
        $this->waitUntil(
            'delivery to both concurrently joined connections',
            static fn() => $a->messages !== [] && $b->messages !== [],
        );
        self::assertSame(['ready'], $a->messages);
        self::assertSame(['ready'], $b->messages);
    }

    public function testAFailedFirstJoinCanBeRetriedWithoutLosingItsConsumer(): void
    {
        $carrier = $this->carrier();
        $this->broker->available = false;
        $received = new \ArrayObject();
        $receive = static function (string $channel, string $message) use ($received): void {
            $received[] = $message;
        };
        $gap = static function (): void {};
        try {
            $this->waitFor('Redis channel join confirmation', static fn() => $carrier->subscribe(
                'orders',
                $receive,
                $gap,
            ));
            self::fail('An unavailable store accepted the join.');
        } catch (\Crab\Redis\RedisException) {
        }
        $this->broker->available = true;
        $this->waitFor('Redis channel join confirmation', static fn() => $carrier->subscribe('orders', $receive, $gap));
        $carrier->publish(['orders'], 'retried');
        $this->waitUntil('delivery after retrying the failed join', static fn() => count($received) === 1);
        self::assertSame(['retried'], $received->getArrayCopy());
    }

    public function testAnOutageIsReportedOnceAndClosesJoinedSocketsToRefetch(): void
    {
        $carrier = $this->carrier();
        $channels = $this->registries[] = new Channels($carrier);
        $socket = new BroadcastRecipient();
        $this->waitFor('Redis channel join confirmation', static fn() => $channels->join($socket, 'orders'));
        $this->broker->available = false;
        $this->broker->close();
        $this->waitUntil('joined sockets to close after a Redis outage', static fn() => $socket->ended !== null);
        // Observe a bounded retry interval to detect duplicate outage reports.
        Async::sleep($this->loop, 0.35);
        self::assertSame([1012, 'Reconnect and refetch state'], $socket->ended);
        self::assertSame(0, $channels->stats()['connections']);
        self::assertCount(1, $this->reports);
        self::assertStringContainsString('connection gap', $this->reports[0][0]);
        $this->broker->available = true;
        $replacement = new BroadcastRecipient();
        $this->waitFor('Redis channel join confirmation', static fn() => $channels->join($replacement, 'orders'));
        new Broadcaster($carrier)->to('orders')->send('fresh');
        $this->waitUntil('delivery on the replacement subscription', static fn() => $replacement->messages !== []);
        self::assertSame(['fresh'], $replacement->messages);
    }

    public function testStopCancelsFanOutAndAllSubscriptionWatchersImmediately(): void
    {
        $carrier = $this->carrier();
        $channels = $this->registries[] = new Channels($carrier);
        $socket = new BroadcastRecipient();
        $this->waitFor('Redis channel join confirmation', static fn() => $channels->join($socket, 'orders'));
        new Broadcaster($carrier)->to('orders')->send('pending');
        $channels->close();
        $this->broker->close();
        self::assertSame([], $this->loop->getIdentifiers());
        self::assertSame(0, $channels->stats()['channels']);
    }

    public function testAnUnopenedSubscriptionAndStopDuringConfirmationLeaveNoWatcher(): void
    {
        $client = $this->clients[] = new EngineClient('redis://127.0.0.1:6399');
        $unopened = $client->subscription();
        self::assertSame([], $this->loop->getIdentifiers());
        $unopened->close();
        self::assertSame([], $this->loop->getIdentifiers());

        $this->broker->confirm = false;
        $carrier = $this->carrier();
        $task = Task::spawn($this->loop, static fn() => $carrier->subscribe(
            'orders',
            static function (): void {},
            static function (): void {},
        ));
        $this->waitUntil(
            'the pending subscription connection',
            fn() => count($this->broker->peers) === 1 && count($this->broker->commands) === 1,
        );
        self::assertCount(1, $this->broker->peers);
        $carrier->close();
        $this->broker->close();
        self::assertSame([], $this->loop->getIdentifiers());
        try {
            $this->waitFor('the stopped join task to fail', static fn() => $task->await());
            self::fail('A stopped carrier confirmed its join.');
        } catch (\Crab\Redis\RedisException|\LogicException $error) {
            self::assertStringContainsString('closed', $error->getMessage());
        }
    }

    public function testAMissingConfirmationFailsAtTheTestDeadlineWithItsWaitReason(): void
    {
        $this->broker->confirm = false;
        $carrier = $this->carrier();
        try {
            $this->waitFor(
                'Redis to confirm orders',
                static fn() => $carrier->subscribe('orders', static function (): void {}, static function (): void {}),
                0.02,
            );
            self::fail('A subscription without confirmation returned.');
        } catch (\PHPUnit\Framework\AssertionFailedError $error) {
            self::assertSame('Timed out after 0.02 seconds waiting for Redis to confirm orders.', $error->getMessage());
        } finally {
            $carrier->close();
        }
    }

    public function testProviderStopClosesACarrierResolvedDirectlyByItsContract(): void
    {
        $container = new \Crab\Container\Container();
        $container->instance(\Crab\Contracts\Config\Config::class, new \Crab\Config\Config([
            'broadcast' => ['carrier' => 'redis', 'prefix' => 'provider:'],
        ]));
        $broadcastProvider = new \Crab\Broadcast\BroadcastProvider();
        $broadcastProvider->register($container);
        $provider = new \Crab\Redis\RedisProvider();
        $provider->register($container);
        $client = $this->clients[] = new PairClient($this->broker);
        $container->instance(Client::class, $client);
        $carrier = $this->carriers[] = $container->get(\Crab\Contracts\Broadcast\Carrier::class);
        $this->waitFor('Redis channel join confirmation', static fn() => $carrier->subscribe(
            'orders',
            static function (): void {},
            static function (): void {},
        ));
        $worker = new \Crab\Engine\Process\Worker($this->loop, new \Crab\Engine\Crab('test'), 0, [], 1.0);
        $provider->stop($worker, $container);
        $broadcastProvider->stop($worker, $container);
        $this->broker->close();
        self::assertSame([], $this->loop->getIdentifiers());
    }

    public function testAFailedOldJoinPreservesTheConfirmedReplacementReceiverAndGapCallback(): void
    {
        $carrier = $this->carrier();
        $channels = $this->registries[] = new Channels($carrier);
        $a = new BroadcastRecipient();
        $b = new BroadcastRecipient();
        $this->broker->confirm = false;
        $first = Task::spawn($this->loop, static fn() => $channels->join($a, 'orders'));
        $this->waitUntil('the old SUBSCRIBE awaiting confirmation', fn() => count($this->broker->commands) === 1);
        $channels->forget($a);
        $second = Task::spawn($this->loop, static fn() => $channels->join($b, 'orders'));
        $this->waitUntil(
            'the replacement membership to install its receiver',
            static fn() => $channels->stats()['connections'] === 1,
        );
        $this->broker->confirm = true;
        $this->broker->close();
        try {
            $this->waitFor('the failed old SUBSCRIBE', static fn() => $first->await());
            self::fail('The interrupted old join succeeded.');
        } catch (\Crab\Redis\RedisException) {
        }
        $this->waitFor('replacement SUBSCRIBE confirmation', static fn() => $second->await());
        $carrier->publish(['orders'], 'after confirmed replacement');
        $this->waitUntil('delivery to the confirmed replacement', static fn() => $b->messages !== []);
        self::assertSame(['after confirmed replacement'], $b->messages);
        self::assertSame(1, $channels->stats()['connections']);
        $this->broker->available = false;
        $this->broker->close();
        $this->waitUntil('the replacement gap callback to close its recipient', static fn() => $b->ended !== null);
        self::assertSame([1012, 'Reconnect and refetch state'], $b->ended);
        self::assertSame(0, $channels->stats()['connections']);
    }

    public function testAGapClosesHealthySocketsWhileAnotherSocketIsDraining(): void
    {
        $this->gapWithClosingSocket(false);
    }

    public function testAGapClosesHealthySocketsWhileAnotherSocketHasTimedOut(): void
    {
        $this->gapWithClosingSocket(true);
    }

    private function gapWithClosingSocket(bool $silent): void
    {
        $carrier = $this->carrier();
        $app = new \Crab\App(new \Crab\Config\Config([
            'websocket' => ['ping' => $silent ? 0.01 : 0],
            'log' => ['path' => '/dev/null'],
        ]));
        $app->register(...\Crab\App::PROVIDERS);
        $app->container->instance(\Crab\Contracts\Broadcast\Carrier::class, $carrier);
        $sockets = new \ArrayObject();
        $app->router->websocket('/live/{channel}', static function (
            \Crab\Contracts\WebSocket\Socket $socket,
            string $channel,
        ) use ($sockets): void {
            $sockets[$channel] = $socket;
            $socket->join($channel);
            $socket->send('ready');
            foreach ($socket as $message) {
            }
        });
        $api = $this->apis[] = new \Crab\Testing\Api($app);
        $a = $api->websocket('/live/orders');
        $a->assertReceived('ready');
        $b = $api->websocket('/live/quiet');
        $b->assertReceived('ready');
        $channels = $this->registries[] = $app->container->get(Channels::class);
        $server = new \ReflectionProperty($a, 'server')->getValue($a);
        $peer = new \ReflectionProperty($a, 'client')->getValue($a);
        $peer->pauseRead();
        try {
            if (!$silent) {
                $server->drain(1.0);
            }
            $this->waitUntil('the first socket to enter its closing state', static fn() => $sockets['orders']->isClosed());
            $this->broker->available = false;
            $this->broker->close();
            $this->waitUntil(
                'the Redis gap to release every membership',
                static fn() => $channels->stats()['connections'] === 0,
            );
            $b->assertClosed(1012);
            self::assertSame(0, $channels->stats()['channels']);
            self::assertCount(1, $this->reports);
            self::assertStringContainsString('connection gap', $this->reports[0][0]);
            $sockets['orders']->close();
            $sockets['orders']->close();
        } finally {
            $peer->resumeRead();
        }
    }

    public function testAGapCallbackFailureDoesNotSkipOtherCallbacks(): void
    {
        $carrier = $this->carrier();
        $called = new \ArrayObject();
        $receive = static function (): void {};
        $this->waitFor('first callback channel confirmation', static fn() => $carrier->subscribe(
            'a',
            $receive,
            static function (): void {
                throw new \RuntimeException('cleanup failed');
            },
        ));
        $this->waitFor('second callback channel confirmation', static fn() => $carrier->subscribe(
            'b',
            $receive,
            static function () use ($called): void {
                $called[] = true;
            },
        ));
        $this->broker->available = false;
        $this->broker->close();
        $this->waitUntil('the healthy gap callback despite the failed callback', static fn() => count($called) === 1);
        self::assertCount(2, $this->reports);
        self::assertSame('cleanup failed', $this->reports[1][1]->getMessage());
    }

    public function testReadyAfterJoinIsTheBarrierForReceivingPublications(): void
    {
        $carrier = $this->carrier('ready:');
        $this->broker->confirm = false;
        $app = new \Crab\App(new \Crab\Config\Config(['websocket' => ['ping' => 0], 'log' => ['path' => '/dev/null']]));
        $app->register(...\Crab\App::PROVIDERS);
        $app->container->instance(\Crab\Contracts\Broadcast\Carrier::class, $carrier);
        $app->router->websocket('/live', static function (\Crab\Contracts\WebSocket\Socket $socket): void {
            $socket->join('orders');
            $socket->send('ready');
            foreach ($socket as $message) {
            }
        });
        $api = $this->apis[] = new \Crab\Testing\Api($app);
        $client = $api->websocket('/live');
        $this->registries[] = $app->container->get(Channels::class);
        self::assertSame([], $this->broker->commands, 'The wire 101 does not confirm channel membership.');
        $carrier->publish(['orders'], 'immediately after open');
        $this->waitUntil('the held SUBSCRIBE confirmation', fn() => count($this->broker->commands) === 1);
        $this->broker->confirm = true;
        new \ReflectionMethod($this->broker, 'reply')->invoke($this->broker, 0, ['subscribe', 'ready:orders', 1]);
        $client->assertReceived('ready');
        $client->assertNothingReceived();
        $carrier->publish(['orders'], 'after ready');
        $client->assertReceived('after ready');
    }

    public function testRealRedisCarriersExchangeMessagesAndRespectPrefixIsolation(): void
    {
        $url = getenv('CRAB_TEST_REDIS_URL');
        if ($url === false || $url === '') {
            self::markTestSkipped('The orchestrator supplies CRAB_TEST_REDIS_URL for its throwaway Redis.');
        }
        if ((parse_url($url, PHP_URL_PORT) ?? 6379) !== 6399) {
            self::markTestSkipped(
                'Use the throwaway Redis on port 6399 for broadcast integration; port 6379 is off limits.',
            );
        }
        $aClient = $this->clients[] = new EngineClient($url);
        $bClient = $this->clients[] = new EngineClient($url);
        $prefix = 'broadcast-test:' . bin2hex(random_bytes(8)) . ':';
        $a = $this->carriers[] = new BroadcastCarrier($aClient, $prefix);
        $b = $this->carriers[] = new BroadcastCarrier($bClient, $prefix);
        $other = $this->carriers[] = new BroadcastCarrier($bClient, $prefix . 'other:');
        $received = new \ArrayObject();
        $receivedA = new \ArrayObject();
        $receive = static function (string $channel, string $message) use ($received): void {
            $received[] = [$channel, $message];
        };
        $gap = static function (): void {};
        $this->waitFor('Redis channel join confirmation', static fn() => $a->subscribe(
            'orders',
            static function (string $channel, string $message) use ($receivedA): void {
                $receivedA[] = [$channel, $message];
            },
            $gap,
        ));
        $this->waitFor('Redis channel join confirmation', static fn() => $b->subscribe('orders', $receive, $gap));
        $this->waitFor('Redis channel join confirmation', static fn() => $other->subscribe('orders', $receive, $gap));
        $this->waitFor('Redis PUBLISH from a to b', static fn() => $a->publish(['orders'], 'a to b'));
        $this->waitUntil(
            'the a to b message on both Redis subscribers',
            static fn() => count($received) === 1 && count($receivedA) === 1,
        );
        self::assertSame([['orders', 'a to b']], $received->getArrayCopy());
        $this->waitFor('Redis PUBLISH from b to a', static fn() => $b->publish(['orders'], 'b to a'));
        $this->waitUntil(
            'the b to a message on both Redis subscribers',
            static fn() => count($received) === 2 && count($receivedA) === 2,
        );
        self::assertSame([['orders', 'a to b'], ['orders', 'b to a']], $receivedA->getArrayCopy());
        self::assertSame(
            [$prefix . 'orders', 2],
            $this->waitFor('Redis PUBSUB NUMSUB reply', static fn() => $aClient->command(
                'PUBSUB',
                'NUMSUB',
                $prefix . 'orders',
            )),
        );
        $b->unsubscribe('orders');
        $this->waitUntil(
            'Redis to confirm the last leave',
            static fn() => $aClient->command('PUBSUB', 'NUMSUB', $prefix . 'orders') === [$prefix . 'orders', 1],
        );
        self::assertSame(
            [$prefix . 'orders', 1],
            $this->waitFor('Redis PUBSUB NUMSUB reply', static fn() => $aClient->command(
                'PUBSUB',
                'NUMSUB',
                $prefix . 'orders',
            )),
        );
    }
}

/**
 * The ordinary publish client shares a broker; subscriptions still use
 * the production Subscription parser, ordering and reconnect behaviour.
 */
final class PairClient implements Client
{
    private array $subscriptions = [];

    public function __construct(
        private readonly PairBroker $broker,
    ) {}

    public function command(string $name, string|int|float ...$arguments): mixed
    {
        throw new \LogicException('This fixture supports publish and subscribe only.');
    }

    public function publish(string $channel, string $payload): int
    {
        return $this->broker->publish($channel, $payload);
    }

    public function subscribe(string ...$channels): Subscription
    {
        $subscription = $this->subscription();
        $subscription->add(...$channels);
        return $subscription;
    }

    public function subscription(): Subscription
    {
        $subscription = new Subscription(fn() => $this->broker->connect());
        $this->subscriptions[] = $subscription;
        return $subscription;
    }

    public function close(): void
    {
        foreach ($this->subscriptions as $subscription) {
            $subscription->close();
        }
        $this->subscriptions = [];
    }
}

/**
 * RESP acknowledgements and publications over owned socket pairs. No
 * listening port or installed Redis is needed for focused tests.
 */
final class PairBroker
{
    public array $peers = [];
    public array $commands = [];
    public bool $available = true;
    public bool $confirm = true;
    private array $watchers = [];
    private array $input = [];
    private array $channels = [];
    private int $nextId = 0;

    public function __construct(
        private readonly EventLoop $loop,
    ) {}

    public function connect(): mixed
    {
        if (!$this->available) {
            throw new \Crab\Redis\RedisException('Fixture Redis is unavailable.');
        }
        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        stream_set_blocking($pair[0], false);
        stream_set_blocking($pair[1], false);
        $id = $this->nextId++;
        $this->peers[$id] = $pair[1];
        $this->input[$id] = '';
        $this->channels[$id] = [];
        $this->watchers[$id] = $this->loop->onReadable($pair[1], fn() => $this->read($id));
        return $pair[0];
    }

    private function read(int $id): void
    {
        $this->input[$id] .= fread($this->peers[$id], 65_536);
        if (feof($this->peers[$id])) {
            $this->removePeer($id);
            return;
        }
        while (true) {
            $offset = 0;
            $words = Resp::reply($this->input[$id], $offset, $complete);
            if (!$complete) {
                return;
            }
            $this->input[$id] = substr($this->input[$id], $offset);
            $this->commands[] = $words;
            foreach (array_slice($words, 1) as $channel) {
                if ($words[0] === 'SUBSCRIBE') {
                    $this->channels[$id][$channel] = true;
                } else {
                    unset($this->channels[$id][$channel]);
                }
                if ($this->confirm) {
                    $this->reply($id, [strtolower($words[0]), $channel, count($this->channels[$id])]);
                }
            }
        }
    }

    private function reply(int $id, array $values): void
    {
        $wire = '*' . count($values) . "\r\n";
        foreach ($values as $value) {
            $wire .= is_int($value) ? ':' . $value . "\r\n" : '$' . strlen($value) . "\r\n" . $value . "\r\n";
        }
        fwrite($this->peers[$id], $wire);
    }

    public function publish(string $channel, string $payload): int
    {
        $receivers = 0;
        foreach ($this->channels as $id => $channels) {
            if (!isset($channels[$channel])) {
                continue;
            }

            $this->reply($id, ['message', $channel, $payload]);
            ++$receivers;
        }
        return $receivers;
    }

    private function removePeer(int $id): void
    {
        $this->loop->cancel($this->watchers[$id]);
        fclose($this->peers[$id]);
        unset($this->peers[$id], $this->watchers[$id], $this->channels[$id], $this->input[$id]);
    }

    public function close(): void
    {
        foreach (array_keys($this->peers) as $id) {
            $this->removePeer($id);
        }
    }
}

/**
 * Receipts and close reasons observe worker-local fan-out without a port.
 */
final class BroadcastRecipient implements Recipient
{
    public array $messages = [];
    public ?array $ended = null;

    public function broadcast(string $message): WriteResult
    {
        if ($this->ended !== null) {
            return WriteResult::Closed;
        }
        $this->messages[] = $message;
        return WriteResult::Accepted;
    }

    public function close(int $code = 1000, string $reason = ''): void
    {
        $this->ended = [$code, $reason];
    }

    public function broadcastTimeout(): float
    {
        return 0.02;
    }

    public function awaitBroadcastWritable(Cancellation $cancellation): bool
    {
        $cancellation->throwIfCancelled();
        return $this->ended === null;
    }
}
