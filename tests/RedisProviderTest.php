<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\App;
use Crab\Cache\CacheProvider;
use Crab\Config\Config;
use Crab\Container\Container;
use Crab\Contracts\Config\Config as ConfigContract;
use Crab\Contracts\Container\Container as ContainerContract;
use Crab\Console\Output;
use Crab\Contracts\Cache\Cache;
use Crab\Contracts\Cache\Store as CacheStoreContract;
use Crab\Contracts\Queue\Queue;
use Crab\Contracts\Queue\Store as QueueStoreContract;
use Crab\Queue\QueueProvider;
use Crab\Redis\EngineClient;
use Crab\Redis\CacheStore;
use Crab\Redis\Client;
use Crab\Redis\QueueStore;
use Crab\Redis\RedisException;
use Crab\Redis\RedisProvider;
use Crab\Redis\Subscription;
use Crab\Engine\Process\Worker;
use Crab\Engine\Async\Async;
use Crab\Testing\WorkerHarness;

final class Sent
{
    /**
     * @var list<string>
     */
    public static array $to = [];
    public static int $failures = 0;
}

final class SendEmail
{
    public function handle(string $to): void
    {
        if (Sent::$failures > 0) {
            --Sent::$failures;
            throw new \RuntimeException("refused {$to}");
        }
        Sent::$to[] = $to;
    }
}

final class RedisProviderTest extends RedisTestCase
{
    private string $log;

    protected function setUp(): void
    {
        parent::setUp();
        $this->log = sys_get_temp_dir() . '/crab-redis-' . bin2hex(random_bytes(6)) . '.log';
        Sent::$to = [];
        Sent::$failures = 0;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        if (isset($this->log) && is_file($this->log)) {
            unlink($this->log);
        }
    }

    public function testTheCacheAndTheQueueAreKeptInRedisWhereTheyAreSetTo(): void
    {
        $app = $this->app(['cache' => ['store' => 'redis'], 'queue' => ['store' => 'redis', 'tries' => 2, 'backoff' => [0]]]);

        self::assertInstanceOf(EngineClient::class, $app->container->get(Client::class));
        self::assertInstanceOf(CacheStore::class, $app->container->get(CacheStoreContract::class));
        self::assertInstanceOf(QueueStore::class, $app->container->get(QueueStoreContract::class));

        $cache = $app->container->get(Cache::class);
        $cache->set('greeting', ['hello' => 'crab'], 60);
        self::assertSame(['hello' => 'crab'], $cache->get('greeting'));
        self::assertSame(1, $this->redis->command('EXISTS', $this->prefix . 'cache:greeting'));

        // Jobs pushed, run by the queue's own command, one of them on its second try.
        $queue = $app->container->get(Queue::class);
        $queue->push(SendEmail::class, to: 'ada@example.com');
        $queue->on('bulk')->push(SendEmail::class, to: 'everyone@example.com');
        Sent::$failures = 1;
        [$output, $read] = $this->captured();
        self::assertSame(0, $app->console->run(['queue:work', '--queue=default,bulk', '--stop-when-empty'], $output));
        self::assertSame("Ran 3 jobs.\n", $read());
        self::assertEqualsCanonicalizing(['ada@example.com', 'everyone@example.com'], Sent::$to);

        // One that keeps failing is listed as failed, and put back by queue:retry.
        $queue->push(SendEmail::class, to: 'bob@example.com');
        Sent::$failures = 2;
        self::assertSame(0, $app->console->run(['queue:work', '--stop-when-empty'], $this->captured()[0]));
        [$output, $read] = $this->captured();
        self::assertSame(0, $app->console->run(['queue:failed'], $output));
        self::assertStringContainsString('refused bob@example.com', $read());
        self::assertSame(0, $app->console->run(['queue:retry', 'all'], $this->captured()[0]));
        self::assertSame(0, $app->console->run(['queue:work', '--stop-when-empty'], $this->captured()[0]));
        self::assertContains('bob@example.com', Sent::$to);
    }

    public function testStoresSetToSomethingElseAreLeftAlone(): void
    {
        $app = $this->app(['cache' => ['store' => 'array'], 'queue' => ['store' => 'memory']]);

        self::assertNotInstanceOf(CacheStore::class, $app->container->get(CacheStoreContract::class));
        self::assertNotInstanceOf(QueueStore::class, $app->container->get(QueueStoreContract::class));
        self::assertSame('redis://127.0.0.1:6379', (new Config())->get('redis.url', 'redis://127.0.0.1:6379'));
    }

    public function testTheReplyTimeoutDefaultsToFiveSeconds(): void
    {
        $app = $this->app([]);
        $redis = $app->container->get(Client::class);

        self::assertSame(5.0, $app->config->get('redis.reply_timeout'));
        self::assertSame(5.0, (new \ReflectionProperty($redis, 'replyTimeout'))->getValue($redis));
    }

    public function testStoppingTheWorkerClosesItsSubscriptionsAndOrdinaryConnection(): void
    {
        $container = new Container();
        $container->instance(ConfigContract::class, new Config(['redis' => ['url' => $this->url]]));
        $subscription = null;
        $redis = null;
        WorkerHarness::run([new RedisProvider()], function (Worker $worker, ContainerContract $container) use (&$subscription, &$redis): void {
            $redis = $container->get(Client::class);
            $redis->command('PING');
            $subscription = $redis->subscribe($this->prefix . 'worker');
            self::assertSame(1, $redis->publish($this->prefix . 'worker', 'before stop'));
        }, $container);

        self::assertInstanceOf(Subscription::class, $subscription);
        self::assertSame([], iterator_to_array($subscription));
        self::assertNull((new \ReflectionProperty($redis, 'stream'))->getValue($redis));
        self::assertSame(0, $this->redis->publish($this->prefix . 'worker', 'after stop'));
    }

    public function testProviderStopClosesAnOpeningTlsSocketWithoutAnotherLoopTurn(): void
    {
        $listener = stream_socket_server('tcp://127.0.0.1:0', $code, $reason);
        self::assertIsResource($listener, (string) $reason);
        stream_set_blocking($listener, false);
        $address = stream_socket_get_name($listener, false);
        $container = new Container();
        $container->instance(ConfigContract::class, new Config(['redis' => ['url' => 'rediss://' . $address, 'timeout' => 10.0]]));
        $socket = $peer = $worker = null;
        try {
            WorkerHarness::run([new RedisProvider()], static function (Worker $running, ContainerContract $container) use ($listener, $address, &$socket, &$peer, &$worker): void {
                $worker = $running;
                $accepted = $worker->loop->getSuspension();
                $worker->loop->onReadable($listener, static function (string $watcher) use ($listener, $worker, $accepted, &$peer): void {
                    $peer = stream_socket_accept($listener, 0);
                    $worker->loop->cancel($watcher);
                    $accepted->resume();
                });
                $redis = $container->get(Client::class);
                $worker->spawn(static function () use ($redis): void {
                    try {
                        $redis->subscribe('opening');
                    } catch (RedisException) {
                        // The opener will observe shutdown if the loop runs again.
                    }
                });
                $accepted->suspend();
                Async::sleep($worker->loop, 0.01);
                foreach (get_resources('stream') as $candidate) {
                    if (@stream_socket_get_name($candidate, true) === $address) {
                        $socket = $candidate;
                    }
                }
                self::assertIsResource($socket);
            }, $container);

            // Provider stop runs after the loop stops; do not await the opener here.
            self::assertFalse(is_resource($socket), 'Provider stop left its opening TLS socket alive.');
            self::assertSame([], $worker->loop->getIdentifiers());
        } finally {
            $worker?->loop->run();
            if (is_resource($peer)) {
                fclose($peer);
            }
            fclose($listener);
        }
    }

    public function testTheProviderPassesTheConfiguredReplyTimeoutToTheClient(): void
    {
        $app = $this->app(['redis' => ['url' => $this->url, 'reply_timeout' => 0.35]]);
        $redis = $app->container->get(Client::class);

        self::assertSame(0.35, $app->config->get('redis.reply_timeout'));
        self::assertSame(0.35, (new \ReflectionProperty($redis, 'replyTimeout'))->getValue($redis));
    }

    public function testAConfiguredReplyTimeoutOfZeroIsRefused(): void
    {
        $app = $this->app(['redis' => ['url' => $this->url, 'reply_timeout' => 0]]);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Set replyTimeout to a positive, finite number');
        $app->container->get(Client::class);
    }

    public function testAServerThatIsNotThereAndACommandItRefusesAreReported(): void
    {
        try {
            $this->redis->command('NOSUCHCOMMAND', 'x');
            self::fail('A command Redis does not have was accepted.');
        } catch (RedisException $error) {
            self::assertStringContainsString('NOSUCHCOMMAND', $error->getMessage());
        }

        $this->expectException(RedisException::class);
        $this->expectExceptionMessage('Redis at 127.0.0.1:1 could not be reached: the connection was refused');
        (new EngineClient('redis://127.0.0.1:1'))->command('GET', 'x');
    }

    /**
     * @param array<string, mixed> $settings
     */
    private function app(array $settings): App
    {
        $app = new App(new Config($settings + ['redis' => ['url' => $this->url, 'prefix' => $this->prefix], 'log' => ['path' => $this->log]]));
        $app->register(CacheProvider::class, QueueProvider::class, RedisProvider::class);
        return $app;
    }

    /**
     * @return array{Output, \Closure(): string} An output, and what has been written to it.
     */
    private function captured(): array
    {
        $stream = fopen('php://memory', 'w+');
        return [new Output($stream, $stream), static function () use ($stream): string {
            rewind($stream);
            return (string) stream_get_contents($stream);
        }];
    }
}
