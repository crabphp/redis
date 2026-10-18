<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Contracts\Broadcast\Carrier;
use Crab\Contracts\Cache\Store as CacheStoreContract;
use Crab\Contracts\Config\Config;
use Crab\Contracts\Container\Container;
use Crab\Contracts\Queue\Store as QueueStoreContract;
use Crab\Contracts\WorkerProvider;
use Crab\Engine\Process\Worker;

/**
 * Binds a Redis client from the "redis" configuration section, and
 * keeps cache and queue entries and carries broadcasts in Redis when chosen:
 *
 *     url      where the server is; "redis://127.0.0.1:6379" unless set
 *     prefix   put before every key, to keep two applications apart on one server; "crab:"
 *     timeout  seconds to wait for a connection before a command fails; 2
 *     reply_timeout  seconds to wait for a command's reply, besides any blocking timeout; 5
 *     tls      PHP's "ssl" context options for a "rediss://" address, such as "cafile"; none unless set
 *
 * With "cache.store" set to "redis" the cache's entries are kept there,
 * and with "queue.store" set to "redis" the queue's jobs are. Register
 * broadcast.carrier set to redis carries updates between workers, using
 * broadcast.prefix for its channel names. Register it in app.php after the
 * default framework providers, whose stores and carrier it
 * takes the place of:
 *
 *     ->withProviders([RedisProvider::class, AppProvider::class])
 */
final class RedisProvider implements WorkerProvider
{
    public function register(Container $container): void
    {
        $config = $container->get(Config::class);
        $config->defaults('redis', [
            'url' => 'redis://127.0.0.1:6379',
            'prefix' => 'crab:',
            'timeout' => 2.0,
            'reply_timeout' => 5.0,
            'tls' => [],
        ]);
        $container->singleton(
            Client::class,
            static fn(Container $container): Client => new EngineClient(
                (string) $container->get(Config::class)->get('redis.url'),
                (float) $container->get(Config::class)->get('redis.timeout'),
                (array) $container->get(Config::class)->get('redis.tls'),
                replyTimeout: (float) $container->get(Config::class)->get('redis.reply_timeout'),
            ),
        );
        if ($config->get('cache.store') === 'redis') {
            $container->singleton(
                CacheStoreContract::class,
                static fn(Container $container): CacheStore => new CacheStore(
                    $container->get(Client::class),
                    $container->get(Config::class)->get('redis.prefix') . 'cache:',
                ),
            );
        }
        if ($config->get('queue.store') === 'redis') {
            $container->singleton(
                QueueStoreContract::class,
                static fn(Container $container): QueueStore => new QueueStore(
                    $container->get(Client::class),
                    $container->get(Config::class)->get('redis.prefix') . 'queue:',
                ),
            );
        }
        if ($config->get('broadcast.carrier') === 'redis') {
            $container->singleton(
                Carrier::class,
                static fn(Container $container): BroadcastCarrier => new BroadcastCarrier(
                    $container->get(Client::class),
                    (string) $container->get(Config::class)->get('broadcast.prefix'),
                ),
            );
        }
    }

    public function start(Worker $worker, Container $container): void
    {
        // Connections resolve the worker's loop on first use.
    }

    public function stop(Worker $worker, Container $container): void
    {
        if ($container->get(Config::class)->get('broadcast.carrier') === 'redis') {
            // Direct users of the carrier contract bypass the framework's
            // broadcaster lifetime; its monitor belongs to this provider too.
            $container->get(Carrier::class)->close();
        }
        $container->get(Client::class)->close();
    }
}
