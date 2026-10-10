<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Contracts\Cache\Store as CacheStoreContract;
use Crab\Contracts\Config\Config;
use Crab\Contracts\Container\Container;
use Crab\Contracts\Provider;
use Crab\Contracts\Queue\Store as QueueStoreContract;

/**
 * Binds a Redis client from the "redis" configuration section, and
 * keeps the cache and the queue in Redis where they are set to:
 *
 *     url      where the server is; "redis://127.0.0.1:6379" unless set
 *     prefix   put before every key, to keep two applications apart on one server; "crab:"
 *     timeout  seconds to wait for a connection before a command fails; 2
 *     tls      PHP's "ssl" context options for a "rediss://" address, such as "cafile"; none unless set
 *
 * With "cache.store" set to "redis" the cache's entries are kept there,
 * and with "queue.store" set to "redis" the queue's jobs are. Register
 * it after the cache and queue providers, whose stores it takes the
 * place of:
 *
 *     ->withProviders([CacheProvider::class, QueueProvider::class, RedisProvider::class, AppProvider::class])
 */
final class RedisProvider implements Provider
{
    public function register(Container $container): void
    {
        $config = $container->get(Config::class);
        $config->defaults('redis', [
            'url' => 'redis://127.0.0.1:6379',
            'prefix' => 'crab:',
            'timeout' => 2.0,
            'tls' => [],
        ]);
        $container->singleton(Client::class, static fn (Container $container): Client => new EngineClient(
            (string) $container->get(Config::class)->get('redis.url'),
            (float) $container->get(Config::class)->get('redis.timeout'),
            (array) $container->get(Config::class)->get('redis.tls'),
        ));
        if ($config->get('cache.store') === 'redis') {
            $container->singleton(CacheStoreContract::class, static fn (Container $container): CacheStore => new CacheStore(
                $container->get(Client::class),
                $container->get(Config::class)->get('redis.prefix') . 'cache:',
            ));
        }
        if ($config->get('queue.store') === 'redis') {
            $container->singleton(QueueStoreContract::class, static fn (Container $container): QueueStore => new QueueStore(
                $container->get(Client::class),
                $container->get(Config::class)->get('redis.prefix') . 'queue:',
            ));
        }
    }
}
