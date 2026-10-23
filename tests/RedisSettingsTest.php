<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\App;
use Crab\Broadcast\BroadcastProvider;
use Crab\Cache\CacheProvider;
use Crab\Config\Config;
use Crab\Contracts\Config\InvalidSettings;
use Crab\Queue\QueueProvider;
use Crab\Redis\Client;
use Crab\Redis\RedisProvider;
use PHPUnit\Framework\TestCase;

/**
 * Redis's own settings are checked with the application's, and "redis"
 * is one more choice for the stores and the carrier it can stand behind.
 * Nothing here needs a Redis server.
 */
final class RedisSettingsTest extends TestCase
{
    public function testRedisIsAChoiceWhereTheProviderItIsAChoiceOfIsRegistered(): void
    {
        $app = new App(new Config([
            'cache' => ['store' => 'redis'],
            'queue' => ['store' => 'redis'],
            'broadcast' => ['carrier' => 'redis'],
        ]));
        $app->register(CacheProvider::class, QueueProvider::class, BroadcastProvider::class, RedisProvider::class);
        self::assertSame([], $app->config->problems());
        self::assertSame(['file', 'database', 'array', 'redis'], $app->config->rules()['cache']['store']['enum']);

        // Without the cache provider there is no cache to offer a store to, and no rules for one.
        $alone = new App(new Config());
        $alone->register(RedisProvider::class);
        self::assertSame(['app', 'log', 'server', 'websocket', 'redis'], array_keys($alone->config->rules()));
    }

    public function testRedisSettingsThatDoNotFitAreRefusedWithoutRepeatingAPassword(): void
    {
        $app = new App(new Config(['redis' => [
            'url' => 'http://:hunter2@cache:6379',
            'timeout' => 0,
            'reply_timout' => 5,
        ]]));
        $app->register(RedisProvider::class);
        try {
            $app->container->get(Client::class);
            self::fail('A client with settings that do not fit is not made.');
        } catch (InvalidSettings $error) {
            self::assertSame(
                [
                    'redis.url is "http://:(hidden)@cache:6379"; it takes a string, such as "redis://127.0.0.1:6379".',
                    'redis.timeout is 0; it takes a number greater than 0.',
                    'redis.reply_timout is not a setting; redis takes url, prefix, timeout, reply_timeout and tls. Did you mean redis.reply_timeout?',
                ],
                $error->problems(),
            );
        }
    }
}
