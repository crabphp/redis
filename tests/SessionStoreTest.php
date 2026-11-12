<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\App;
use Crab\Config\Config;
use Crab\Contracts\Session\Driver;
use Crab\Contracts\Session\Record;
use Crab\Redis\RedisPlugin;
use Crab\Redis\SessionStore;
use Crab\Session\SessionPlugin;

final class SessionStoreTest extends RedisTestCase
{
    public function testASessionIsKeptUnderARandomIdUntilItExpires(): void
    {
        $store = new SessionStore($this->redis, $this->prefix);
        $now = time();
        $id = $store->write(null, new Record(['cart' => [1, 2]], ['cart'], $now + 60));
        self::assertMatchesRegularExpression('/^[0-9a-f]{64}$/', $id);
        self::assertEquals(new Record(['cart' => [1, 2]], ['cart'], $now + 60), $store->read($id, $now));
        self::assertSame($id, $store->write($id, new Record(['cart' => [3]], [], $now + 60)));
        self::assertSame(['cart' => [3]], $store->read($id, $now)?->values);

        $ttl = $this->redis->command('TTL', $this->prefix . $id);
        self::assertGreaterThan(0, $ttl, 'Redis drops the session when it expires.');
        self::assertNull($store->read($id, $now + 61), 'An expired session is none, whatever Redis still holds.');
        self::assertNull($store->read('*', $now), 'Only an id is looked up.');
        self::assertSame(0, $store->sweep($now));

        $store->destroy($id);
        self::assertNull($store->read($id, $now));
    }

    public function testThePluginKeepsSessionsInRedisWhenChosen(): void
    {
        $app = new App(new Config(['session' => ['driver' => 'redis'], 'redis' => ['url' => $this->url]]));
        $app->register(SessionPlugin::class, RedisPlugin::class);
        self::assertSame([], $app->config->problems(), '"redis" is one of the drivers once the plugin is listed.');
        self::assertInstanceOf(SessionStore::class, $app->container->get(Driver::class));

        $this->expectException(\InvalidArgumentException::class);
        new SessionStore($this->redis, '');
    }
}
