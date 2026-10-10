<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Cache\Cache;
use Crab\Redis\CacheStore;
use Crab\Testing\FrozenClock;

final class CacheStoreTest extends RedisTestCase
{
    public function testAnEntryIsKeptUntilItsTimeByTheCallersClock(): void
    {
        $store = new CacheStore($this->redis, $this->prefix);
        $now = time();

        self::assertNull($store->get('rates', $now));
        $store->set('rates', 'a:value:with:colons', $now + 60);
        $store->set('forever', '', null);

        self::assertSame('a:value:with:colons', $store->get('rates', $now + 59));
        self::assertNull($store->get('rates', $now + 60));
        self::assertSame('', $store->get('forever', $now + 1_000_000));
        // Redis drops it on its own a minute after it expires; what never expires is never dropped.
        self::assertEqualsWithDelta(120, $this->redis->command('TTL', $this->prefix . 'rates'), 2);
        self::assertSame(-1, $this->redis->command('TTL', $this->prefix . 'forever'));

        $store->delete('rates');
        self::assertNull($store->get('rates', $now));

        // A time of zero is a time, long past, and not "never".
        $store->set('epoch', 'gone', 0);
        self::assertNull($store->get('epoch', 100));
        self::assertTrue($store->add('epoch', 'new', null, 100));
        self::assertSame('new', $store->get('epoch', 100));
        // And a time before zero is long past too.
        $store->set('before', 'gone', -1);
        self::assertTrue($store->add('before', 'new', null, 100));
    }

    public function testAddSetsOnlyWhatIsNotThereAndClearRemovesOnlyItsOwn(): void
    {
        $store = new CacheStore($this->redis, $this->prefix . 'a:');
        $other = new CacheStore($this->redis, $this->prefix . 'b:');
        $now = time();

        self::assertTrue($store->add('lock', 'first', $now + 10, $now));
        self::assertFalse($store->add('lock', 'second', $now + 10, $now));
        self::assertSame('first', $store->get('lock', $now));
        // Once it has expired, by the clock of the one who asks, it is free again.
        self::assertTrue($store->add('lock', 'third', $now + 30, $now + 10));
        self::assertSame('third', $store->get('lock', $now + 10));
        self::assertTrue($store->add('kept', 'x', null, $now));
        self::assertFalse($store->add('kept', 'y', null, $now + 1_000_000));

        $other->set('theirs', 'stays', null);
        for ($i = 0; $i < 700; ++$i) {
            $store->set("many:{$i}", 'x', null);
        }
        $store->clear();

        self::assertNull($store->get('many:699', $now));
        self::assertNull($store->get('lock', $now));
        self::assertSame('stays', $other->get('theirs', $now));

        $this->expectException(\InvalidArgumentException::class);
        new CacheStore($this->redis, '');
    }

    public function testTheFrameworksCacheWorksOnIt(): void
    {
        $clock = new FrozenClock('2026-01-31 12:00:00');
        $cache = new Cache(new CacheStore($this->redis, $this->prefix), clock: $clock);
        $made = 0;
        $make = static function () use (&$made): array {
            ++$made;
            return ['eur' => 1.08, 'at' => new \DateTimeImmutable('2026-01-31')];
        };

        $first = $cache->remember('rates', 60, $make);
        $second = $cache->remember('rates', 60, $make);
        self::assertSame(1, $made);
        self::assertEquals($first, $second);

        // The test's clock is far from the server's, and it is the test's that decides.
        $clock->advance('+61 seconds');
        $cache->remember('rates', 60, $make);
        self::assertSame(2, $made);
        self::assertTrue($cache->add('lock', true, 30));
        self::assertFalse($cache->add('lock', true, 30));
        self::assertTrue($cache->clear());
        self::assertFalse($cache->has('rates'));
    }
}
