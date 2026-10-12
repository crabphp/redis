<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Cache\Cache;
use Crab\Contracts\Cache\Lock;
use Crab\Redis\CacheStore;
use Crab\Testing\FrozenClock;

final class CacheStoreTest extends RedisTestCase
{
    public function testTakingAnEntryReturnsItOnceAndChecksTheCallersExpiry(): void
    {
        $store = new CacheStore($this->redis, $this->prefix);
        $store->set('live', "binary:\0\xff\n", 130);
        $store->set('expired', 'old', 130);
        $store->set('empty', '', null);
        self::assertSame("binary:\0\xff\n", $store->take('live', 129));
        self::assertNull($store->take('live', 129));
        self::assertNull($store->take('expired', 130));
        self::assertSame('', $store->take('empty', 1_000_000));
        self::assertNull($store->take('empty', 100));
        self::assertNull($store->get('expired', 100));
        $cache = new Cache($store);
        $cache->set('value', false);
        self::assertFalse($cache->take('value', 'missing'));
        self::assertSame('missing', $cache->take('value', 'missing'));
    }

    public function testTwoConcurrentRedisConsumersTakeExactlyOneValue(): void
    {
        $loop = \Crab\Engine\EventLoop::current();
        $store = new CacheStore($this->redis, $this->prefix);
        $store->set('once', 'winner', null);
        $tasks = [];
        for ($i = 0; $i < 2; ++$i) {
            $tasks[] = \Crab\Engine\Async\Task::spawn($loop, function (): ?string {
                $client = new \Crab\Redis\EngineClient($this->url, replyTimeout: 2);
                try {
                    return (new CacheStore($client, $this->prefix))->take('once', 100);
                } finally {
                    $client->close();
                }
            });
        }
        $values = array_map(static fn ($task): ?string => $task->await(), $tasks);
        $loop->run();
        self::assertSame(1, count(array_filter($values, static fn ($value): bool => $value === 'winner')));
        self::assertSame(1, count(array_filter($values, static fn ($value): bool => $value === null)));
    }

    public function testALockCanBeTakenByOneHolderAndTakenAgainAfterRelease(): void
    {
        $clock = new FrozenClock('2026-01-31 12:00:00');
        $cache = new Cache(new CacheStore($this->redis, $this->prefix), clock: $clock);

        $lock = $cache->lock('report', 60);
        self::assertInstanceOf(Lock::class, $lock);
        self::assertNull($cache->lock('report', 60));
        self::assertTrue($lock->release());
        $next = $cache->lock('report', 60);
        self::assertInstanceOf(Lock::class, $next);
        self::assertTrue($next->release());
    }

    public function testAnExpiredHolderCannotReleaseTheLockTakenInItsPlace(): void
    {
        $clock = new FrozenClock('2026-01-31 12:00:00');
        $cache = new Cache(new CacheStore($this->redis, $this->prefix), clock: $clock);
        $first = $cache->lock('report', 10);
        self::assertInstanceOf(Lock::class, $first);

        $clock->advance('+11 seconds');
        $second = $cache->lock('report', 10);
        self::assertInstanceOf(Lock::class, $second);
        self::assertFalse($first->release());
        self::assertNull($cache->lock('report', 10));
        self::assertTrue($second->release());
    }

    public function testALockCanBeReleasedOnlyOnce(): void
    {
        $clock = new FrozenClock('2026-01-31 12:00:00');
        $cache = new Cache(new CacheStore($this->redis, $this->prefix), clock: $clock);
        $lock = $cache->lock('report', 60);
        self::assertInstanceOf(Lock::class, $lock);

        self::assertTrue($lock->release());
        self::assertFalse($lock->release());
    }

    public function testAnEntryIsDeletedOnlyWhenItIsLiveAndItsValueMatchesExactly(): void
    {
        $clock = new FrozenClock('2026-01-31 12:00:00');
        $store = new CacheStore($this->redis, $this->prefix);
        $cache = new Cache($store, clock: $clock);
        $cache->set('owned', "token:\0\n", 10);
        $cache->set('expired', 'old', 1);
        $cache->set('forever', 'kept');
        $now = $clock->now()->getTimestamp();

        self::assertFalse($store->deleteIf('missing', serialize('token'), $now));
        self::assertFalse($store->deleteIf('owned', serialize("Token:\0\n"), $now));
        self::assertSame("token:\0\n", $cache->get('owned'));
        $clock->advance('+1 second');
        $now = $clock->now()->getTimestamp();
        self::assertFalse($store->deleteIf('expired', serialize('old'), $now));
        self::assertNull($cache->get('expired'));
        self::assertTrue($store->deleteIf('owned', serialize("token:\0\n"), $now));
        self::assertNull($cache->get('owned'));
        self::assertFalse($store->deleteIf('owned', serialize("token:\0\n"), $now));
        self::assertTrue($store->deleteIf('forever', serialize('kept'), $now));
        self::assertNull($cache->get('forever'));
        $store->set('empty', '', null);
        self::assertTrue($store->deleteIf('empty', '', $now));
    }

    public function testRedisNeedsNoPruningBecauseItDropsEntriesItself(): void
    {
        $clock = new FrozenClock('2026-01-31 12:00:00');
        $store = new CacheStore($this->redis, $this->prefix);
        $cache = new Cache($store, clock: $clock);
        $cache->set('expired', 'old', 10);
        $cache->set('live', 'new', 100);
        $cache->set('forever', 'kept');
        $clock->advance('+11 seconds');

        self::assertSame(0, $store->prune($clock->now()->getTimestamp()));
        self::assertSame([null, 'new', 'kept'], array_values($cache->getMultiple(['expired', 'live', 'forever'])));
    }

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

    public function testIncrementCountsAsOneStepAndKeepsTheTimeTheCountStartedWith(): void
    {
        $clock = new FrozenClock('2026-01-31 12:00:00');
        $cache = new Cache(new CacheStore($this->redis, $this->prefix), clock: $clock);

        self::assertSame(1, $cache->increment('hits', ttl: 60));
        self::assertSame(2, $cache->increment('hits', ttl: 600));
        self::assertSame(7, $cache->increment('hits', 5));
        self::assertSame(4, $cache->increment('hits', -3));
        self::assertSame(4, $cache->get('hits'));

        $clock->advance('+60 seconds');
        self::assertFalse($cache->has('hits'));
        self::assertSame(1, $cache->increment('hits'));
        // Counted for ever, and never dropped by Redis.
        self::assertSame(-1, $this->redis->command('TTL', $this->prefix . 'hits'));

        $cache->set('seen', 40, 30);
        self::assertSame(41, $cache->increment('seen', ttl: 600));
        $clock->advance('+30 seconds');
        self::assertFalse($cache->has('seen'));
        $cache->set('text', 'not a number');
        self::assertSame(1, $cache->increment('text'));
        self::assertSame(-2, $cache->increment('below', -2));
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
