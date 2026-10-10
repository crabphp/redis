<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Redis\QueueStore;

final class QueueStoreTest extends RedisTestCase
{
    private QueueStore $store;

    protected function setUp(): void
    {
        parent::setUp();
        $this->store = new QueueStore($this->redis, $this->prefix);
    }

    public function testJobsAreTakenInTheOrderPushedFromTheMostUrgentQueueFirst(): void
    {
        $now = 1_000;
        $first = $this->store->push('default', 'App\\SendEmail', ['userId' => 7, 'tags' => ['a', 'b']], $now, $now);
        $this->store->push('default', 'App\\SendEmail', ['userId' => 8], $now, $now);
        $this->store->push('high', 'App\\ResetPassword', ['userId' => 9], $now, $now);

        self::assertSame([2, 1, 0], [$this->store->size('default'), $this->store->size('high'), $this->store->size('other')]);

        $taken = [];
        while (($message = $this->store->reserve(['high', 'default'], $now, 90)) !== null) {
            $taken[] = [$message->queue, $message->job, $message->data, $message->attempts];
            if ($message->id === $first) {
                self::assertSame(['userId' => 7, 'tags' => ['a', 'b']], $message->data);
            }
        }

        self::assertSame([
            ['high', 'App\\ResetPassword', ['userId' => 9], 1],
            ['default', 'App\\SendEmail', ['userId' => 7, 'tags' => ['a', 'b']], 1],
            ['default', 'App\\SendEmail', ['userId' => 8], 1],
        ], $taken);
        // Taken and not yet done, they still count.
        self::assertSame(2, $this->store->size('default'));
    }

    public function testADelayedOrReleasedJobWaitsForItsTimeAndADoneOneIsForgotten(): void
    {
        $now = 1_000;
        $this->store->push('default', 'App\\Remind', [], $now + 600, $now);

        $this->store->push('default', 'App\\Now', [], $now, $now);
        self::assertSame(['oldest' => $now, 'pending' => 1, 'delayed' => 1, 'processing' => 0], $this->store->counts('default', $now + 5));
        $first = $this->store->reserve(['default'], $now, 90);
        self::assertSame(['oldest' => null, 'pending' => 0, 'delayed' => 1, 'processing' => 1], $this->store->counts('default', $now));
        // Its time come, the delayed one is pending since then, though no worker has asked yet.
        self::assertSame(['oldest' => $now + 600, 'pending' => 1, 'delayed' => 0, 'processing' => 1], $this->store->counts('default', $now + 700));
        $this->store->complete($first);

        self::assertNull($this->store->reserve(['default'], $now + 599, 90));
        $message = $this->store->reserve(['default'], $now + 600, 90);
        self::assertNotNull($message);
        self::assertSame(1, $message->attempts);

        // Tried again in thirty seconds, and that is its second attempt.
        $this->store->release($message, $now + 630);
        self::assertNull($this->store->reserve(['default'], $now + 629, 90));
        $again = $this->store->reserve(['default'], $now + 630, 90);
        self::assertSame([$message->id, 2], [$again?->id, $again?->attempts]);

        $this->store->complete($again);
        self::assertSame(0, $this->store->size('default'));
        self::assertNull($this->store->reserve(['default'], $now + 10_000, 90));
    }

    public function testAJobIsGivenToOneWorkerAndAgainWhenItsWorkerDied(): void
    {
        $now = 1_000;
        $this->store->push('default', 'App\\Export', [], $now, $now);

        $held = $this->store->reserve(['default'], $now, 90);
        self::assertNotNull($held);
        self::assertNull($this->store->reserve(['default'], $now + 89, 90));
        // Held longer than a worker may: it is taken to have died, and another gets the job.
        $second = $this->store->reserve(['default'], $now + 90, 90);
        self::assertSame([$held->id, 2], [$second?->id, $second?->attempts]);

        // Held too long again, but this time the worker finishes after all: the job is not run a third time.
        self::assertNull($this->store->reserve(['default'], $now + 179, 90));
        $this->store->push('default', 'App\\Next', [], $now, $now);
        $this->store->complete($second);
        $next = $this->store->reserve(['default'], $now + 400, 90);
        self::assertSame('App\\Next', $next?->job);
        self::assertNull($this->store->reserve(['default'], $now + 400, 90));
    }

    public function testAJobGivenUpOnIsKeptUntilItIsRetriedOrForgotten(): void
    {
        $now = 1_000;
        foreach ([1, 2, 3] as $id) {
            $this->store->push('mail', 'App\\SendEmail', ['id' => $id], $now, $now);
            $this->store->fail($this->store->reserve(['mail'], $now, 90), "refused {$id}", $now + $id);
        }

        $failed = $this->store->failed();
        self::assertSame([[1], [2], [3]], array_map(static fn ($message): array => array_values($message->data), $failed));
        self::assertSame(['mail', 'App\\SendEmail', 1, 'refused 1', $now + 1], [$failed[0]->queue, $failed[0]->job, $failed[0]->attempts, $failed[0]->error, $failed[0]->failedAt]);
        self::assertSame(0, $this->store->size('mail'));
        self::assertSame([[2], [3]], array_map(static fn ($message): array => array_values($message->data), $this->store->failed(2)));
        self::assertSame([], $this->store->failed(0));

        self::assertSame(0, $this->store->retry('no-such-id', $now));
        self::assertSame(1, $this->store->retry($failed[0]->id, $now));
        self::assertSame(0, $this->store->retry($failed[0]->id, $now));
        $retried = $this->store->reserve(['mail'], $now, 90);
        self::assertSame([['id' => 1], 1], [$retried?->data, $retried?->attempts]);

        self::assertSame(1, $this->store->forget($failed[1]->id));
        self::assertSame(0, $this->store->forget($failed[1]->id));
        self::assertCount(1, $this->store->failed());
        self::assertSame(1, $this->store->retry(null, $now));
        self::assertSame([], $this->store->failed());
        self::assertSame(0, $this->store->forget(null));
        self::assertSame(2, $this->store->size('mail'));
    }

    public function testAJobIsAlwaysInOnePlaceHoweverItIsMoved(): void
    {
        $now = 1_000;
        $this->store->push('default', 'App\\Export', [], $now, $now);
        $places = fn (): array => [
            $this->redis->command('LLEN', $this->prefix . 'ready:default'),
            $this->redis->command('ZCARD', $this->prefix . 'delayed:default'),
            $this->redis->command('ZCARD', $this->prefix . 'reserved:default'),
            $this->redis->command('ZCARD', $this->prefix . 'failed'),
        ];

        self::assertSame([1, 0, 0, 0], $places());
        $message = $this->store->reserve(['default'], $now, 90);
        self::assertSame([0, 0, 1, 0], $places());
        $this->store->release($message, $now);
        self::assertSame([0, 1, 0, 0], $places());
        // Released twice by mistake, or by a worker whose job was given to another: still once.
        $this->store->release($message, $now);
        self::assertSame([0, 1, 0, 0], $places());

        $again = $this->store->reserve(['default'], $now, 90);
        $this->store->fail($again, 'gave up', $now);
        self::assertSame([0, 0, 0, 1], $places());
        // A release that arrives after the job was given up on does not bring it back.
        $this->store->release($again, $now);
        self::assertSame([0, 0, 0, 1], $places());
        self::assertNull($this->store->reserve(['default'], $now + 1_000, 90));

        // Done, and then reported as failed by a worker that was slow to notice: it stays gone.
        self::assertSame(1, $this->store->retry(null, $now));
        $last = $this->store->reserve(['default'], $now, 90);
        $this->store->complete($last);
        $this->store->fail($last, 'too late', $now);
        self::assertSame([0, 0, 0, 0], $places());
        self::assertSame([], $this->store->failed());
    }

    public function testAWorkerThatWasSlowCannotUndoWhatTheNextOneHolds(): void
    {
        $now = 1_000;
        $this->store->push('default', 'App\\Export', [], $now, $now);
        $slow = $this->store->reserve(['default'], $now, 90);
        // Taken to have died, the job is given to another worker.
        $next = $this->store->reserve(['default'], $now + 90, 90);
        self::assertSame([$slow->id, 2], [$next?->id, $next?->attempts]);

        // The slow one wakes and reports a failure of its own try: the job is the other's now, and stays so.
        $this->store->release($slow, $now + 91);
        $this->store->fail($slow, 'too slow', $now + 91);
        self::assertSame(['oldest' => null, 'pending' => 0, 'delayed' => 0, 'processing' => 1], $this->store->counts('default', $now + 100));
        self::assertNull($this->store->reserve(['default'], $now + 100, 90));
        self::assertSame([], $this->store->failed());

        // The other puts it back to try later, and then the slow one finishes it after all: done is done.
        $this->store->release($next, $now + 200);
        $this->store->complete($slow);
        self::assertSame(['oldest' => null, 'pending' => 0, 'delayed' => 0, 'processing' => 0], $this->store->counts('default', $now + 300));
        self::assertNull($this->store->reserve(['default'], $now + 300, 90));
        self::assertSame([], $this->redis->command('KEYS', $this->prefix . 'job:*'));
    }

    public function testWhatHasWaitedLongestIsTakenFirstAndSaidToBeTheOldest(): void
    {
        $now = 1_000;
        $this->store->push('default', 'App\\DueFirst', [], $now + 100, $now);
        $this->store->push('default', 'App\\DueSecond', [], $now + 150, $now);
        // Pushed later, to run at once, before any worker has asked since the delayed ones came due.
        $this->store->push('default', 'App\\Immediate', [], $now + 200, $now + 200);

        self::assertSame($now + 100, $this->store->counts('default', $now + 250)['oldest']);
        $taken = [];
        while (($message = $this->store->reserve(['default'], $now + 250, 90)) !== null) {
            $taken[] = $message->job;
            if (count($taken) === 1) {
                // With the first gone, the oldest still waiting is the second.
                self::assertSame($now + 150, $this->store->counts('default', $now + 250)['oldest']);
            }
        }
        self::assertSame(['App\\DueFirst', 'App\\DueSecond', 'App\\Immediate'], $taken);

        // Put back after failing for good, a job waits from then, not from when it was first pushed.
        $this->store->push('mail', 'App\\Send', [], $now, $now);
        $this->store->fail($this->store->reserve(['mail'], $now, 90), 'refused', $now + 1);
        self::assertSame(1, $this->store->retry(null, $now + 5_000));
        self::assertSame($now + 5_000, $this->store->counts('mail', $now + 5_010)['oldest']);
    }
}
