<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Contracts\Queue\Message;
use Crab\Contracts\Queue\Store;

/**
 * Keeps the queue's jobs in Redis, for more workers than a database
 * table hands jobs to comfortably.
 *
 * Each job is a hash, "job:<id>". Its id waits in one of three places
 * for its queue: the list "ready:<queue>" when it may run, the sorted
 * set "delayed:<queue>" by the time it may run from, or the sorted set
 * "reserved:<queue>" by the time a worker took it. The ids of jobs given
 * up on are in the sorted set "failed", by when they failed.
 *
 * Every step that moves a job, from pushing it to forgetting it, is one
 * script, which Redis runs alone: a job is given to one worker at a time
 * however many ask, and is never in two places or in none. The scripts
 * build key names from the prefix, which a Redis Cluster does not allow:
 * this store is for one server, or a primary with replicas.
 */
final readonly class QueueStore implements Store
{
    /**
     * ARGV: prefix, queue, job class, data as JSON, the time it may run from, the time now.
     */
    private const PUSH = <<<'LUA'
        local prefix, queue = ARGV[1], ARGV[2]
        local id = redis.call('INCR', prefix .. 'id')
        redis.call('HSET', prefix .. 'job:' .. id, 'queue', queue, 'job', ARGV[3], 'data', ARGV[4], 'attempts', 0, 'at', ARGV[5])
        if tonumber(ARGV[5]) <= tonumber(ARGV[6]) then
            redis.call('RPUSH', prefix .. 'ready:' .. queue, id)
        else
            redis.call('ZADD', prefix .. 'delayed:' .. queue, ARGV[5], id)
        end
        return id
        LUA;

    /**
     * In each queue in turn: makes ready the jobs whose time has come and
     * the ones a worker took and never reported on, then takes the first
     * that is ready.
     * ARGV: prefix, the time now, the seconds a worker may hold a job, then the queues.
     */
    private const RESERVE = <<<'LUA'
        local prefix, now, retryAfter = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
        for index = 4, #ARGV do
            local queue = ARGV[index]
            local ready = prefix .. 'ready:' .. queue
            local delayed = prefix .. 'delayed:' .. queue
            local reserved = prefix .. 'reserved:' .. queue
            -- What has waited longest goes first: jobs whose time has come, and before them the
            -- ones a worker took and never reported on. Each is put at the head, the newest first,
            -- so that the oldest ends up in front. One whose job is gone is dropped.
            local due = redis.call('ZREVRANGEBYSCORE', delayed, now, '-inf', 'WITHSCORES', 'LIMIT', 0, 100)
            for index = 1, #due, 2 do
                redis.call('ZREM', delayed, due[index])
                if redis.call('EXISTS', prefix .. 'job:' .. due[index]) == 1 then
                    -- Since when it may run, for whoever asks how long the oldest has waited.
                    redis.call('HSET', prefix .. 'job:' .. due[index], 'at', due[index + 1])
                    redis.call('LPUSH', ready, due[index])
                end
            end
            for _, id in ipairs(redis.call('ZREVRANGEBYSCORE', reserved, now - retryAfter, '-inf', 'LIMIT', 0, 100)) do
                redis.call('ZREM', reserved, id)
                if redis.call('EXISTS', prefix .. 'job:' .. id) == 1 then
                    redis.call('LPUSH', ready, id)
                end
            end
            local id = redis.call('LPOP', ready)
            while id do
                local job = prefix .. 'job:' .. id
                -- A job taken to have died may have been finished after all, by the worker that had it.
                if redis.call('EXISTS', job) == 1 then
                    local attempts = redis.call('HINCRBY', job, 'attempts', 1)
                    redis.call('ZADD', reserved, now, id)
                    return {id, queue, redis.call('HGET', job, 'job'), redis.call('HGET', job, 'data'), attempts}
                end
                id = redis.call('LPOP', ready)
            end
        end
        return false
        LUA;

    /**
     * Puts a failed job back to wait as if new, when it is still a failed one.
     * ARGV: prefix, id, the time now.
     */
    private const RETRY = <<<'LUA'
        local prefix, id = ARGV[1], ARGV[2]
        if redis.call('ZREM', prefix .. 'failed', id) == 0 then
            return 0
        end
        local job = prefix .. 'job:' .. id
        -- Waiting from now: the time it spent failed is not time it waited for a worker.
        redis.call('HSET', job, 'attempts', 0, 'at', ARGV[3])
        redis.call('HDEL', job, 'error', 'failed_at')
        redis.call('RPUSH', prefix .. 'ready:' .. redis.call('HGET', job, 'queue'), id)
        return 1
        LUA;

    /**
     * The job is done, whoever did it, and is taken from wherever it is:
     * a worker that was slow to finish may find it given to another, or
     * put back to wait, and done is done.
     * ARGV: prefix, queue, id.
     */
    private const COMPLETE = <<<'LUA'
        local prefix, queue, id = ARGV[1], ARGV[2], ARGV[3]
        redis.call('ZREM', prefix .. 'reserved:' .. queue, id)
        redis.call('ZREM', prefix .. 'delayed:' .. queue, id)
        redis.call('LREM', prefix .. 'ready:' .. queue, 0, id)
        return redis.call('DEL', prefix .. 'job:' .. id)
        LUA;

    /**
     * Moves a job from reserved to delayed, to be tried again, when it is
     * still this worker's: taken on the try the worker says. One that was
     * since taken to have died and given to another is that other's now,
     * and is left as it is.
     * ARGV: prefix, queue, id, the time it may run from, the try it was taken on.
     */
    private const RELEASE = <<<'LUA'
        if redis.call('HGET', ARGV[1] .. 'job:' .. ARGV[3], 'attempts') ~= ARGV[5] then
            return 0
        end
        if redis.call('ZREM', ARGV[1] .. 'reserved:' .. ARGV[2], ARGV[3]) == 0 then
            return 0
        end
        return redis.call('ZADD', ARGV[1] .. 'delayed:' .. ARGV[2], ARGV[4], ARGV[3])
        LUA;

    /**
     * Moves a job from wherever it waits to the failed set, with the reason.
     * Only on the try the worker says it was taken on: a job since given
     * to another worker is not this one's to give up on.
     * ARGV: prefix, queue, id, the error, the time now, the try it was taken on.
     */
    private const FAIL = <<<'LUA'
        local prefix, queue, id = ARGV[1], ARGV[2], ARGV[3]
        if redis.call('HGET', prefix .. 'job:' .. id, 'attempts') ~= ARGV[6] then
            return 0
        end
        redis.call('ZREM', prefix .. 'reserved:' .. queue, id)
        redis.call('ZREM', prefix .. 'delayed:' .. queue, id)
        redis.call('LREM', prefix .. 'ready:' .. queue, 0, id)
        redis.call('HSET', prefix .. 'job:' .. id, 'error', ARGV[4], 'failed_at', ARGV[5])
        return redis.call('ZADD', prefix .. 'failed', ARGV[5], id)
        LUA;

    /**
     * Deletes a failed job, when it is still a failed one.
     * ARGV: prefix, id.
     */
    private const FORGET = <<<'LUA'
        if redis.call('ZREM', ARGV[1] .. 'failed', ARGV[2]) == 0 then
            return 0
        end
        redis.call('DEL', ARGV[1] .. 'job:' .. ARGV[2])
        return 1
        LUA;

    /**
     * @param string $prefix Put before every key, to keep two applications apart on one server.
     */
    public function __construct(private Client $redis, private string $prefix = 'crab:queue:')
    {
    }

    public function push(string $queue, string $job, array $data, int $availableAt, int $now): int|string
    {
        return (string) $this->redis->command('EVAL', self::PUSH, 0, $this->prefix, $queue, $job, json_encode($data, JSON_THROW_ON_ERROR), $availableAt, $now);
    }

    public function reserve(array $queues, int $now, int $retryAfter): ?Message
    {
        $taken = $this->redis->command('EVAL', self::RESERVE, 0, $this->prefix, $now, $retryAfter, ...$queues);
        if (!is_array($taken)) {
            return null;
        }
        [$id, $queue, $job, $data, $attempts] = $taken;
        return new Message((string) $id, $queue, $job, json_decode($data, true, flags: JSON_THROW_ON_ERROR), (int) $attempts);
    }

    public function complete(Message $message): void
    {
        $this->redis->command('EVAL', self::COMPLETE, 0, $this->prefix, $message->queue, $message->id);
    }

    public function release(Message $message, int $availableAt): void
    {
        $this->redis->command('EVAL', self::RELEASE, 0, $this->prefix, $message->queue, $message->id, $availableAt, $message->attempts);
    }

    public function fail(Message $message, string $error, int $now): void
    {
        $this->redis->command('EVAL', self::FAIL, 0, $this->prefix, $message->queue, $message->id, $error, $now, $message->attempts);
    }

    public function failed(?int $latest = null): array
    {
        $failed = [];
        foreach ($this->failedIds($latest) as $id) {
            $fields = $this->redis->command('HMGET', $this->prefix . 'job:' . $id, 'queue', 'job', 'data', 'attempts', 'error', 'failed_at');
            if (!is_array($fields) || $fields[1] === null) {
                continue;
            }
            [$queue, $job, $data, $attempts, $error, $failedAt] = $fields;
            $failed[] = new Message((string) $id, $queue, $job, json_decode($data, true, flags: JSON_THROW_ON_ERROR), (int) $attempts, $error, $failedAt === null ? null : (int) $failedAt);
        }
        return $failed;
    }

    public function retry(int|string|null $id, int $now): int
    {
        $count = 0;
        foreach ($id === null ? $this->failedIds() : [$id] as $one) {
            $count += (int) $this->redis->command('EVAL', self::RETRY, 0, $this->prefix, $one, $now);
        }
        return $count;
    }

    public function forget(int|string|null $id): int
    {
        $count = 0;
        foreach ($id === null ? $this->failedIds() : [$id] as $one) {
            $count += (int) $this->redis->command('EVAL', self::FORGET, 0, $this->prefix, $one);
        }
        return $count;
    }

    public function size(string $queue): int
    {
        return (int) $this->redis->command('LLEN', $this->prefix . 'ready:' . $queue)
            + (int) $this->redis->command('ZCARD', $this->prefix . 'delayed:' . $queue)
            + (int) $this->redis->command('ZCARD', $this->prefix . 'reserved:' . $queue);
    }

    public function counts(string $queue, int $now): array
    {
        // A delayed job whose time has come is moved to the ready list only when a worker next
        // asks, so it is counted as pending here by its time, as a worker would find it.
        $due = (int) $this->redis->command('ZCOUNT', $this->prefix . 'delayed:' . $queue, '-inf', $now);
        // The longest wait is at the head of the ready list, or is the delayed job whose time came first.
        $since = [];
        $head = $this->redis->command('LINDEX', $this->prefix . 'ready:' . $queue, 0);
        if (is_string($head)) {
            $at = $this->redis->command('HGET', $this->prefix . 'job:' . $head, 'at');
            $since[] = $at === null ? $now : (int) $at;
        }
        $first = $due > 0 ? (array) $this->redis->command('ZRANGEBYSCORE', $this->prefix . 'delayed:' . $queue, '-inf', $now, 'WITHSCORES', 'LIMIT', 0, 1) : [];
        if (isset($first[1])) {
            $since[] = (int) $first[1];
        }
        return [
            'oldest' => $since === [] ? null : min($since),
            'pending' => (int) $this->redis->command('LLEN', $this->prefix . 'ready:' . $queue) + $due,
            // Never below none: a worker may take a job between the two looks.
            'delayed' => max(0, (int) $this->redis->command('ZCARD', $this->prefix . 'delayed:' . $queue) - $due),
            'processing' => (int) $this->redis->command('ZCARD', $this->prefix . 'reserved:' . $queue),
        ];
    }

    /**
     * @return list<string> The ids of the jobs given up on, the one that failed first first; the latest so many when a number is given.
     */
    private function failedIds(?int $latest = null): array
    {
        if ($latest !== null && $latest < 1) {
            return [];
        }
        return array_map(strval(...), (array) $this->redis->command('ZRANGE', $this->prefix . 'failed', $latest === null ? 0 : -$latest, -1));
    }
}
