<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Contracts\Cache\Store;

/**
 * Keeps cache entries in Redis, for servers that share a cache and for
 * one read too often for the database.
 *
 * An entry is stored with the time it expires in front of it, or "n"
 * for one that never does, and that
 * time decides whether it is still there, by the clock of whoever asks.
 * Redis is told to drop it a minute later, so that a server whose clock
 * is a little behind still finds it and nothing is kept for ever.
 */
final readonly class CacheStore implements Store
{
    /**
     * What stands in front of an entry that never expires, where the time would be.
     */
    private const NEVER = 'n';

    /**
     * Sets an entry unless one is there that has not expired.
     * KEYS: the entry. ARGV: the stored value, when Redis drops it (0 for never), the time now.
     */
    private const ADD = <<<'LUA'
        local current = redis.call('GET', KEYS[1])
        if current then
            local expires = tonumber(string.match(current, '^(-?%d+):'))
            if expires == nil or expires > tonumber(ARGV[3]) then
                return 0
            end
        end
        if ARGV[2] == '0' then
            redis.call('SET', KEYS[1], ARGV[1])
        else
            redis.call('SET', KEYS[1], ARGV[1], 'EXAT', ARGV[2])
        end
        return 1
        LUA;

    /**
     * Adds to the whole number at an entry, which is kept as "i:5;". One
     * that is absent, expired or not such a number counts from zero.
     * KEYS: the entry. ARGV: how much to add, when a new count expires
     * ("n" for never), when Redis drops a new count (0 for never), the time now.
     */
    private const INCREMENT = <<<'LUA'
        local current = redis.call('GET', KEYS[1])
        local count = 0
        local expires = nil
        if current then
            local at, number = string.match(current, '^([^:]+):i:(-?%d+);$')
            if number and (at == 'n' or tonumber(at) > tonumber(ARGV[4])) then
                count = tonumber(number)
                expires = at
            end
        end
        count = count + tonumber(ARGV[1])
        if expires then
            redis.call('SET', KEYS[1], expires .. ':i:' .. string.format('%d', count) .. ';', 'KEEPTTL')
        elseif ARGV[3] == '0' then
            redis.call('SET', KEYS[1], ARGV[2] .. ':i:' .. string.format('%d', count) .. ';')
        else
            redis.call('SET', KEYS[1], ARGV[2] .. ':i:' .. string.format('%d', count) .. ';', 'EXAT', ARGV[3])
        end
        return count
        LUA;

    /**
     * @param string $prefix Put before every key. clear() removes the keys that start with it and no others, so it may not be empty.
     */
    public function __construct(private Client $redis, private string $prefix = 'crab:cache:')
    {
        if ($prefix === '') {
            throw new \InvalidArgumentException('The Redis cache needs a prefix for its keys, so that clearing the cache removes its own entries and nothing else.');
        }
    }

    public function get(string $key, int $now): ?string
    {
        $stored = $this->redis->command('GET', $this->prefix . $key);
        if (!is_string($stored)) {
            return null;
        }
        [$expiresAt, $value] = explode(':', $stored, 2);
        return $expiresAt === self::NEVER || (int) $expiresAt > $now ? $value : null;
    }

    public function set(string $key, string $value, ?int $expiresAt): void
    {
        if ($expiresAt === null) {
            $this->redis->command('SET', $this->prefix . $key, self::NEVER . ':' . $value);
            return;
        }
        $this->redis->command('SET', $this->prefix . $key, $expiresAt . ':' . $value, 'EXAT', self::droppedAt($expiresAt));
    }

    public function add(string $key, string $value, ?int $expiresAt, int $now): bool
    {
        return $this->redis->command(
            'EVAL',
            self::ADD,
            1,
            $this->prefix . $key,
            ($expiresAt ?? self::NEVER) . ':' . $value,
            $expiresAt === null ? 0 : self::droppedAt($expiresAt),
            $now,
        ) === 1;
    }

    public function increment(string $key, int $by, ?int $expiresAt, int $now): int
    {
        return (int) $this->redis->command(
            'EVAL',
            self::INCREMENT,
            1,
            $this->prefix . $key,
            $by,
            $expiresAt ?? self::NEVER,
            $expiresAt === null ? 0 : self::droppedAt($expiresAt),
            $now,
        );
    }

    public function delete(string $key): void
    {
        $this->redis->command('DEL', $this->prefix . $key);
    }

    public function clear(): void
    {
        $cursor = '0';
        do {
            [$cursor, $keys] = $this->redis->command('SCAN', $cursor, 'MATCH', self::escape($this->prefix) . '*', 'COUNT', 500);
            if ($keys !== []) {
                $this->redis->command('UNLINK', ...$keys);
            }
        } while ((string) $cursor !== '0');
    }

    /**
     * When Redis drops an entry: a minute after it expires, and never in
     * the past, which Redis would take as "now".
     */
    private static function droppedAt(int $expiresAt): int
    {
        return max($expiresAt, time()) + 60;
    }

    /**
     * A prefix as SCAN's pattern takes it, with nothing in it read as a wildcard.
     */
    private static function escape(string $prefix): string
    {
        return addcslashes($prefix, '*?[]\\');
    }
}
