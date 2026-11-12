<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Contracts\Session\Driver;
use Crab\Contracts\Session\Record;

/**
 * Keeps sessions in Redis under a random id that the cookie carries. Redis
 * drops a session when it expires, so there is nothing to sweep, and every
 * worker on every machine sharing the server sees the same sessions.
 */
final readonly class SessionStore implements Driver
{
    /**
     * @param string $prefix Put before every session's id; it must not be empty.
     */
    public function __construct(
        private Client $redis,
        private string $prefix = 'crab:session:',
    ) {
        if ($prefix === '') {
            throw new \InvalidArgumentException(
                'The Redis session store needs a prefix for its keys, so that they stay apart from every other key on the server.',
            );
        }
    }

    public function read(string $cookie, int $now): ?Record
    {
        if (!self::isId($cookie)) {
            return null;
        }
        $json = $this->redis->command('GET', $this->prefix . $cookie);
        return is_string($json) ? Record::fromJson($json, $now) : null;
    }

    public function write(?string $cookie, Record $record): string
    {
        $id = $cookie !== null && self::isId($cookie) ? $cookie : bin2hex(random_bytes(32));
        $this->redis->command('SET', $this->prefix . $id, $record->toJson(), 'EXAT', $record->expiresAt);
        return $id;
    }

    public function destroy(string $cookie): void
    {
        if (self::isId($cookie)) {
            $this->redis->command('DEL', $this->prefix . $cookie);
        }
    }

    public function sweep(int $now): int
    {
        return 0;
    }

    private static function isId(string $cookie): bool
    {
        return preg_match('/^[0-9a-f]{64}$/D', $cookie) === 1;
    }
}
