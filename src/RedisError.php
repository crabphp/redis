<?php

declare(strict_types=1);

namespace Crab\Redis;

/**
 * An error as Redis replied with it, before it is thrown to whoever sent
 * the command.
 *
 * @internal
 */
final readonly class RedisError
{
    public function __construct(public string $message)
    {
    }
}
