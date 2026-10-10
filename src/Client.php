<?php

declare(strict_types=1);

namespace Crab\Redis;

/**
 * The one thing this package asks of a Redis client: send a command and
 * return its reply. The stores are written against it and nothing else,
 * so the client behind it can be replaced without touching them.
 *
 * A reply is as Redis gives it: a string, an integer, null, or a list of
 * those.
 */
interface Client
{
    /**
     * Sends one command and waits for its reply without holding the
     * worker: other requests and jobs go on meanwhile.
     *
     *     $redis->command('SET', 'greeting', 'hello');
     *     $redis->command('EVAL', $script, 1, 'a-key', 'an-argument');
     *
     * @throws RedisException When the server cannot be reached or refuses the command.
     */
    public function command(string $name, string|int|float ...$arguments): mixed;
}
