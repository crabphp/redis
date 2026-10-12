<?php

declare(strict_types=1);

namespace Crab\Redis;

/**
 * Commands and channel messages, without holding the worker. The stores
 * are written against this contract and nothing else,
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

    /**
     * Publishes the payload byte for byte and returns Redis's receiver count.
     */
    public function publish(string $channel, string $payload): int;

    /**
     * Subscribes on a dedicated connection and waits for confirmation.
     * An empty set is allowed, so channels can be added later.
     */
    public function subscribe(string ...$channels): Subscription;

    /**
     * Creates an unopened subscription. Its owner can retain and close it
     * before add() starts waiting for connection or channel confirmation.
     */
    public function subscription(): Subscription;

    /**
     * Releases ordinary connections and subscriptions without suspending,
     * including when the worker's event loop has already stopped.
     */
    public function close(): void;
}
