<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Engine\Async\Cancellation;
use Crab\Engine\Async\CancelledException;
use Crab\Engine\EventLoop;
use Crab\Engine\FailureReporter;
use Crab\Engine\Suspension;

/**
 * Channel messages on a connection of their own. Redis is always read,
 * even when the application is busy: a full buffer loses its oldest
 * message rather than holding unread bytes on the server.
 *
 * Reconnection restores the current channel set, but cannot restore
 * publications missed while disconnected. Check gaps() and resynchronise
 * application state when its count changes.
 *
 * @implements \IteratorAggregate<string, string>
 */
final class Subscription implements \IteratorAggregate
{
    private ?EventLoop $loop = null;
    /**
     * @var resource|null
     */
    private mixed $stream = null;
    private ?string $reader = null;
    private ?string $writer = null;
    private ?string $idleTimer = null;
    private ?string $pingTimer = null;
    private ?string $replyTimer = null;
    private ?string $reconnectTimer = null;
    private string $received = '';
    private string $unsent = '';
    private bool $closed = false;
    private ?RedisException $failure = null;
    private bool $connected = false;
    private bool $everConnected = false;
    private bool $outage = false;
    private float $backoff = 0.1;
    private int $gapCount = 0;
    private int $dropCount = 0;
    private bool $consuming = false;
    private ?Suspension $consumer = null;
    private ?Cancellation $opening = null;
    /**
     * Prefixing map keys preserves numeric and binary channel names.
     *
     * @var array<string, string>
     */
    private array $channels = [];
    /**
     * Channel changes and reconnection share one control lane. The next
     * caller owns it before being resumed, so a third cannot overtake it.
     *
     * @var list<Suspension>|null
     */
    private ?array $changing = null;
    /**
     * @var array{suspension: Suspension, name: string, acknowledgements: array<string, string>}|null
     */
    private ?array $pending = null;
    /**
     * @var \SplQueue<array{string, string}>
     */
    private readonly \SplQueue $messages;

    /**
     * The client supplies its existing DNS, TLS and URL settings without
     * lending its ordinary socket. Construction starts no I/O and keeps
     * no event loop; add() resolves the loop in the worker that uses it.
     *
     * @internal Created by Client::subscribe().
     * @param \Closure(EventLoop, Cancellation): mixed $open Opens a dedicated stream.
     * @param list<list<string|int>> $setup AUTH and SELECT commands, before subscribing.
     * @param (\Closure(self): void)|null $onClose Releases the client's ownership.
     */
    public function __construct(
        private readonly \Closure $open,
        private readonly array $setup = [],
        private readonly int $bufferCapacity = 10_000,
        private readonly float $replyTimeout = 5.0,
        private readonly float $idleInterval = 30.0,
        private readonly float $pingTimeout = 5.0,
        private readonly ?\Closure $onClose = null,
    ) {
        if ($bufferCapacity < 1) {
            throw new \InvalidArgumentException('Set bufferCapacity to at least one message.');
        }
        foreach (['replyTimeout' => $replyTimeout, 'idleInterval' => $idleInterval, 'pingTimeout' => $pingTimeout] as $name => $seconds) {
            if ($seconds <= 0 || !is_finite($seconds)) {
                throw new \InvalidArgumentException("Set {$name} to a positive, finite number of seconds.");
            }
        }
        $this->messages = new \SplQueue();
    }

    /**
     * Adds channels and waits for Redis to confirm each one. Repeating a
     * channel is harmless. If confirmation fails, the desired set is
     * still restored on reconnection; the failed call throws.
     */
    public function add(string ...$channels): void
    {
        $this->change(function () use ($channels): void {
            $added = [];
            foreach ($channels as $channel) {
                $key = ':' . $channel;
                if (!isset($this->channels[$key])) {
                    $added[$key] = $this->channels[$key] = $channel;
                }
            }
            if (!$this->connected) {
                $this->connect();
            } elseif ($added !== []) {
                $this->request(['SUBSCRIBE', ...array_values($added)], $added);
            }
        });
    }

    /**
     * Removes channels and waits for confirmation. Unknown channels are
     * harmless; removing the last leaves an open, empty subscription.
     */
    public function remove(string ...$channels): void
    {
        $this->change(function () use ($channels): void {
            $removed = [];
            foreach ($channels as $channel) {
                $key = ':' . $channel;
                if (isset($this->channels[$key])) {
                    $removed[$key] = $this->channels[$key];
                    unset($this->channels[$key]);
                }
            }
            if (!$this->connected) {
                $this->connect();
            } elseif ($removed !== []) {
                $this->request(['UNSUBSCRIBE', ...array_values($removed)], $removed);
            }
        });
    }

    /**
     * Messages discarded at the buffer limit or because a connection gap
     * made undelivered updates stale.
     */
    public function dropped(): int
    {
        return $this->dropCount;
    }

    /**
     * Outages since the first successful subscription. Each may have
     * missed messages; Redis pub/sub has no replay or delivery guarantee.
     */
    public function gaps(): int
    {
        return $this->gapCount;
    }

    /**
     * Suspends only the calling fiber until a message arrives. One
     * iterator owns consumption, including while its body processes a
     * message, so two callers cannot split the stream by accident.
     *
     * @return \Traversable<string, string>
     */
    public function getIterator(): \Traversable
    {
        if ($this->failure !== null) {
            throw $this->failure;
        }
        if ($this->consuming) {
            throw new \LogicException('This Redis subscription already has a consumer. Finish its iterator before starting another, or create a separate subscription.');
        }
        if ($this->closed) {
            return;
        }
        $this->useLoop();
        $this->consuming = true;
        $this->reference();
        try {
            while (!$this->closed) {
                if (!$this->messages->isEmpty()) {
                    [$channel, $payload] = $this->messages->dequeue();
                } else {
                    $this->consumer = $this->loop->getSuspension();
                    $this->reference();
                    try {
                        $available = $this->consumer->suspend();
                    } finally {
                        $this->consumer = null;
                        $this->reference();
                    }
                    if ($available === null) {
                        return;
                    }
                    // The message stays in the queue until this fiber runs:
                    // a gap can discard it after wakeup but before delivery.
                    continue;
                }
                if (!$this->closed) {
                    yield $channel => $payload;
                }
            }
            if ($this->failure !== null) {
                throw $this->failure;
            }
        } finally {
            $this->consuming = false;
            $this->reference();
        }
    }

    /**
     * Ends iteration cleanly and cancels reconnecting, without I/O or
     * suspension. Buffered messages are discarded on an explicit close.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->opening?->cancel();
        if ($this->reconnectTimer !== null) {
            $this->loop->cancel($this->reconnectTimer);
            $this->reconnectTimer = null;
        }
        $error = $this->failure ?? new RedisException('The Redis subscription is closed. Create a new subscription to change channels.');
        $this->drop($error);
        $waiting = $this->changing ?? [];
        $this->changing = null;
        foreach ($waiting as $suspension) {
            $suspension->throw($error);
        }
        $consumer = $this->consumer;
        $this->consumer = null;
        if ($consumer !== null) {
            if ($this->failure !== null) {
                $consumer->throw($this->failure);
            } else {
                $consumer->resume();
            }
        }
        while (!$this->messages->isEmpty()) {
            $this->messages->dequeue();
        }
        ($this->onClose)?->__invoke($this);
    }

    private function useLoop(): void
    {
        $loop = EventLoop::current();
        if ($this->loop !== null && $this->loop !== $loop) {
            throw new \LogicException('This Redis subscription belongs to another event loop. Create it in the worker that consumes it.');
        }
        $this->loop = $loop;
    }

    private function change(\Closure $operation): void
    {
        if ($this->failure !== null) {
            throw $this->failure;
        }
        $this->useLoop();
        if ($this->closed) {
            throw new RedisException('The Redis subscription is closed. Create a new subscription to change channels.');
        }
        if ($this->changing !== null) {
            $suspension = $this->loop->getSuspension();
            $this->changing[] = $suspension;
            $suspension->suspend();
        } else {
            $this->changing = [];
        }
        try {
            if ($this->failure !== null) {
                throw $this->failure;
            }
            if ($this->closed) {
                throw new RedisException('The Redis subscription is closed. Create a new subscription to change channels.');
            }
            $operation();
        } finally {
            if ($this->changing !== null) {
                $next = array_shift($this->changing);
                if ($next === null) {
                    $this->changing = null;
                } else {
                    $next->resume();
                }
            }
        }
    }

    private function connect(): void
    {
        $this->opening = new Cancellation();
        try {
            $this->stream = ($this->open)($this->loop, $this->opening);
            $this->opening->throwIfCancelled();
            $this->reader = $this->loop->onReadable($this->stream, $this->read(...));
            $this->reference();
            foreach ($this->setup as $command) {
                $this->request($command);
            }
            if ($this->channels !== []) {
                $this->request(['SUBSCRIBE', ...array_values($this->channels)], $this->channels);
            }
            if ($this->failure !== null) {
                throw $this->failure;
            }
            if (!is_resource($this->stream) || $this->closed) {
                throw new RedisException('Redis lost the connection before the subscription was ready. Check server availability and try again.');
            }
            $this->connected = $this->everConnected = true;
            $this->outage = false;
            $this->backoff = 0.1;
            if ($this->reconnectTimer !== null) {
                $this->loop->cancel($this->reconnectTimer);
                $this->reconnectTimer = null;
            }
            $this->watchIdle();
        } catch (CancelledException $error) {
            $this->drop();
            throw new RedisException('The Redis subscription was closed while connecting. Create a new subscription to try again.', previous: $error);
        } catch (RedisException $error) {
            $this->drop($error);
            if ($this->everConnected && !$this->closed) {
                $this->reconnect();
            }
            throw $error;
        } finally {
            $this->opening = null;
        }
    }

    /**
     * Only control replies have a deadline. Published messages never do.
     * Per-channel acknowledgements may be interleaved with publications.
     *
     * @param list<string|int> $words
     * @param array<string, string> $acknowledgements
     */
    private function request(array $words, array $acknowledgements = []): void
    {
        $suspension = $this->loop->getSuspension();
        $name = $words[0];
        $this->pending = ['suspension' => $suspension, 'name' => $name, 'acknowledgements' => $acknowledgements];
        $this->reference();
        $this->replyTimer = $this->loop->delay($this->replyTimeout, function () use ($name): void {
            $this->lost("Redis did not confirm {$name}. Check server availability and the reply timeout.");
        });
        $this->unsent .= Resp::command($words);
        if (!$this->flush()) {
            // This caller has not suspended yet; only other waiters can
            // be failed through their suspensions.
            $this->pending = null;
            $why = 'The Redis subscription connection failed while writing. Check server availability.';
            $this->lost($why);
            throw new RedisException($why);
        }
        $suspension->suspend();
        if ($this->failure !== null) {
            throw $this->failure;
        }
    }

    /**
     * @param list<string|int> $words
     */
    private function write(array $words): void
    {
        $this->unsent .= Resp::command($words);
        if (!$this->flush()) {
            $this->lost('The Redis subscription connection failed while writing. Check server availability.');
        }
    }

    private function flush(): bool
    {
        while ($this->unsent !== '') {
            $sent = @fwrite($this->stream, $this->unsent);
            if ($sent === false) {
                return false;
            }
            if ($sent === 0) {
                break;
            }
            $this->unsent = substr($this->unsent, $sent);
        }
        if ($this->unsent === '' && $this->writer !== null) {
            $this->loop->cancel($this->writer);
            $this->writer = null;
        } elseif ($this->unsent !== '' && $this->writer === null) {
            $this->writer = $this->loop->onWritable($this->stream, function (): void {
                if (!$this->flush()) {
                    $this->lost('The Redis subscription connection failed while writing. Check server availability.');
                }
            });
        }
        return true;
    }

    private function read(): void
    {
        $bytes = @fread($this->stream, 65_536);
        if ($bytes === false || ($bytes === '' && feof($this->stream))) {
            $this->lost('Redis closed the subscription connection. Check server availability.');
            return;
        }
        $this->received .= $bytes;
        $offset = 0;
        try {
            while (true) {
                $reply = Resp::reply($this->received, $offset, $complete);
                if (!$complete) {
                    break;
                }
                if ($reply instanceof RedisError) {
                    // Server errors may echo command arguments, including an AUTH
                    // password. Keep those bytes out of exceptions and reports.
                    $name = $this->pending['name'] ?? 'PING';
                    $this->refused($name);
                    return;
                }
                if (is_array($reply) && ($reply[0] ?? null) === 'message' && count($reply) === 3 && is_string($reply[1]) && is_string($reply[2])) {
                    $this->deliver($reply[1], $reply[2]);
                } elseif ($reply === 'PONG' || (is_array($reply) && ($reply[0] ?? null) === 'pong')) {
                    if ($this->pingTimer !== null) {
                        $this->loop->cancel($this->pingTimer);
                        $this->pingTimer = null;
                    }
                } elseif ($this->pending !== null) {
                    if ($this->pending['acknowledgements'] !== []) {
                        if (!is_array($reply) || ($reply[0] ?? null) !== strtolower($this->pending['name']) || !is_string($reply[1] ?? null)) {
                            throw new RedisException('Redis sent an unexpected subscription reply. Check the server protocol; this client uses RESP2.');
                        }
                        unset($this->pending['acknowledgements'][':' . $reply[1]]);
                    } elseif ($reply !== 'OK') {
                        throw new RedisException('Redis did not confirm subscription setup. Check credentials and database settings.');
                    }
                    if ($this->pending['acknowledgements'] === []) {
                        $pending = $this->pending;
                        $this->pending = null;
                        $this->loop->cancel($this->replyTimer);
                        $this->replyTimer = null;
                        $this->reference();
                        $pending['suspension']->resume();
                    }
                } else {
                    throw new RedisException('Redis sent an unexpected subscription reply. Check the server protocol; this client uses RESP2.');
                }
            }
        } catch (RedisException $error) {
            $this->lost($error->getMessage());
            return;
        }
        $this->received = substr($this->received, $offset);
        if ($this->connected && $bytes !== '') {
            $this->watchIdle();
        }
    }

    private function deliver(string $channel, string $payload): void
    {
        if ($this->messages->count() === $this->bufferCapacity) {
            $this->messages->dequeue();
            ++$this->dropCount;
        }
        $this->messages->enqueue([$channel, $payload]);
        if ($this->consumer !== null) {
            $consumer = $this->consumer;
            $this->consumer = null;
            $this->reference();
            $consumer->resume(true);
        }
    }

    private function watchIdle(): void
    {
        if ($this->idleTimer !== null) {
            $this->loop->cancel($this->idleTimer);
        }
        $this->idleTimer = $this->loop->delay($this->idleInterval, function (): void {
            $this->idleTimer = null;
            if ($this->pending !== null || $this->pingTimer !== null) {
                $this->watchIdle();
                return;
            }
            $this->pingTimer = $this->loop->delay($this->pingTimeout, function (): void {
                $this->lost('Redis did not answer the subscription PING. Check server availability.');
            });
            $this->loop->unreference($this->pingTimer);
            $this->write(['PING']);
        });
        $this->loop->unreference($this->idleTimer);
    }

    private function lost(string $why): void
    {
        if ($this->closed) {
            return;
        }
        $error = new RedisException($why . ($this->everConnected ? ' Publications during this gap may be lost; resynchronise application state.' : ''));
        $this->dropCount += $this->messages->count();
        while (!$this->messages->isEmpty()) {
            $this->messages->dequeue();
        }
        $this->drop($error);
        if (!$this->everConnected) {
            return;
        }
        if (!$this->outage) {
            $this->outage = true;
            ++$this->gapCount;
            FailureReporter::report('Redis subscription lost messages during a connection gap; resynchronise application state. Reconnecting to its current channels', $error);
        }
        $this->reconnect();
    }

    /**
     * Immutable setup cannot recover from a server refusal. Preserve one
     * password-free terminal error for the consumer and channel changes.
     */
    private function refused(string $command): void
    {
        $this->failure = new RedisException("Redis refused subscription {$command}: credentials, database or channel permissions were refused. Check credentials and database, then create a new client with updated settings and subscribe again.");
        $this->close();
        if ($this->everConnected) {
            FailureReporter::report('Redis subscription cannot reconnect with its current settings', $this->failure);
        }
    }

    private function reconnect(): void
    {
        if ($this->closed || $this->reconnectTimer !== null) {
            return;
        }
        $this->reconnectTimer = $this->loop->delay($this->backoff, function (): void {
            $this->reconnectTimer = null;
            if ($this->closed) {
                return;
            }
            try {
                $this->change(function (): void {
                    if (!$this->connected) {
                        $this->connect();
                    }
                });
            } catch (RedisException) {
                // lost() reports once for the entire outage; retries do not
                // imply another gap or flood the worker's failure logger.
                $this->reconnect();
            }
        });
        $this->backoff = min(5.0, $this->backoff * 2);
        $this->reference();
    }

    private function reference(): void
    {
        foreach ([$this->reader, $this->reconnectTimer] as $watcher) {
            if ($watcher !== null) {
                if ($this->consumer !== null || $this->pending !== null) {
                    $this->loop->reference($watcher);
                } else {
                    $this->loop->unreference($watcher);
                }
            }
        }
    }

    private function drop(?RedisException $error = null): void
    {
        foreach ([$this->reader, $this->writer, $this->idleTimer, $this->pingTimer, $this->replyTimer] as $watcher) {
            if ($watcher !== null) {
                $this->loop->cancel($watcher);
            }
        }
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
        $this->stream = $this->reader = $this->writer = $this->idleTimer = $this->pingTimer = $this->replyTimer = null;
        $this->received = $this->unsent = '';
        $this->connected = false;
        $pending = $this->pending;
        $this->pending = null;
        if ($pending !== null) {
            $pending['suspension']->throw($error ?? new RedisException('The Redis subscription connection was closed. Create a new subscription to try again.'));
        }
    }
}
