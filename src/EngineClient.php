<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Engine\Async\Cancellation;
use Crab\Engine\Async\CancelledException;
use Crab\Engine\EventLoop;
use Crab\Engine\Network\DnsResolver;
use Crab\Engine\Suspension;

/**
 * A Redis client written on the engine: its event loop, its name
 * resolver, and nothing from outside.
 *
 * Inside a worker it keeps one connection, made on the first command.
 * Every command of the worker goes down it one after another without
 * waiting for the reply before, and each reply wakes whoever sent the
 * command it answers; Redis replies in the order it was asked. Whoever
 * waits is suspended, and the worker goes on with other work.
 *
 * A connection that fails is dropped, the commands waiting on it fail,
 * and the next command connects again.
 *
 * Outside a worker, in a command such as `cache:clear` or in the process
 * that is about to start the workers, a command connects, waits for its
 * reply and disconnects, and leaves the event loop at rest: no
 * connection is carried into a worker, and the engine finds a loop it
 * can replace with its own.
 */
final class EngineClient implements Client
{
    private readonly bool $encrypted;
    private readonly string $host;
    private readonly int $port;
    private readonly ?string $username;
    private readonly ?string $password;
    private readonly int $database;
    private DnsResolver $resolver;

    /**
     * The loop the connection was made on. A worker has a loop of its
     * own, and a connection made on another is of no use to it.
     */
    private ?EventLoop $loop = null;
    /**
     * @var resource|null
     */
    private mixed $stream = null;
    /**
     * Bytes received and not yet read as replies, and bytes not yet sent.
     */
    private string $received = '';
    private string $unsent = '';
    private ?string $reader = null;
    private ?string $writer = null;
    /**
     * Byte positions on this connection let each reply deadline start
     * when its whole command has been written, even under backpressure.
     */
    private int $queued = 0;
    private int $written = 0;
    private ?string $replyTimer = null;
    private ?float $replyDeadline = null;
    /**
     * Who waits for a reply, in wire order. A null timeout means an
     * indefinite blocking command; a null deadline means it is not yet
     * fully written, or has no deadline.
     *
     * @var list<array{suspension: Suspension, name: string, timeout: ?float, deadline: ?float, end: int}>
     */
    private array $waiting = [];
    /**
     * Who waits for the connection another is making; null when none is being made.
     *
     * @var list<Suspension>|null
     */
    private ?array $connecting = null;
    /**
     * The identity and cancellation of the ordinary connection attempt.
     * A cancelled opener must never clean up a newer caller's connection.
     */
    private ?Cancellation $opening = null;
    /**
     * Dedicated connections are kept until closed, so worker shutdown can
     * release even a subscription whose caller has stopped iterating.
     *
     * @var array<int, Subscription>
     */
    private array $subscriptions = [];

    /**
     * @param string $url Where the server is: "redis://127.0.0.1:6379", with "?password=..." and "&database=1" when needed, or "rediss://" for an encrypted connection.
     * @param float $timeout Seconds to wait for a connection before the command fails.
     * @param array<string, mixed> $tls PHP's "ssl" context options for an encrypted connection, such as "cafile".
     * @param float $replyTimeout Seconds to wait for a reply after writing a command, in addition to any explicit blocking timeout.
     * @param int $subscriptionBuffer Maximum undelivered messages per subscription; the oldest are dropped at the limit.
     * @param float $subscriptionIdleInterval Idle seconds before checking a subscription with PING.
     * @param float $subscriptionPingTimeout Seconds to wait for that PING, independent of ordinary reply deadlines.
     */
    public function __construct(
        string $url,
        private readonly float $timeout = 2.0,
        private readonly array $tls = [],
        private readonly float $replyTimeout = 5.0,
        private readonly int $subscriptionBuffer = 10_000,
        private readonly float $subscriptionIdleInterval = 30.0,
        private readonly float $subscriptionPingTimeout = 5.0,
    ) {
        if ($replyTimeout <= 0 || !is_finite($replyTimeout)) {
            throw new \InvalidArgumentException(
                'Redis reply timeout must be greater than zero seconds. Set replyTimeout to a positive, finite number.',
            );
        }
        if ($subscriptionBuffer < 1) {
            throw new \InvalidArgumentException('Set subscriptionBuffer to at least one message.');
        }
        foreach ([
            'subscriptionIdleInterval' => $subscriptionIdleInterval,
            'subscriptionPingTimeout' => $subscriptionPingTimeout,
        ] as $name => $seconds) {
            if ($seconds <= 0 || !is_finite($seconds)) {
                throw new \InvalidArgumentException("Set {$name} to a positive, finite number of seconds.");
            }
        }
        $parts = parse_url($url);
        $scheme = is_array($parts) ? $parts['scheme'] ?? '' : '';
        if (!is_array($parts) || !in_array($scheme, ['redis', 'rediss'], true) || !isset($parts['host'])) {
            throw new \InvalidArgumentException(
                'The Redis address is invalid. Write it as redis://127.0.0.1:6379, or rediss:// for an encrypted connection.',
            );
        }
        parse_str($parts['query'] ?? '', $query);
        $this->encrypted = $scheme === 'rediss';
        $this->host = $parts['host'];
        $this->port = $parts['port'] ?? 6379;
        $this->username = isset($parts['user']) && $parts['user'] !== '' ? rawurldecode($parts['user']) : null;
        // What stands before the host is still encoded; what parse_str() read from the query is not.
        $password = isset($parts['pass']) ? rawurldecode($parts['pass']) : $query['password'] ?? null;
        $this->password = is_string($password) && $password !== '' ? $password : null;
        $this->database = (int) ($query['database'] ?? ltrim($parts['path'] ?? '', '/'));
        // One round of the name servers, each given half the connection's time, and no falling
        // back to the system's resolver, which would hold the worker while it asked.
        $this->resolver = $this->resolver();
    }

    private function resolver(): DnsResolver
    {
        return new DnsResolver(patience: max(0.1, $this->timeout / 2), rounds: 1, fallback: false);
    }

    public function publish(string $channel, string $payload): int
    {
        return $this->command('PUBLISH', $channel, $payload);
    }

    public function subscribe(string ...$channels): Subscription
    {
        $subscription = $this->subscription();
        try {
            $subscription->add(...$channels);
        } catch (\Throwable $error) {
            $subscription->close();
            throw $error;
        }
        return $subscription;
    }

    public function subscription(): Subscription
    {
        $setup = [];
        if ($this->password !== null) {
            $setup[] = $this->username === null
                ? ['AUTH', $this->password]
                : ['AUTH', $this->username, $this->password];
        }
        if ($this->database !== 0) {
            $setup[] = ['SELECT', $this->database];
        }
        $subscription = new Subscription(
            function (EventLoop $loop, Cancellation $cancellation): mixed {
                try {
                    return $this->open($loop, $cancellation);
                } catch (CancelledException $error) {
                    throw $error;
                } catch (\Throwable $error) {
                    throw new RedisException(
                        "Redis at {$this->host}:{$this->port} could not be reached. Check the address, TLS settings and server availability.",
                        previous: $error,
                    );
                }
            },
            $setup,
            $this->subscriptionBuffer,
            $this->replyTimeout,
            $this->subscriptionIdleInterval,
            $this->subscriptionPingTimeout,
            function (Subscription $subscription): void {
                unset($this->subscriptions[spl_object_id($subscription)]);
            },
        );
        $this->subscriptions[spl_object_id($subscription)] = $subscription;
        return $subscription;
    }

    public function close(): void
    {
        foreach ($this->subscriptions as $subscription) {
            $subscription->close();
        }
        $opening = $this->opening;
        $others = $this->connecting ?? [];
        $this->opening = $this->connecting = null;
        $opening?->cancel();
        $this->fail('the client was closed; send a new command to connect again');
        foreach ($others as $other) {
            $other->throw(
                new RedisException('The Redis connection attempt was closed. Send a new command to connect again.'),
            );
        }
    }

    public function command(string $name, string|int|float ...$arguments): mixed
    {
        $loop = EventLoop::current();
        $inWorker = \Fiber::getCurrent() !== null;
        try {
            if (!$inWorker || $this->loop !== $loop) {
                // A connection is kept only from one command in a worker to the next in the same
                // worker. One made on another loop, or in a run that has ended, is of no use,
                // and neither is one still being made there: whoever waited for it is gone too.
                $this->drop();
                $this->connecting = null;
                // A lookup still under way belonged to the loop that is gone, and would never answer here.
                $this->resolver = $this->resolver();
            }
            $this->connect($loop);
            return $this->send($loop, [$name, ...$arguments]);
        } finally {
            if (!$inWorker) {
                // Waiting from outside a fiber leaves the loop suspended half way, which
                // counts as running, and a running loop cannot be replaced by a worker's.
                $this->drop();
                $loop->run();
            }
        }
    }

    /**
     * Sends a command down the connection and waits for its reply.
     *
     * @param list<string|int|float> $words
     */
    private function send(EventLoop $loop, array $words): mixed
    {
        if ($this->stream === null) {
            throw new RedisException("Redis command {$words[0]} failed: the connection was lost.");
        }
        $suspension = $loop->getSuspension();
        $bytes = Resp::command($words);
        $this->queued += strlen($bytes);
        $this->waiting[] = [
            'suspension' => $suspension,
            'name' => strtoupper((string) $words[0]),
            'timeout' => $this->replyWait($words),
            'deadline' => null,
            'end' => $this->queued,
        ];
        $this->unsent .= $bytes;
        // A reply is awaited, so the connection keeps the loop going until it comes.
        $loop->reference($this->reader);
        if (!$this->flush($loop)) {
            // This caller is not suspended yet, so it is told by an exception thrown here,
            // and the others, who are, by fail().
            array_pop($this->waiting);
            $this->fail('the connection was lost while sending');
            throw new RedisException("Redis command {$words[0]} failed: the connection was lost while sending.");
        }
        return $suspension->suspend();
    }

    /**
     * How long this command may wait for its reply; null for as long as
     * it takes. Redis's blocking list and sorted-set commands end in
     * their seconds, and XREAD and XREADGROUP give BLOCK in milliseconds
     * before STREAMS. A command that blocks adds that time to the
     * ordinary timeout, and one that blocks for zero has none.
     *
     * Only where the time stands and how it is written is checked, the
     * way Redis reads it. The rest of the command is Redis's to refuse,
     * which it does at once, so a mistake there never waits.
     *
     * @param list<string|int|float> $words
     */
    private function replyWait(array $words): ?float
    {
        $name = strtoupper((string) $words[0]);
        $count = count($words);
        $block = null;
        if (in_array($name, ['BLPOP', 'BRPOP', 'BZPOPMIN', 'BZPOPMAX'], true)) {
            $block = $count >= 3 ? self::seconds($words[$count - 1]) : null;
        } elseif ($name === 'BRPOPLPUSH' || $name === 'BLMOVE') {
            $block = $count === ($name === 'BLMOVE' ? 6 : 4) ? self::seconds($words[$count - 1]) : null;
        } elseif ($name === 'XREAD' || $name === 'XREADGROUP') {
            // Options come in any order before STREAMS, and the last BLOCK counts. What follows
            // an option is stepped over, so a group, a consumer or a count is never read as one.
            $follows = ['GROUP' => 2, 'COUNT' => 1, 'CLAIM' => 1, 'BLOCK' => 1];
            for ($index = 1; $index < $count; ++$index) {
                $option = strtoupper((string) $words[$index]);
                if ($option === 'STREAMS') {
                    break;
                }
                if ($option === 'BLOCK') {
                    $word = (string) ($words[$index + 1] ?? '');
                    $block = preg_match('/^\d+$/', $word) === 1 ? (float) $word / 1000 : null;
                }
                $index += $follows[$option] ?? 0;
            }
        }
        if ($block === null) {
            return $this->replyTimeout;
        }
        return $block === 0.0 ? null : $block + $this->replyTimeout;
    }

    /**
     * Seconds as Redis accepts them: a number that is not negative, with
     * no space around it, which PHP would let through and Redis refuses.
     */
    private static function seconds(string|int|float $word): ?float
    {
        $word = (string) $word;
        if ($word !== trim($word) || !is_numeric($word) || (float) $word < 0 || !is_finite((float) $word)) {
            return null;
        }
        return abs((float) $word);
    }

    /**
     * One timer watches the earliest reply deadline, normally the oldest
     * command. Keeping a timer per pipelined command would add watchers
     * without changing how a timeout drops the whole connection. A later
     * command can expire first when an earlier one explicitly blocks.
     * Deadlines stay tied to writing, never to the previous reply.
     * The timer reads who still waits when it fires: equal deadlines may
     * remain after the command that first owned the timer was answered.
     */
    private function watchReply(EventLoop $loop): void
    {
        $next = null;
        foreach ($this->waiting as $waiting) {
            if (!($waiting['deadline'] !== null && ($next === null || $waiting['deadline'] < $next['deadline']))) {
                continue;
            }

            $next = $waiting;
        }
        $deadline = $next['deadline'] ?? null;
        if ($deadline === $this->replyDeadline) {
            return;
        }
        if ($this->replyTimer !== null) {
            $loop->cancel($this->replyTimer);
            $this->replyTimer = null;
        }
        $this->replyDeadline = $deadline;
        if ($next === null) {
            return;
        }
        $left = max(0.0, $deadline - (hrtime(true) / 1_000_000_000));
        $this->replyTimer = $loop->delay($left, function (string $timer): void {
            if ($this->replyTimer !== $timer) {
                return;
            }
            foreach ($this->waiting as $waiting) {
                if ($waiting['deadline'] !== $this->replyDeadline) {
                    continue;
                }

                $this->fail(
                    new RedisException("Redis did not answer {$waiting['name']} within {$waiting['timeout']} seconds"),
                );
                return;
            }
        });
    }

    /**
     * Makes the connection when there is none, or waits for the one that
     * another command of the worker is making.
     */
    private function connect(EventLoop $loop): void
    {
        if ($this->connecting !== null) {
            $suspension = $loop->getSuspension();
            $this->connecting[] = $suspension;
            $suspension->suspend();
            return;
        }
        if ($this->stream !== null) {
            return;
        }
        $this->connecting = [];
        $this->opening = $opening = new Cancellation();
        // Said before the connection exists: the worker's other commands, arriving while it
        // is made, must see that it is being made on their own loop and wait for it.
        $this->loop = $loop;
        try {
            $stream = $this->open($loop, $opening);
            $opening->throwIfCancelled();
            $this->stream = $stream;
            $this->reader = $loop->onReadable($this->stream, fn() => $this->read($loop));
            // Idle, the connection does not keep a process alive that has nothing else to do.
            $loop->unreference($this->reader);
            if ($this->password !== null) {
                $this->send(
                    $loop,
                    $this->username === null ? ['AUTH', $this->password] : ['AUTH', $this->username, $this->password],
                );
            }
            if ($this->database !== 0) {
                $this->send($loop, ['SELECT', $this->database]);
            }
            $opening->throwIfCancelled();
        } catch (\Throwable $error) {
            if ($opening->isCancelled()) {
                $error = new RedisException(
                    'The Redis connection attempt was closed. Send a new command to connect again.',
                );
            } elseif (!$error instanceof RedisException) {
                $error = new RedisException(
                    "Redis at {$this->host}:{$this->port} could not be reached: {$error->getMessage()}",
                    previous: $error,
                );
            }
            if ($this->opening === $opening) {
                $others = $this->connecting ?? [];
                $this->opening = $this->connecting = null;
                $this->drop();
                foreach ($others as $other) {
                    $other->throw($error);
                }
            }
            throw $error;
        }
        $others = $this->connecting ?? [];
        $this->opening = $this->connecting = null;
        foreach ($others as $other) {
            $other->resume();
        }
    }

    /**
     * A socket connected to the server, encrypted when the address asks
     * for it. Each address the host's name has is tried in turn.
     *
     * @return resource
     */
    private function open(EventLoop $loop, ?Cancellation $cancellation = null): mixed
    {
        $deadline = (hrtime(true) / 1_000_000_000) + $this->timeout;
        $failure = null;
        foreach ($this->resolver->resolve($loop, $this->host, $cancellation) as $address) {
            $cancellation?->throwIfCancelled();
            $target = str_contains($address, ':')
                ? "tcp://[{$address}]:{$this->port}"
                : "tcp://{$address}:{$this->port}";
            $options = ['socket' => ['tcp_nodelay' => true]];
            if ($this->encrypted) {
                $options['ssl'] = [
                    'peer_name' => $this->host,
                    'SNI_enabled' => true,
                    'verify_peer' => true,
                    'verify_peer_name' => true,
                    ...$this->tls,
                ];
            }
            $stream = @stream_socket_client(
                $target,
                $code,
                $reason,
                0,
                STREAM_CLIENT_CONNECT | STREAM_CLIENT_ASYNC_CONNECT,
                stream_context_create($options),
            );
            if ($stream === false) {
                $failure ??= new \RuntimeException((string) $reason);
                continue;
            }
            stream_set_blocking($stream, false);
            // Cancellation owns the local socket until it is handed to the
            // client. Provider stop cannot wait for this fiber's finally block.
            $release = $cancellation?->subscribe(static function () use ($stream): void {
                if (is_resource($stream)) {
                    fclose($stream);
                }
            });
            try {
                $this->await($loop, $stream, true, $deadline, $cancellation);
                // Being writable also follows a refusal; only a connected socket knows its peer.
                if (@stream_socket_get_name($stream, true) === false) {
                    throw new \RuntimeException('the connection was refused');
                }
                if ($this->encrypted) {
                    $this->encrypt($loop, $stream, $deadline, $cancellation);
                }
                $cancellation?->throwIfCancelled();
                return $stream;
            } catch (\Throwable $error) {
                if (is_resource($stream)) {
                    fclose($stream);
                }
                if ($error instanceof CancelledException) {
                    throw $error;
                }
                $failure ??= $error;
            } finally {
                $release?->__invoke();
            }
        }
        throw $failure ?? new \RuntimeException('its name has no address');
    }

    /**
     * @param resource $stream
     */
    private function encrypt(EventLoop $loop, mixed $stream, float $deadline, ?Cancellation $cancellation = null): void
    {
        while (true) {
            error_clear_last();
            $done = @stream_socket_enable_crypto($stream, true, STREAM_CRYPTO_METHOD_TLS_CLIENT);
            if ($done === true) {
                return;
            }
            if ($done === false) {
                $why = error_get_last()['message'] ?? 'the handshake failed';
                // PHP's message starts with the name of its own function.
                throw new \RuntimeException(
                    'the encrypted connection could not be made: '
                        . trim((string) preg_replace('/^stream_socket_enable_crypto\(\):\s*/', '', $why)),
                );
            }
            // Neither done nor failed: the handshake waits for the server's next message.
            $this->await($loop, $stream, false, $deadline, $cancellation);
        }
    }

    /**
     * Suspends until the socket can be written to, or read from, or the
     * deadline has passed.
     *
     * @param resource $stream
     */
    private function await(
        EventLoop $loop,
        mixed $stream,
        bool $writable,
        float $deadline,
        ?Cancellation $cancellation = null,
    ): void {
        $cancellation?->throwIfCancelled();
        $left = $deadline - (hrtime(true) / 1_000_000_000);
        if ($left <= 0) {
            throw new \RuntimeException("it did not answer within {$this->timeout} seconds");
        }
        $suspension = $loop->getSuspension();
        $settled = false;
        $timeout = $this->timeout;
        $finish = static function (bool $late = false) use ($suspension, $timeout, &$settled): void {
            if ($settled) {
                return;
            }
            $settled = true;
            if ($late) {
                $suspension->throw(new \RuntimeException("it did not answer within {$timeout} seconds"));
            } else {
                $suspension->resume();
            }
        };
        $watcher = $writable
            ? $loop->onWritable($stream, static fn() => $finish())
            : $loop->onReadable($stream, static fn() => $finish());
        $timer = $loop->delay($left, static fn() => $finish(true));
        $unsubscribe = $cancellation?->subscribe(static function (CancelledException $error) use (
            $loop,
            $watcher,
            $timer,
            $suspension,
            &$settled,
        ): void {
            // Remove watchers even if readiness has already queued a resume,
            // and even when the worker will never run another loop turn.
            $loop->cancel($watcher);
            $loop->cancel($timer);
            if (!$settled) {
                $settled = true;
                $suspension->throw($error);
            }
        });
        try {
            $suspension->suspend();
            $cancellation?->throwIfCancelled();
        } finally {
            $settled = true;
            $loop->cancel($watcher);
            $loop->cancel($timer);
            $unsubscribe?->__invoke();
        }
    }

    /**
     * Sends what is waiting to be sent, as much as the socket takes now,
     * and the rest when it can take more.
     *
     * @return bool False when the connection would not take it; the caller then fails the connection.
     */
    private function flush(EventLoop $loop): bool
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
            $this->written += $sent;
            $now = hrtime(true) / 1_000_000_000;
            foreach ($this->waiting as &$waiting) {
                if ($waiting['end'] > $this->written) {
                    break;
                }
                if ($waiting['deadline'] === null && $waiting['timeout'] !== null) {
                    $waiting['deadline'] = $now + $waiting['timeout'];
                }
            }
            unset($waiting);
        }
        if ($this->unsent === '' && $this->writer !== null) {
            $loop->cancel($this->writer);
            $this->writer = null;
        } elseif ($this->unsent !== '' && $this->writer === null) {
            $this->writer = $loop->onWritable($this->stream, function () use ($loop): void {
                if (!$this->flush($loop)) {
                    $this->fail('the connection was lost while sending');
                }
            });
        }
        $this->watchReply($loop);
        return true;
    }

    /**
     * Reads what has arrived and wakes, for each whole reply, whoever
     * sent the command it answers.
     */
    private function read(EventLoop $loop): void
    {
        $bytes = @fread($this->stream, 65_536);
        if ($bytes === false || $bytes === '' && feof($this->stream)) {
            $this->fail('the server closed the connection');
            return;
        }
        $this->received .= $bytes;
        $offset = 0;
        try {
            while ($this->waiting !== []) {
                $reply = Resp::reply($this->received, $offset, $complete);
                if (!$complete) {
                    break;
                }
                $waiting = array_shift($this->waiting);
                $suspension = $waiting['suspension'];
                $name = $waiting['name'];
                if ($reply instanceof RedisError) {
                    $suspension->throw(new RedisException("Redis refused {$name}: {$reply->message}"));
                } else {
                    $suspension->resume($reply);
                }
            }
        } catch (RedisException $error) {
            $this->fail($error->getMessage());
            return;
        }
        $this->received = substr($this->received, $offset);
        $this->watchReply($loop);
        if ($this->waiting === []) {
            $loop->unreference($this->reader);
        }
    }

    /**
     * Drops the connection and fails every command that waits on it.
     */
    private function fail(string|RedisException $why): void
    {
        $waiting = $this->waiting;
        $this->drop();
        foreach ($waiting as $command) {
            $error = $why instanceof RedisException
                ? $why
                : new RedisException("Redis command {$command['name']} failed: {$why}.");
            $command['suspension']->throw($error);
        }
    }

    /**
     * Forgets the connection, closed, with nothing waiting on it.
     */
    private function drop(): void
    {
        // The watchers belong to the loop they were made on, which may no longer be the current one.
        foreach ([$this->reader, $this->writer, $this->replyTimer] as $watcher) {
            if ($watcher === null) {
                continue;
            }

            $this->loop?->cancel($watcher);
        }
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
        $this->stream = $this->reader = $this->writer = $this->replyTimer = $this->loop = null;
        $this->replyDeadline = null;
        $this->queued = $this->written = 0;
        $this->received = $this->unsent = '';
        $this->waiting = [];
    }
}
