<?php

declare(strict_types=1);

namespace Crab\Redis;

use Crab\Contracts\Broadcast\Carrier;
use Crab\Engine\Async\Task;
use Crab\Engine\EventLoop;
use Crab\Engine\FailureReporter;

/**
 * Publishes on the ordinary client and consumes one dedicated subscription.
 * Serialized changes confirm joins, while leaves never suspend socket cleanup.
 */
final class BroadcastCarrier implements Carrier
{
    /**
     * @var array<string, array{\Closure, \Closure, object}>
     */
    private array $channels = [];
    private ?Subscription $subscription = null;
    private ?Task $changing = null;
    /**
     * @var array<int, Task>
     */
    private array $changes = [];
    private ?EventLoop $loop = null;
    private ?string $consumer = null;
    private ?string $monitor = null;
    private int $gaps = 0;
    private int $dropped = 0;
    private bool $closed = false;

    public function __construct(private readonly Client $client, private readonly string $prefix = 'crab:broadcast:')
    {
    }

    public function publish(array $channels, string $message): void
    {
        $this->requireOpen();
        foreach ($channels as $channel) {
            $this->client->publish($this->prefix . $channel, $message);
        }
    }

    public function subscribe(string $channel, \Closure $receive, \Closure $gap): void
    {
        $this->requireOpen();
        $wire = $this->prefix . $channel;
        $joined = [$receive, $gap, new \stdClass()];
        $this->channels[':' . $wire] = $joined;
        $task = $this->change(function () use ($wire): void {
            if ($this->subscription === null) {
                $subscription = $this->client->subscription();
                $this->subscription = $subscription;
                try {
                    $subscription->add($wire);
                } catch (\Throwable $error) {
                    $subscription->close();
                    if ($this->subscription === $subscription) {
                        $this->subscription = null;
                    }
                    throw $error;
                }
                if ($this->closed) {
                    $subscription->close();
                    return;
                }
                $this->consumer = $this->loop->defer(fn () => $this->consume($subscription));
                // Subscription reports outages even when no message arrives.
                $this->monitor = $this->loop->repeat(0.05, $this->checkGap(...));
            } else {
                $this->subscription->add($wire);
            }
        });
        try {
            $task->await();
            $this->requireOpen();
        } catch (\Throwable $error) {
            // A leave and replacement join can overtake a failed setup.
            if (($this->channels[':' . $wire] ?? null) === $joined) {
                unset($this->channels[':' . $wire]);
            }
            throw $error;
        }
    }

    public function unsubscribe(string $channel): void
    {
        $wire = $this->prefix . $channel;
        if (!isset($this->channels[':' . $wire])) {
            return;
        }
        unset($this->channels[':' . $wire]);
        if ($this->closed) {
            return;
        }
        $task = $this->change(fn () => $this->subscription?->remove($wire));
        $task->onComplete(function (Task $done): void {
            try {
                $done->result();
            } catch (\Throwable $error) {
                if (!$this->closed) {
                    FailureReporter::report('Redis broadcast could not leave its channel', $error);
                }
            }
        });
    }

    private function change(\Closure $operation): Task
    {
        $this->loop ??= EventLoop::current();
        $previous = $this->changing;
        $task = Task::spawn($this->loop, function () use ($previous, $operation): void {
            if ($previous !== null) {
                try {
                    $previous->await();
                } catch (\Throwable $error) {
                    // Its caller already owns the failure; this change still
                    // reconciles the desired set after that failed change.
                    if ($this->closed) {
                        return;
                    }
                }
            }
            if (!$this->closed) {
                $operation();
            }
        });
        $this->changing = $task;
        $this->changes[spl_object_id($task)] = $task;
        $task->onComplete(function (Task $done): void {
            unset($this->changes[spl_object_id($done)]);
            if ($this->changing === $done) {
                $this->changing = null;
            }
        });
        return $task;
    }

    private function consume(Subscription $subscription): void
    {
        $this->consumer = null;
        try {
            foreach ($subscription as $wire => $payload) {
                if ($this->subscription !== $subscription) {
                    break;
                }
                $this->checkGap();
                $receiver = $this->channels[':' . $wire][0] ?? null;
                if ($receiver !== null) {
                    $receiver(substr($wire, strlen($this->prefix)), $payload);
                }
            }
        } catch (\Throwable $error) {
            if (!$this->closed && $this->subscription === $subscription) {
                $this->notifyGap();
                // Subscription already reports its transport gaps and
                // terminal Redis refusals; delivery callback failures do not.
                if (!$error instanceof RedisException) {
                    FailureReporter::report('Redis broadcast delivery ended; reconnect clients and check Redis settings', $error);
                }
            }
        }
    }

    private function checkGap(): void
    {
        if ($this->subscription === null) {
            return;
        }
        $gaps = $this->subscription->gaps();
        $dropped = $this->subscription->dropped();
        if ($gaps !== $this->gaps || $dropped !== $this->dropped) {
            if ($gaps === $this->gaps && $dropped !== $this->dropped) {
                FailureReporter::report('Redis broadcast buffer lost updates; reconnect clients and refetch state');
            }
            $this->gaps = $gaps;
            $this->dropped = $dropped;
            $this->notifyGap();
        }
    }

    private function notifyGap(): void
    {
        // Membership supplies one callback for all channels, so call each
        // distinct callback once even if many channels share it.
        $callbacks = [];
        foreach ($this->channels as [, $gap]) {
            $callbacks[spl_object_id($gap)] = $gap;
        }
        // Retire this subscription as well: retrying an outage with no
        // remaining local clients only creates work and duplicate failures.
        // The next join opens a fresh subscription before it returns.
        $this->channels = [];
        $this->subscription?->close();
        $this->subscription = null;
        $this->gaps = $this->dropped = 0;
        if ($this->monitor !== null) {
            $this->loop->cancel($this->monitor);
            $this->monitor = null;
        }
        if ($this->consumer !== null) {
            $this->loop->cancel($this->consumer);
            $this->consumer = null;
        }
        foreach ($callbacks as $gap) {
            try {
                $gap();
            } catch (\Throwable $error) {
                FailureReporter::report('Redis broadcast gap callback failed; check client cleanup', $error);
            }
        }
    }

    private function requireOpen(): void
    {
        if ($this->closed) {
            throw new \LogicException('The Redis broadcast carrier is stopped; send and join in a running worker.');
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        if ($this->monitor !== null) {
            $this->loop->cancel($this->monitor);
            $this->monitor = null;
        }
        if ($this->consumer !== null) {
            $this->loop->cancel($this->consumer);
            $this->consumer = null;
        }
        $this->subscription?->close();
        foreach ($this->changes as $task) {
            $task->cancel();
        }
        $this->channels = [];
    }
}
