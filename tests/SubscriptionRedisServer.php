<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Engine\Async\Async;
use Crab\Engine\Process\Worker;
use Crab\Redis\EngineClient;
use Crab\Redis\RedisException;

/**
 * A disposable Redis process for restart checks. It owns an unused port,
 * its log and its process handle; stopping it signals only its own PID.
 */
final class SubscriptionRedisServer
{
    public readonly string $url;
    private readonly int $port;
    /**
     * @var resource|null
     */
    private mixed $process = null;
    /**
     * @var resource
     */
    private mixed $log;

    /**
     * @param array{cert: string, key: string}|array{} $tls
     */
    public function __construct(
        private readonly array $tls = [],
    ) {
        $listener = stream_socket_server('tcp://127.0.0.1:0', $code, $reason);
        if ($listener === false) {
            throw new \RuntimeException('Could not reserve a port for the disposable Redis: ' . $reason);
        }
        $address = stream_socket_get_name($listener, false);
        fclose($listener);
        $this->port = (int) substr($address, strrpos($address, ':') + 1);
        $this->url = ($tls === [] ? 'redis://' : 'rediss://') . $address;
        $this->log = tmpfile();
    }

    public function start(Worker $worker): void
    {
        $this->process = proc_open(
            [
                'redis-server',
                '--bind',
                '127.0.0.1',
                '--port',
                (string) ($this->tls === [] ? $this->port : 0),
                '--save',
                '',
                '--appendonly',
                'no',
                ...(
                    $this->tls === []
                        ? []
                        : [
                            '--tls-port',
                            (string) $this->port,
                            '--tls-cert-file',
                            $this->tls['cert'],
                            '--tls-key-file',
                            $this->tls['key'],
                            '--tls-ca-cert-file',
                            $this->tls['cert'],
                            '--tls-auth-clients',
                            'no',
                        ]
                ),
            ],
            [0 => ['file', '/dev/null', 'r'], 1 => $this->log, 2 => $this->log],
            $pipes,
        );
        if (!is_resource($this->process)) {
            throw new \RuntimeException('Could not start the disposable Redis process. Install redis-server on PATH.');
        }
        $redis = new EngineClient(
            $this->url,
            timeout: 0.5,
            tls: $this->tls === [] ? [] : ['cafile' => $this->tls['cert']],
        );
        $deadline = hrtime(true) + 5_000_000_000;
        while (hrtime(true) < $deadline) {
            if (!proc_get_status($this->process)['running']) {
                rewind($this->log);
                throw new \RuntimeException(
                    'The disposable Redis stopped during startup: ' . stream_get_contents($this->log),
                );
            }
            try {
                $redis->command('PING');
                $redis->close();
                return;
            } catch (RedisException) {
                // The process has started but its listener is not ready yet.
                Async::sleep($worker->loop, 0.01);
            }
        }
        throw new \RuntimeException('The disposable Redis did not start within five seconds.');
    }

    public function stop(Worker $worker): void
    {
        if (!is_resource($this->process)) {
            return;
        }
        proc_terminate($this->process, SIGTERM);
        $deadline = hrtime(true) + 5_000_000_000;
        while (proc_get_status($this->process)['running']) {
            if (hrtime(true) >= $deadline) {
                proc_terminate($this->process, SIGKILL);
                break;
            }
            Async::sleep($worker->loop, 0.01);
        }
        proc_close($this->process);
        $this->process = null;
    }
}
