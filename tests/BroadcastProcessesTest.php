<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

require_once dirname(__DIR__) . '/vendor/autoload.php';

use PHPUnit\Framework\TestCase;

/**
 * An opt-in background application with two HTTP workers and a Redis queue
 * worker. Its isolated directory owns every process started by this test.
 */
final class BroadcastProcessesTest extends TestCase
{
    private string $directory;
    private string $address;
    private array $clients = [];

    protected function setUp(): void
    {
        $url = getenv('CRAB_TEST_REDIS_URL');
        if ($url === false || $url === '' || !extension_loaded('pcntl') || !extension_loaded('posix')) {
            self::markTestSkipped(
                'The orchestrator supplies throwaway Redis and pcntl/posix for the process integration test.',
            );
        }
        if ((parse_url($url, PHP_URL_PORT) ?? 6379) !== 6399) {
            self::markTestSkipped('Use throwaway Redis on port 6399; the owner Redis on port 6379 is off limits.');
        }
        $this->directory = sys_get_temp_dir() . '/crab-broadcast-' . bin2hex(random_bytes(8));
        mkdir($this->directory);
        $autoload = dirname(__DIR__) . '/vendor/autoload.php';
        file_put_contents(
            $this->directory . '/cua',
            "<?php\nrequire "
            . var_export($autoload, true)
            . ";\n\$app = require __DIR__ . '/app.php';\nexit(\$app->console->run(array_slice(\$argv, 1)));\n",
        );
        $source = <<<'APP'
            <?php

            declare(strict_types=1);

            use Crab\Contracts\Broadcast\Broadcaster;
            use Crab\Contracts\Queue\Queue;
            use Crab\Contracts\WebSocket\Socket;

            final readonly class WatchBroadcast
            {
                public function __construct(private Broadcaster $broadcast, private Queue $queue) {}

                public function handle(Socket $socket): void
                {
                    $socket->join('orders');
                    $socket->send(['worker' => getmypid()]);
                    foreach ($socket as $message) {
                        if ($message === 'job') {
                            $this->queue->push(AnnounceFromQueue::class);
                        } else {
                            $this->broadcast->to('orders')->send(['via' => 'http', 'pid' => getmypid(), 'label' => $message]);
                        }
                    }
                }
            }

            final readonly class AnnounceFromQueue
            {
                public function __construct(private Broadcaster $broadcast) {}

                public function handle(): void
                {
                    $this->broadcast->to('orders')->send(['via' => 'queue', 'pid' => getmypid()]);
                }
            }

            $app = new Crab\App(new Crab\Config\Config([
                'server' => ['with' => ['queue'], 'drain' => 1.0],
                'websocket' => ['ping' => 0],
                'broadcast' => ['carrier' => 'redis', 'prefix' => PREFIX . 'broadcast:'],
                'redis' => ['url' => REDIS_URL, 'prefix' => PREFIX],
                'queue' => ['store' => 'redis', 'sleep' => 0.01, 'grace' => 1.0],
                'log' => ['path' => __DIR__ . '/app.log'],
            ]), basePath: __DIR__);
            $app->register(...Crab\App::PROVIDERS);
            $app->register(Crab\Redis\RedisProvider::class);
            $app->router->websocket('/live', WatchBroadcast::class);
            return $app;
            APP;
        $source = str_replace(
            ['REDIS_URL', 'PREFIX'],
            [var_export($url, true), var_export('broadcast-process:' . bin2hex(random_bytes(8)) . ':', true)],
            $source,
        );
        file_put_contents($this->directory . '/app.php', $source);
        for ($port = 8200; $port <= 8299; ++$port) {
            $probe = @stream_socket_server("tcp://127.0.0.1:{$port}");
            if ($probe !== false) {
                $this->address = "tcp://127.0.0.1:{$port}";
                fclose($probe);
                return;
            }
        }
        self::fail('No integration test port is free between 8200 and 8299.');
    }

    protected function tearDown(): void
    {
        if (!isset($this->directory)) {
            return;
        }
        foreach ($this->clients as $client) {
            if (!is_resource($client)) {
                continue;
            }

            fclose($client);
        }
        $this->command('stop', '--force', '--timeout=3');
        $files = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($this->directory, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );
        foreach ($files as $file) {
            $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        }
        rmdir($this->directory);
    }

    public function testMessagesFromBothHttpWorkersAndAQueueWorkerReachOtherProcesses(): void
    {
        [$status, $output] = $this->command('serve', '--daemon', '--workers=2', '--address=' . $this->address);
        self::assertSame(0, $status, $output);
        $a = $this->connect();
        $aPid = $this->receive($a, 'the first HTTP worker identity')['worker'];
        $b = null;
        $bPid = null;
        for ($attempt = 0; $attempt < 30; ++$attempt) {
            $candidate = $this->connect();
            $pid = $this->receive($candidate, 'an HTTP worker identity')['worker'];
            if ($pid !== $aPid) {
                $b = $candidate;
                $bPid = $pid;
                break;
            }
            fclose($candidate);
        }
        self::assertIsResource($b, 'No socket reached the second HTTP worker after 30 attempts.');
        $this->send($a, 'from a');
        self::assertSame(
            ['via' => 'http', 'pid' => $aPid, 'label' => 'from a'],
            $this->receive($b, 'worker a publication on worker b'),
        );
        self::assertSame($aPid, $this->receive($a, 'worker a publication on its own socket')['pid']);
        $this->send($b, 'from b');
        self::assertSame(
            ['via' => 'http', 'pid' => $bPid, 'label' => 'from b'],
            $this->receive($a, 'worker b publication on worker a'),
        );
        self::assertSame($bPid, $this->receive($b, 'worker b publication on its own socket')['pid']);
        $this->send($a, 'job');
        $job = $this->receive($b, 'the queue worker publication on worker b');
        self::assertSame('queue', $job['via']);
        self::assertNotContains($job['pid'], [$aPid, $bPid]);
        self::assertSame($job, $this->receive($a, 'the queue worker publication on worker a'));
        fclose($a);
        fclose($b);
        [$status, $output] = $this->command('stop', '--timeout=3');
        self::assertSame(0, $status, $output);
        self::assertFileDoesNotExist($this->directory . '/storage/crab.pid');
        self::assertFalse(@posix_kill($aPid, 0));
        self::assertFalse(@posix_kill($bPid, 0));
        self::assertFalse(@posix_kill($job['pid'], 0));
    }

    private function connect(): mixed
    {
        $client = stream_socket_client($this->address, $errno, $error, 3);
        self::assertIsResource($client, $error);
        $this->clients[] = $client;
        stream_set_blocking($client, false);
        $this->write(
            $client,
            "GET /live HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
            . base64_encode(random_bytes(16))
            . "\r\n\r\n",
            'writing the WebSocket upgrade request',
        );
        $head = '';
        $deadline = microtime(true) + 3.0;
        while (!str_ends_with($head, "\r\n\r\n")) {
            $head .= $this->bytes($client, 1, $deadline, 'the WebSocket 101 upgrade');
        }
        self::assertStringStartsWith('HTTP/1.1 101', $head);
        return $client;
    }

    private function send(mixed $client, string $text): void
    {
        $mask = random_bytes(4);
        $this->write(
            $client,
            "\x81" . chr(128 | strlen($text)) . $mask . ($text ^ str_repeat($mask, intdiv(strlen($text) + 3, 4))),
            "writing the {$text} socket message",
        );
    }

    private function receive(mixed $client, string $what): array
    {
        $deadline = microtime(true) + 3.0;
        $head = $this->bytes($client, 2, $deadline, $what);
        self::assertSame(1, ord($head[0]) & 15, 'Expected a JSON text update, not a close frame.');
        $length = ord($head[1]) & 127;
        if ($length === 126) {
            $length = unpack('n', $this->bytes($client, 2, $deadline, $what))[1];
        }
        return json_decode($this->bytes($client, $length, $deadline, $what), true, flags: JSON_THROW_ON_ERROR);
    }

    private function bytes(mixed $client, int $length, float $deadline, string $what): string
    {
        $bytes = '';
        while (strlen($bytes) < $length) {
            $this->ready($client, false, $deadline, $what);
            $part = fread($client, $length - strlen($bytes));
            if ($part === false || $part === '' && feof($client)) {
                self::fail("The socket ended while waiting for {$what}.");
            }
            $bytes .= $part;
        }
        return $bytes;
    }

    private function write(mixed $client, string $bytes, string $what): void
    {
        $deadline = microtime(true) + 3.0;
        while ($bytes !== '') {
            $this->ready($client, true, $deadline, $what);
            $sent = fwrite($client, $bytes);
            if ($sent === false) {
                self::fail("The socket failed while {$what}.");
            }
            $bytes = substr($bytes, $sent);
        }
    }

    /**
     * Share one deadline across the complete frame or HTTP head, so a
     * peer sending a byte at a time cannot extend a test indefinitely.
     */
    private function ready(mixed $client, bool $writable, float $deadline, string $what): void
    {
        $left = $deadline - microtime(true);
        if ($left <= 0) {
            self::fail("Timed out after three seconds waiting for {$what}.");
        }
        $read = $writable ? [] : [$client];
        $write = $writable ? [$client] : [];
        $except = [];
        $ready = stream_select($read, $write, $except, (int) $left, (int) (($left - (int) $left) * 1_000_000));
        if ($ready === 0) {
            self::fail("Timed out after three seconds waiting for {$what}.");
        }
        if ($ready === false) {
            self::fail("Could not wait for {$what}.");
        }
    }

    private function command(string ...$arguments): array
    {
        $process = proc_open(
            [PHP_BINARY, $this->directory . '/cua', ...$arguments],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
            $this->directory,
        );
        self::assertIsResource($process);
        foreach ($pipes as $pipe) {
            stream_set_blocking($pipe, false);
        }
        $seconds = $arguments[0] === 'serve' ? 30.0 : 10.0;
        $deadline = microtime(true) + $seconds;
        $output = '';
        $timedOut = false;
        try {
            do {
                foreach ($pipes as $pipe) {
                    $output .= stream_get_contents($pipe);
                }
                $status = proc_get_status($process);
                if (!$status['running']) {
                    break;
                }
                if (microtime(true) >= $deadline) {
                    $timedOut = true;
                    proc_terminate($process, SIGKILL);
                    break;
                }
                usleep(10_000);
            } while (true);
            foreach ($pipes as $pipe) {
                $output .= stream_get_contents($pipe);
            }
        } finally {
            foreach ($pipes as $pipe) {
                fclose($pipe);
            }
            $closed = proc_close($process);
        }
        if ($timedOut) {
            self::fail(
                'Timed out after '
                . $seconds
                . ' seconds waiting for cua '
                . implode(' ', $arguments)
                . " to exit.\n"
                . $output,
            );
        }
        return [$status['exitcode'] >= 0 ? $status['exitcode'] : $closed, $output];
    }
}
