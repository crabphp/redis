<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Redis\EngineClient;
use Crab\Redis\Client;
use PHPUnit\Framework\TestCase;

/**
 * A test against a real Redis server, named by CRAB_TEST_REDIS_URL and
 * skipped without it. Each test has a prefix of its own and removes the
 * keys under it afterwards, so a server that holds other things is left
 * as it was.
 */
abstract class RedisTestCase extends TestCase
{
    protected Client $redis;
    protected string $prefix;
    protected string $url;

    protected function setUp(): void
    {
        $url = getenv('CRAB_TEST_REDIS_URL');
        if (!is_string($url) || $url === '') {
            self::markTestSkipped('Set CRAB_TEST_REDIS_URL, such as redis://127.0.0.1:6379, to run the tests that need Redis.');
        }
        $this->url = $url;
        $this->redis = new EngineClient($url);
        $this->prefix = 'crab-test:' . bin2hex(random_bytes(6)) . ':';
    }

    protected function tearDown(): void
    {
        if (!isset($this->redis)) {
            return;
        }
        $cursor = '0';
        do {
            [$cursor, $keys] = $this->redis->command('SCAN', $cursor, 'MATCH', $this->prefix . '*', 'COUNT', 500);
            if ($keys !== []) {
                $this->redis->command('DEL', ...$keys);
            }
        } while ((string) $cursor !== '0');
    }
}
