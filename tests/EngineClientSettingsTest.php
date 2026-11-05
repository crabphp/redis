<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\Redis\EngineClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class EngineClientSettingsTest extends TestCase
{
    /**
     * @return iterable<string, array{string}>
     */
    public static function invalidDatabases(): iterable
    {
        yield 'name' => ['/cache'];
        yield 'trailing text' => ['/2oops'];
        yield 'nested path' => ['/1/2'];
        yield 'extra slash' => ['//2'];
        yield 'negative' => ['/-1'];
        yield 'fraction' => ['?database=1.5'];
        yield 'exponent' => ['?database=1e2'];
        yield 'empty' => ['?database='];
        yield 'array' => ['?database[]=2'];
        yield 'overflow' => ['?database=99999999999999999999'];
    }

    #[DataProvider('invalidDatabases')]
    public function testAnInvalidDatabaseCannotSelectAnotherDatabaseSilently(string $suffix): void
    {
        $failure = null;
        try {
            new EngineClient('redis://user:secret@127.0.0.1:6379' . $suffix);
        } catch (\InvalidArgumentException $error) {
            $failure = $error;
        }

        self::assertInstanceOf(\InvalidArgumentException::class, $failure);
        self::assertStringContainsString('database', $failure->getMessage());
        self::assertStringNotContainsString('secret', $failure->getMessage());
    }

    /**
     * @return iterable<string, array{float}>
     */
    public static function invalidTimeouts(): iterable
    {
        yield 'zero' => [0.0];
        yield 'negative' => [-1.0];
        yield 'infinity' => [INF];
        yield 'not a number' => [NAN];
    }

    #[DataProvider('invalidTimeouts')]
    public function testAConnectionTimeoutMustBePositiveAndFinite(float $timeout): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('connection timeout');
        new EngineClient('redis://127.0.0.1:6379', timeout: $timeout);
    }

    public function testAnArrayPasswordCannotSilentlyDisableAuthentication(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('password must be a string');
        new EngineClient('redis://127.0.0.1:6379?password[]=secret');
    }
}
