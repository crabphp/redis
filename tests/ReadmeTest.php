<?php

declare(strict_types=1);

namespace Crab\Redis\Tests;

use Crab\App;
use Crab\Contracts\Broadcast\Carrier;
use Crab\Contracts\Cache\Store as CacheStoreContract;
use Crab\Contracts\Container\Container;
use Crab\Contracts\Plugin;
use Crab\Contracts\Queue\Store as QueueStoreContract;
use Crab\Contracts\Session\Driver as SessionDriver;
use Crab\Redis\BroadcastCarrier;
use Crab\Redis\CacheStore;
use Crab\Redis\Client;
use Crab\Redis\QueueStore;
use Crab\Redis\SessionStore;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;

final class ReadmeTest extends TestCase
{
    #[RunInSeparateProcess]
    public function testTheInstallationExampleRegistersEveryStoreBeforeTheApplicationsSetup(): void
    {
        class_alias(ReadmeSetup::class, 'App\\Setup');
        $directory = sys_get_temp_dir() . '/crab-redis-readme-' . bin2hex(random_bytes(8));
        mkdir($directory);
        mkdir($directory . '/config');
        $files = [];
        try {
            $readme = file_get_contents(dirname(__DIR__) . '/README.md');
            preg_match_all('/```php\n(.*?)\n```/s', $readme, $blocks);
            foreach ($blocks[1] as $block) {
                if (preg_match('/\A\/\/ (app\.php|config\/[a-z]+\.php)\n/', $block, $match) !== 1) {
                    continue;
                }
                $path = $directory . '/' . $match[1];
                file_put_contents($path, "<?php\n" . $block);
                $files[] = $path;
            }
            $app = require $directory . '/app.php';

            self::assertInstanceOf(App::class, $app);
            self::assertSame([], $app->config->problems());
            self::assertTrue(ReadmeSetup::$registered, 'The application setup ran before Redis was registered.');
            self::assertInstanceOf(CacheStore::class, $app->container->get(CacheStoreContract::class));
            self::assertInstanceOf(QueueStore::class, $app->container->get(QueueStoreContract::class));
            self::assertInstanceOf(BroadcastCarrier::class, $app->container->get(Carrier::class));
            self::assertInstanceOf(SessionStore::class, $app->container->get(SessionDriver::class));
            self::assertSame('shop:', $app->config->get('redis.prefix'));
            self::assertSame('shop:broadcast:', $app->config->get('broadcast.prefix'));
        } finally {
            foreach ($files as $path) {
                unlink($path);
            }
            rmdir($directory . '/config');
            rmdir($directory);
        }
    }
}

final class ReadmeSetup implements Plugin
{
    public static bool $registered = false;

    public function register(Container $container): void
    {
        self::$registered =
            $container->has(Client::class)
            && $container->has(CacheStoreContract::class)
            && $container->has(QueueStoreContract::class)
            && $container->has(Carrier::class)
            && $container->has(SessionDriver::class);
    }
}
