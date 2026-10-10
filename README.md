# Crab Redis

Redis for the [Crab framework](../framework): a store for the cache and a store for the queue. A command waits for Redis without holding its worker, so other requests and jobs go on meanwhile. The client is Crab's own, on the framework's engine, with no other dependency.

Use it when several servers share a cache, when the cache is read too often for the database, or when the queue has more workers than a database table hands jobs to comfortably. A small application does not need it: the framework's file and database stores are enough.

## Installation

```sh
composer require crabphp/redis
```

Register the provider after the cache and queue providers, whose stores it takes the place of:

```php
// app.php
use Crab\Redis\RedisProvider;

->withProviders([
    CacheProvider::class,
    QueueProvider::class,
    RedisProvider::class,
    AppProvider::class,
])
```

Then say which of the two is kept in Redis. Either may be, or both:

```php
// config/cache.php
return ['store' => 'redis'];

// config/queue.php
return ['store' => 'redis'];
```

Nothing else changes. `Cache`, `Queue`, `queue:work`, supervisors, retries and `queue:failed` work as they do on the other stores. There is no table to install.

## Configuration

```php
// config/redis.php
return [
    'url' => env('REDIS_URL', 'redis://127.0.0.1:6379'),
    'prefix' => 'shop:',
];
```

| Setting | Default | |
| --- | --- | --- |
| `redis.url` | `redis://127.0.0.1:6379` | Where the server is. `redis://user:secret@host:6379/1` names who asks and which database; `?password=secret&database=1` does the same. `rediss://` for an encrypted connection |
| `redis.prefix` | `crab:` | Put before every key, to keep two applications apart on one server |
| `redis.timeout` | `2.0` | Seconds to wait for a connection before a command fails |
| `redis.tls` | none | PHP's `ssl` context options for a `rediss://` address, such as `['cafile' => '/etc/ssl/redis-ca.pem']` |

The cache's keys start with the prefix and `cache:`, the queue's with the prefix and `queue:`. `cache:clear` removes the cache's keys and nothing else on the server.

## Using Redis yourself

Ask for `Crab\Redis\Client` in a constructor and send any command:

```php
use Crab\Redis\Client;

final readonly class CountVisit
{
    public function __construct(private Client $redis)
    {
    }

    public function handle(string $page): array
    {
        return ['visits' => $this->redis->command('INCR', "visits:{$page}")];
    }
}
```

A reply is as Redis gives it: a string, an integer, null, or a list of those. A server that cannot be reached, or a command it refuses, throws `Crab\Redis\RedisException`.

## What to know

- **One server.** The queue's scripts build key names from the prefix, which a Redis Cluster does not allow. Use one server, or a primary with replicas.
- **The server's name** is looked up through the system's name servers and hosts file, within `redis.timeout`, and never in a way that holds the worker.
- **When Redis is down,** a command fails after `redis.timeout` seconds at most, at once when the connection is refused. The cache does not fall back to another store; a request that needs it answers `500`.
- **Jobs run at least once,** as on every store. A job a worker took and never reported on is given out again after `queue.retry_after` seconds.
- **Outside a worker,** in a command such as `cache:clear`, each Redis command opens a connection and closes it. Inside a worker one connection is kept.
- **A command that waits, such as `BLPOP`, holds the worker's connection** until it returns, and the worker's other commands wait behind it. The stores use none. For one of your own, make a second `EngineClient` for it.
- **A reply has no time limit.** `redis.timeout` is for connecting. A server that accepts a command and never answers keeps its caller waiting.
- **The cache's clock is yours.** An entry carries the time it expires, and the application's clock decides. Redis drops it a minute later on its own.

## How it is built

The client is Crab's own, `EngineClient`, written on the engine's event loop and name resolver. The package needs nothing but the framework.

- **One connection per worker.** Every command of a worker goes down it one after another, without waiting for the reply before, and each reply wakes whoever sent its command. Fifty requests asking Redis at once are fifty commands on one connection.
- **A connection that fails is made again.** The commands waiting on it fail with `RedisException`, and the next command connects afresh.
- **The stores know one interface,** `Crab\Redis\Client`, with one method, `command()`.

To use another client, implement `Client` and bind it after the provider:

```php
$container->singleton(Client::class, static fn (): Client => new MyClient(config('redis.url')));
```

## Tests

The tests need a Redis server and are skipped without one:

```sh
CRAB_TEST_REDIS_URL=redis://127.0.0.1:6379 composer test
```

Each test writes under a prefix of its own and removes its keys afterwards.
