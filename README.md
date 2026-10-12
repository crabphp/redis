# Crab Redis

Redis for the [Crab framework](../framework): a cache store, a queue store and a broadcast carrier. A command waits for Redis without holding its worker, so other requests and jobs go on meanwhile. The client is Crab's own, on the framework's engine, with no other dependency.

Use it when several servers share a cache, when the cache is read too often for the database, or when the queue has more workers than a database table hands jobs to comfortably. A small application does not need it: the framework's file and database stores are enough.

## Installation

```sh
composer require crabphp/redis
```

The cache, queue and broadcast providers are registered by default. Add the Redis provider to replace their stores:

```php
// app.php
use App\Providers\AppProvider;
use Crab\Redis\RedisProvider;

->withProviders([
    RedisProvider::class,
    AppProvider::class,
])
```

Then choose which services use Redis:

```php
// config/cache.php
return ['store' => 'redis'];

// config/queue.php
return ['store' => 'redis'];

// config/broadcast.php
return ['carrier' => 'redis', 'prefix' => 'shop:broadcast:'];
```

Nothing else changes. `Cache`, `Queue`, `queue:work`, supervisors, retries and `queue:failed` work as they do on the other stores. There is no table to install.

## Configuration

```php
// config/redis.php
return [
    'url' => env('REDIS_URL', 'redis://127.0.0.1:6379'),
    'prefix' => 'shop:',
    'reply_timeout' => 5.0,
];
```

| Setting | Default | |
| --- | --- | --- |
| `redis.url` | `redis://127.0.0.1:6379` | Where the server is. `redis://user:secret@host:6379/1` names who asks and which database; `?password=secret&database=1` does the same. `rediss://` for an encrypted connection |
| `redis.prefix` | `crab:` | Put before every key, to keep two applications apart on one server |
| `redis.timeout` | `2.0` | Seconds to wait for a connection before a command fails |
| `redis.reply_timeout` | `5.0` | Positive seconds to wait for a reply after writing a command, in addition to its explicit blocking timeout |
| `redis.tls` | none | PHP's `ssl` context options for a `rediss://` address, such as `['cafile' => '/etc/ssl/redis-ca.pem']` |

The cache's keys start with the prefix and `cache:`, the queue's with the prefix and `queue:`. `cache:clear` removes the cache's keys and nothing else on the server.

Set `reply_timeout` when a stalled Redis connection should fail sooner, or when your commands need longer to answer. With no provider, pass it as `new EngineClient($url, replyTimeout: 0.5)`.

## Broadcasting

With `broadcast.carrier = 'redis'`, the provider binds `Crab\Contracts\Broadcast\Carrier` to `Crab\Redis\BroadcastCarrier`. Inject the framework broadcaster and send arrays or UTF-8 text:

```php
$this->broadcast->to('orders.42')->send(['status' => 'shipped']);
```

Publications use the ordinary client. Each worker opens one dedicated subscription on its first local socket join, subscribes on the first join to a channel and unsubscribes after the last leave. `broadcast.prefix` defaults to `crab:broadcast:` independently of `redis.prefix`; set a distinct broadcast prefix for each application sharing Redis.

Delivery is at most once, ordered per sender and channel, with no replay. A subscription gap is logged once; a 50 ms monitor closes joined local sockets with 1012 so clients reconnect and refetch current state. The old subscription is retired, and the next join opens a fresh one. Stop and reload close the subscription and cancel its watchers. See the framework [Broadcasting guide](../framework/docs/broadcasting.md) for limits, transactions and tests.

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

## Publishing and subscribing

Use channels for live notifications across workers, such as an order update sent to every worker that has an interested browser. Publish on the ordinary client; each subscription gets a connection of its own:

```php
$subscription = $redis->subscribe('orders.42', 'orders.43');
$subscription->add('orders.44');
$subscription->remove('orders.42');

$receivers = $redis->publish('orders.43', $payload);

$gaps = $subscription->gaps();
try {
    foreach ($subscription as $channel => $payload) {
        if ($subscription->gaps() !== $gaps) {
            $gaps = $subscription->gaps();
            // Reload current application state: messages may have been missed.
            continue; // The reload covers this notification too.
        }
        // Handle this channel and payload. Waiting suspends only this fiber.
    }
} finally {
    $subscription->close();
}
```

`publish()` returns Redis's receiver count, including the sender if it subscribes too. It does not mean the receivers have processed the payload. Channel names and payloads are binary safe; `redis.prefix` applies to store keys, and is not added to channel names. An empty `subscribe()` is useful when channels will be added later.

An owner that must cancel while subscription setup is still waiting can call `$redis->subscription()` to create an unopened subscription, keep it, then call `$subscription->add('orders')`. Creating it performs no I/O; `close()` can then cancel setup before `add()` returns. `subscribe()` remains the shortcut that creates and confirms the requested channels.

`add()` and `remove()` wait for Redis's confirmations. Adding an existing channel or removing an unknown one is harmless. Removing every channel leaves the subscription open. A transport failure during confirmation throws `RedisException`; the desired channel set is still restored on reconnect. Only one iterator can consume a subscription at a time, including while its body processes a yielded message. Finish that iterator before consuming again, or create another subscription.

Messages arrive in wire order. Each subscription buffers at most 10,000 undelivered messages by default; when full it drops the oldest and increments `dropped()`. On a connection gap it also discards every undelivered message and counts those in `dropped()`, including a message that has woken the consumer but has not yet been yielded. After `gaps()` changes, everything yielded was published after the reconnect. The example reloads state and skips the notification that triggered that reload. Reads continue while the consumer is busy. Check `dropped()` as well as `gaps()` if missing a notification means application state must be reloaded. These counters are cumulative and polling them from another fiber also works while the iterator waits.

A lost connection reconnects automatically, restores its current channels, and waits 0.1 seconds before its first retry, doubling up to a five-second cap. Each outage increments `gaps()` and reports once through the engine's `FailureReporter`, which a framework worker sends to its failure logger. [Redis pub/sub delivers at most once](https://redis.io/docs/latest/develop/pubsub/): anything published during the gap is gone. Use the queue when work must survive a disconnection.

If Redis refuses credentials, the database or channel permissions, the subscription stops retrying, reports the refusal once, and throws a password-free `RedisException` to its iterator and to subsequent `add()`/`remove()` calls. Create a new client with updated settings and subscribe again. Transport failures continue to reconnect automatically.

Waiting for a publication has no reply deadline. After 30 idle seconds the connection sends `PING`; without an answer within five seconds it reconnects and reports a gap. Channel confirmations and `AUTH`/`SELECT` still use `redis.reply_timeout`. For standalone clients, change buffer and heartbeat limits through constructor arguments:

```php
$redis = new EngineClient(
    $url,
    subscriptionBuffer: 1_000,
    subscriptionIdleInterval: 15.0,
    subscriptionPingTimeout: 3.0,
);
```

Create subscriptions in the worker that uses them. They resolve its event loop on first use and use the URL's TLS, `AUTH` and `SELECT` settings just like ordinary commands. `close()` ends a waiting iterator cleanly and cancels reconnecting; `$redis->close()` closes every subscription and its ordinary connection without suspending. Closing during connection opening cancels the attempt and queued commands; its socket, watchers and timer are released before close returns, even if the loop has stopped. `RedisProvider` does this when the worker stops. A new ordinary command may connect again after client close; a closed subscription needs replacing.

The client continues to use RESP2, Redis's default protocol; it does not send `HELLO` or negotiate RESP3. Pattern subscriptions (`PSUBSCRIBE`) and sharded pub/sub are outside this API.

## What to know

- **One server.** The queue's scripts build key names from the prefix, which a Redis Cluster does not allow. Use one server, or a primary with replicas.
- **The server's name** is looked up through the system's name servers and hosts file, within `redis.timeout`, and never in a way that holds the worker.
- **When Redis is down,** a command fails after `redis.timeout` seconds at most, at once when the connection is refused. The cache does not fall back to another store; a request that needs it answers `500`.
- **Jobs run at least once,** as on every store. A job a worker took and never reported on is given out again after `queue.retry_after` seconds.
- **Outside a worker,** in a command such as `cache:clear`, each Redis command opens a connection and closes it. Inside a worker one connection is kept.
- **A command that waits, such as `BLPOP`, holds the worker's connection** until it returns, and the worker's other commands wait behind it. The stores use none. For one of your own, make a second `EngineClient` for it.
- **A reply has a deadline.** A server that accepts a command and never answers fails it after `redis.reply_timeout` with `RedisException` naming the command. Every command waiting on that connection fails with the same reason; the next command connects again. This applies outside workers too.
- **Explicit blocking time is added to the reply deadline.** `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BLMOVE`, `BZPOPMIN` and `BZPOPMAX` give seconds as their last argument; `XREAD` and `XREADGROUP` give milliseconds with `BLOCK`. A block of `0` waits indefinitely. Other commands use the ordinary reply timeout.
- **The cache's clock is yours.** An entry carries the time it expires, and the application's clock decides. Redis drops it a minute later on its own.

## How it is built

The client is Crab's own, `EngineClient`, written on the engine's event loop and name resolver. The package needs nothing but the framework.

- **One connection per worker.** Every command of a worker goes down it one after another, without waiting for the reply before, and each reply wakes whoever sent its command. Fifty requests asking Redis at once are fifty commands on one connection.
- **One reply timer per connection.** Each command's deadline starts when it is written. The timer watches the earliest deadline and moves to the next after a reply, so a pipeline does not need a timer for every command.
- **A connection that fails is made again.** The commands waiting on it fail with `RedisException`, and the next command connects afresh.
- **The stores know one interface,** `Crab\Redis\Client`. They use `command()`; applications can also `publish()`, `subscribe()` and `close()`.

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
The pub/sub restart and TLS checks start disposable Redis processes on unused ports and stop only those processes. Scripted peers cover stalled heartbeats and control replies.
