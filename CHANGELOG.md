# Changelog

Crab Redis follows [semantic versioning](https://semver.org). Before 1.0, a minor version may break what an application writes; the entry says what to change.

## Unreleased

## 0.2.1 — 2026-11-13

- Requires `crabphp/framework` ^0.7.3, for its sessions.
- **Sessions in Redis:** with `session.driver` set to `redis`, `RedisPlugin` keeps the framework's sessions in Redis, under the prefix and `session:`, through the new `Crab\Redis\SessionStore`. List it after `SessionPlugin`. Redis drops a session when it expires, so there is nothing to sweep.

## 0.2.0 — 2026-11-12

- Requires `crabphp/framework` ^0.7, for its 0.7.0 release.

## 0.1.1 — 2026-11-07

- A failure only changes a job still reserved by that worker. Completing a job after another worker failed it also removes its failed index, so retrying cannot recreate an incomplete job.
- Queue payloads keep whole-valued floats, and job IDs stay exact beyond Lua's integer precision. Queue lists include queues whose only failed jobs are older than the latest 200.
- Overdue jobs are taken oldest first even when more than 100 are waiting. A continuing stream of newer overdue jobs no longer leaves the oldest behind a recovery batch.
- Cache increments use Redis's signed 64-bit arithmetic. Overflow throws and leaves the value and expiry unchanged.
- `redis.reply_timeout` also bounds writing a command, before its reply wait starts. A peer that stops reading no longer leaves large commands and their callers waiting forever.
- Invalid database text and array passwords in Redis URLs are refused before connecting. A standalone client's connection timeout must be positive and finite.
- The README shows complete framework 0.6.x wiring for cache, queue and broadcast. CI checks PHP 8.4 and 8.5 with framework v0.6.1 and main, including broadcast process tests on any explicitly configured test Redis port.

## 0.1.0 — 2026-11-05

- **First release,** for `crabphp/framework` 0.6: redis for the Crab framework: a cache store and a queue store that wait without holding a worker.
