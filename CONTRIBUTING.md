# Contributing

Setup, tests, formatting, linting and conventions are the same as the framework's: see [crabphp/framework's CONTRIBUTING.md](https://github.com/crabphp/framework/blob/main/CONTRIBUTING.md). Before a pull request, run here:

```sh
composer format:check
composer lint
CRAB_TEST_REDIS_URL=redis://127.0.0.1:6399 composer test
```

See the [README](README.md#tests) for Redis and process test requirements. CI requires every integration test to run.
