<?php

declare(strict_types=1);

namespace Crab\Redis;

/**
 * Redis could not be reached, or refused a command.
 */
final class RedisException extends \RuntimeException {}
