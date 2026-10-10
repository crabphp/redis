<?php

declare(strict_types=1);

namespace Crab\Redis;

/**
 * Redis's wire format, RESP2: how a command is written and how a reply
 * is read.
 *
 * @internal
 */
final class Resp
{
    /**
     * A command as Redis takes it: an array of strings, each with its length.
     *
     * @param list<string|int|float> $words The command's name and its arguments.
     */
    public static function command(array $words): string
    {
        $bytes = '*' . count($words) . "\r\n";
        foreach ($words as $word) {
            $word = (string) $word;
            $bytes .= '$' . strlen($word) . "\r\n" . $word . "\r\n";
        }
        return $bytes;
    }

    /**
     * Reads one reply from the bytes received so far, starting at the
     * offset, and moves the offset past it. A reply that has not all
     * arrived leaves the offset where it was.
     *
     * A reply is a string, an integer, null or a list of replies. An
     * error is returned as a RedisError and not thrown, since it is a
     * reply like any other and the ones after it must still be read.
     *
     * @param bool $complete Set to whether a whole reply was there to read.
     */
    public static function reply(string $bytes, int &$offset, ?bool &$complete): mixed
    {
        $complete = false;
        $end = strpos($bytes, "\r\n", $offset);
        if ($end === false) {
            return null;
        }
        $type = $bytes[$offset];
        $line = substr($bytes, $offset + 1, $end - $offset - 1);
        $next = $end + 2;
        if ($type === '$') {
            $length = (int) $line;
            if ($length >= 0) {
                if (strlen($bytes) < $next + $length + 2) {
                    return null;
                }
                $offset = $next + $length + 2;
                $complete = true;
                return substr($bytes, $next, $length);
            }
            $value = null;
        } elseif ($type === '*') {
            $count = (int) $line;
            $value = $count < 0 ? null : [];
            for ($index = 0; $index < $count; ++$index) {
                $item = self::reply($bytes, $next, $whole);
                if (!$whole) {
                    return null;
                }
                $value[] = $item;
            }
        } else {
            $value = match ($type) {
                '+' => $line,
                ':' => (int) $line,
                '-' => new RedisError($line),
                default => throw new RedisException('Redis sent a reply that cannot be read; it starts with "' . addcslashes($type, "\0..\37\177..\377") . '".'),
            };
        }
        $offset = $next;
        $complete = true;
        return $value;
    }
}
