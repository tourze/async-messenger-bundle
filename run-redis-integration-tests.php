#!/usr/bin/env php
<?php

echo "Redis Integration Tests Runner\n";
echo "==============================\n\n";

// Check if Redis is available
$redis = new Redis();
try {
    if (!$redis->connect('127.0.0.1', 6379)) {
        echo "ERROR: Cannot connect to Redis on 127.0.0.1:6379\n";
        echo "Please make sure Redis is running.\n";
        exit(1);
    }
    $redis->close();
    echo "✓ Redis connection successful\n\n";
} catch (Exception $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
    exit(1);
}

// Run PHPUnit tests
$testDir = __DIR__ . '/tests/Redis/Integration/';
$command = sprintf(
    'cd %s && vendor/bin/phpunit %s',
    escapeshellarg(dirname(dirname(__DIR__))),
    escapeshellarg($testDir)
);

echo "Running integration tests...\n";
echo "Command: $command\n\n";

passthru($command, $exitCode);

exit($exitCode);