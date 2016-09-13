<?php
const FILES_COUNT = 500000;

$nolaunch = true;

require("init.inc");

$all_messages = [];

$expected_len = 0;
echo "Creating lots of files\n";
for ($i = 0; $i < FILES_COUNT; $i++) {
    $msg = "Contents of file #$i\nAnd another line in the same file $i\n";
    $expected_len += file_put_contents("test/source/omg/file_" . sprintf("%06d", $i) . ".log", $msg . "\n");
}

echo "Files created, launching daemon\n";

launchDaemon();

echo "Letting daemon to collect all files\n";

for ($i = 0; $i < 500; $i++) {
    $f = "test/source/omg/file_" . sprintf("%06d", FILES_COUNT - 2) . ".log";
    if (!file_exists($f) && !file_exists($f . ".old")) {
        break;
    }

    mysleep(1);
}

assertOrDie((count($files = glob("test/source/omg/*"))) === 1);
assertOrDie($target_files = glob("test/target/omg/*"));

$actual_len = 0;
foreach ($target_files as $f) {
    if (is_link($f)) {
        continue;
    }

    $actual_len += filesize($f);
}

assertOrDie($expected_len === $actual_len);

$success = true;
