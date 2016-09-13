<?php
require('init.inc');

$contents = "test\n";

echo "Writing a little bit immediately\n";
assertOrDie(file_put_contents("test/source/omg/olo.log", $contents) === strlen($contents));

echo "Writing a little bit after a small sleep (3 < file_rotate_interval = 5)\n";
mysleep(3);

$contents1 = "test1\n";
assertOrDie(file_put_contents("test/source/omg/olo1.log", $contents1) === strlen($contents1));

echo "Waiting for events to be received and target file to be rotated\n";
mysleep(4);

$contents2 = "test2\n";
assertOrDie(file_put_contents("test/source/omg/olo2.log", $contents2) === strlen($contents2));

mysleep(20);

assertOrDie($files = glob("test/target/omg/*"));

$total_bytes = 0;
foreach ($files as $f) {
    if (is_link($f)) {
        continue;
    }

    $total_bytes += filesize($f);
}

assertOrDie(count($files) === 4); // 0001, 0002, 0003 and current
assertOrDie(strlen($contents) + strlen($contents1) + strlen($contents2) === $total_bytes);
assertOrDie(filesize("test/target/omg/omg_current") === 0);

$success = true;
