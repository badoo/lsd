<?php
require('init.inc');

$long_string = json_encode($_ENV);

echo "Writing a lot to files...\n";
$indexes = [];

$total_len = 0;

$start_time = microtime(true);

for ($i = 0; $i < 100000; $i++) {
    $indexes[$i] = true;
    $total_len += file_put_contents("test/source/omg/" . ($i % 2 == 0 ? "a" : "") . "something_big.log", $long_string . "_$i\n", FILE_APPEND | LOCK_EX);
}

echo "Waiting for everything to be delivered...\n";
for ($i = 0; $i < 60; $i++) {
    $len = 0;
    foreach (glob("test/target/omg/*") as $f) {
        if (is_link($f)) continue;
        $len += filesize($f);
    }

    if ($len >= $total_len) {
        break;
    }

    mysleep(1);
}
echo "Delivered in " . ceil(microtime(true) - $start_time) . " sec\n";

assertOrDie(unlink("test/target/omg/omg_current"));
assertOrDie($pp = popen("cat test/target/omg/*", "r"));

echo "Checking end result...\n";

$success = true;

while (false !== ($ln = fgets($pp))) {
    if (strpos($ln, $long_string) !== 0) {
        fwrite(STDERR, "Invalid string in file: '$ln'\n");
        $success = false;
        continue;
    }

    $idx = substr(rtrim($ln), strlen($long_string));

    if ($idx[0] != '_') {
        fwrite(STDERR, "No _ in index after line: '$idx'\n");
        $success = false;
        continue;
    }

    $idx = substr($idx, 1);
    if (!isset($indexes[$idx])) {
        fwrite(STDERR, "Index $idx is not present in a source file or was delivered twice\n");
        $success = false;
        continue;
    }

    unset($indexes[$idx]);
}

if (count($indexes) > 0) {
    fwrite(STDERR, "Indexes " . implode(', ', array_keys($indexes)) . " were not delivered\n");
    $success = false;
}

pclose($pp);
