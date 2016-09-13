<?php

$no_server = true;

require('init.inc');

assertOrDie(file_put_contents("test/source/omg/test.log", "Some test line\n", FILE_APPEND));

echo "Waiting a second for event to be delivered to non-existent server\n";
mysleep(2);

assertOrDie($st = stat("test/source/omg/test.log"));
$test_ino = $st['ino'];

assertOrDie($offsets_db = file_get_contents("test/offsets.db"));
$offsets = json_decode($offsets_db, true);
assertOrDie(is_array($offsets) && count($offsets) <= 1);

foreach ($offsets as $row) {
    assertOrDie(isset($row['Off']) && isset($row['Ino']));
    assertOrDie($row['Ino'] == $test_ino);
    assertOrDie($row['Off'] == 0); // no targets exist, so file cannot be delivered
}

$success = true;
