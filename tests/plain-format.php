<?php
$no_chunk_output = true;

require('init.inc');

$msg = "test\n";
file_put_contents("test/source/plain.log", $msg, FILE_APPEND);
mysleep(1);

assertOrDie(file_get_contents("test/target/plain.log") === $msg);

echo "Pizding file from daemon\n";
rename("test/target/plain.log", "test/target/plain.log.old");
mysleep(2);

echo "Writing new event\n";
$msg2 = "test2\n";
file_put_contents("test/source/plain.log", $msg2, FILE_APPEND);
mysleep(1);

assertOrDie(file_get_contents("test/target/plain.log") === $msg2);

$success = true;
