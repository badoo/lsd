<?php
$do_sharding = true;

require('init.inc');

assertOrDie(file_put_contents("test/source/omg/test.log", "123:Some test line\n", FILE_APPEND));

mysleep(1);

assertOrDie($lines = file("test/target/omg/omg_current", FILE_SKIP_EMPTY_LINES | FILE_IGNORE_NEW_LINES));
assertOrDie($lines === ["123:Some test line"]);

$success = true;
