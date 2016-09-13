<?php
$no_chunk_output = true;

require('init.inc');

function fileIsUsed($pid, $file) {
    $files = [];
    $cmd = "lsof -p " . intval($pid);
    echo "$cmd\n";
    exec($cmd, $files, $retval);

    assertOrDie($retval == 0);

    foreach ($files as $ln) {
        if (strpos($ln, $file) !== false) {
            var_dump($ln);
            return true;
        }
    }

    return false;
}

function fileIsPresentInOffsets($inode) {
    $offsets = json_decode(file_get_contents('test/offsets.db'), true);
    foreach ($offsets as $row) {
        if ($row['Ino'] == $inode) {
            return true;
        }
    }

    return false;
}

$msg = "ololo\n";
$srcfile = 'test/source/deleted.log';
file_put_contents($srcfile, $msg);
assertOrDie($inode = fileinode($srcfile));

echo "Waiting for file to be streamed";
mysleep(1);

assertOrDie(file_get_contents('test/target/deleted.log') == $msg);

echo "Pizding file from daemon\n";
unlink($srcfile);

$pid = explode(" ", trim(shell_exec('ps ax | grep lsd-test | grep -v grep | grep -v kill')))[0];

assertOrDie(fileIsUsed($pid, $srcfile));
assertOrDie(fileIsPresentInOffsets($inode));

mysleep(62); // file usage is checked every 60 sec and it is not configurable

assertOrDie(!fileIsUsed($pid, $srcfile));
assertOrDie(!fileIsPresentInOffsets($inode));

// check that you can create the same file again and will be running again
file_put_contents($srcfile, $msg);

mysleep(1);

assertOrDie(file_get_contents('test/target/deleted.log') == $msg . $msg);

$success = true;
