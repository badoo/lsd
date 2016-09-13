<?php
$have_existing = true;

require('init.inc');

// The following could theorically work in some implementations, but does not work for now:

//assertOrDie($forever_locked_fp = fopen("test/source/omg/forever_locked_big.log", "a"));
//assertOrDie(flock($forever_locked_fp, LOCK_EX));
//fwrite($forever_locked_fp, "Forever alone\n");
// do not unlock so that the file cannot be read and delivered by the system

$total_messages = ["Pre-existing line" => true];

// honeypot file, used to check if inode checker works, file must not be deleted because we hold this file open
$honeyPotFp = fopen("test/source/omg/_honeypot.log", "w");
fwrite($honeyPotFp, "Honeypot line\n");
$total_messages["Honeypot line"] = true;

for ($i = 0; $i < 3; $i++) {
    $msg = "Hello world $i";
    $total_messages[$msg] = true;
    echo "Writing '$msg' to omg.log\n";
    file_put_contents("test/source/omg.log", $msg . "\n", FILE_APPEND);
    mysleep(1);
}

$free_files = [];
$locked_files = [];

for (; $i < 6; $i++) {
    $msg = "Hello world $i";
    $total_messages[$msg] = true;
    $filename = "test/source/omg/something$i.log";

    echo "Writing '$msg' to $filename\n";
    file_put_contents($filename, $msg . "\n", FILE_APPEND);
    $free_files[] = $filename;
    mysleep(1);
}

for (; $i < 10; $i++) {
    $msg = "Hello world $i";

    $filename = "test/source/omg/something${i}_big.log";
    assertOrDie($fp = fopen($filename, "a"), "fopen($filename, 'a')");
    assertOrDie(flock($fp, LOCK_EX));

    if ($i == 9) {
        assertOrDie(symlink(basename($filename), $filename . "_current_link"), "Creating symlink for $filename to check that links are skipped");
    }

    $total_messages[$msg . "$fp"] = true;
    echo "Writing '$msg' to omg/something${i}_big.log using lock\n";

    fwrite($fp, $msg);
    fflush($fp);
    $locked_files[$filename] = $fp;
}

foreach (array_reverse($locked_files, true) as $filename => $fp) {
    echo "Writing the rest of file $filename\n";
    fwrite($fp, "$fp\n");
    flock($fp, LOCK_UN);
    mysleep(1);
}

assertOrDie(is_dir("test/target/omg"));
assertOrDie($files = glob("test/target/omg/*"));

$linksCount = 0;
$filesCount = 0;

foreach ($files as $f) {
    if (is_link($f)) {
        assertOrDie($f === "test/target/omg/omg_current");
        $linksCount++;
        continue;
    }

    $filesCount++;
    assertOrDie($lines = file($f, FILE_IGNORE_NEW_LINES), "Read lines from file $f");
    foreach ($lines as $ln) {
        assertOrDie(isset($total_messages[$ln]), "Checking presence of message $ln");
        unset($total_messages[$ln]);
    }
}

assertOrDie(count($total_messages) == 0, "Checking that all messages were delivered");

assertOrDie($linksCount == 1);
assertOrDie($filesCount == 2);

$max = 300;

if (PHP_OS == 'Linux') {
    $max = 2;
} else {
    echo "lsof is slow, so wait a little bit (max $max sec)\n";
}

for ($i = 0; $i < 300; $i++) {
    $exist = false;
    foreach ($free_files as $filename) {
        if (file_exists($filename) || file_exists($filename . ".old")) {
            $exist = true;
        }
    }

    if (!$exist) {
        break;
    }
    mysleep(1);
}

$should_exist_old = $locked_files;

foreach ($should_exist_old as $filename => $_);
array_pop($should_exist_old);
$should_exist_old["test/source/omg/_honeypot.log"] = $honeyPotFp;

assertOrDie(file_exists($filename), "Checking that last filename $filename is still present without being renamed to .old");

foreach ($should_exist_old as $filename => $_) {
    assertOrDie(file_exists($filename) || file_exists($filename . ".old"), "Checking that still open $filename is not deleted");
}

$free_files[] = "test/source/omg/_pre-existing.log";

foreach ($free_files as $filename) {
    assertOrDie(!file_exists($filename) && !file_exists($filename . ".old"), "Checking that unused $filename is deleted");
}

$success = true;
