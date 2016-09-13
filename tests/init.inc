<?php
$opts = getopt('dr');

if (isset($opts['d'])) {
    $level = 'DEBUG';
} else {
    echo "Hint: launch with '-d' flag to see debug from daemon\n\n";
    $level = 'NOTICE';
}

$build_flags = '';

if (isset($opts['r'])) {
    $build_flags .= ' -race';
} else {
    echo "Hint: launch with '-r' flag to build with race\n\n";
}

$success = false;
register_shutdown_function(
    function() use (&$success) {
        if ($success) {
            echo "\n\033[1;32mAll tests passed, all is ok\033[m\n";
        } else {
            fwrite(STDERR, "\n\033[1;31mTest failed, see errors above\033[m\n");
        }
    }
);

function systemOrDie($cmd)
{
    echo "$cmd\n";
    system($cmd, $retval);
    if ($retval) {
        fwrite(STDERR, "Command failed with code $retval\n");
        exit($retval);
    }
}

function mysleep($duration)
{
    echo "slp $duration\r";
    sleep($duration);
}

function assertOrDie($expr, $descr = '')
{
    $backtace = debug_backtrace();
    $file = $backtace[0]['file'];
    $lines = file($file, FILE_IGNORE_NEW_LINES);
    $line = $backtace[0]['line'];
    $code = $lines[$line - 1];
    if (preg_match('/assertOrDie\\((.*?)(\\s*\\,\\s*"[^"]+")?\\)\\;$/s', $code, $matches)) {
        $code = $matches[1];
    }

    if ($descr) {
        $descr .= "\033[m - $code";
    } else {
        $descr = $code;
    }

    if (!$expr) {
        fwrite(STDERR, "\033[1;31mAssertion failed: $descr\033[m at $file:$line\n");
        exit(1);
    } else {
        echo "\033[1;32mAssertion passed: $descr\033[m\n";
    }
}

assertOrDie(chdir(dirname(__DIR__)));

systemOrDie("rm -rf test");
systemOrDie("mkdir -p test test/source test/target test/source/omg");

if (!empty($have_existing)) {
    systemOrDie("echo 'Pre-existing line' >> test/source/omg/_pre-existing.log");
}

$go_full_path = `which go`;

if (!$go_full_path) {
    echo "Could not find 'go', launching just ./lsd-test\n";
} else {
    systemOrDie("go build $build_flags -o lsd-test");
}

file_put_contents(
    "test/test.conf",
    '{
    "daemon_config": {
        "listen": [
            { "proto": "lsd-gpb",                  "address": "0.0.0.0:7357" },
            { "proto": "lsd-gpb/json",             "address": "0.0.0.0:7358" },
            { "proto": "service-stats-gpb",        "address": "0.0.0.0:7359" },
            { "proto": "service-stats-gpb/json",   "address": "0.0.0.0:7360" },
        ],
        "max_cpus": 0,
        "http_pprof_addr": "0.0.0.0:7361",
        "pid_file": "test/test.pid",
        "log_file": "-",
        "log_level": ' . json_encode($level) . ',
        "service_name": "lsd",
        "service_instance_name": ' . json_encode(gethostname()) . ',
        "syslog_identity": "${daemon}-${service_instance_name}-${version}",
    },
	' . (empty($no_server) ? '"server_config": {
	        ' . (!empty($no_chunk_output) ? '"chunk_output": false,' : "") . '
	        "target_dir": ' . json_encode(getcwd() . '/test/target') . ',
    },' : '') . '
    "client_config": {
        "source_dir": ' . json_encode(getcwd() . '/test/source') . ',
        "offsets_db": ' . json_encode(getcwd() . '/test/offsets.db') . ',
        "usage_check_interval": 1, /* check every second so that something really get deleted */
        "routing": [
            {
                "receivers": [
                    {"addr": "localhost:7357", "weight": 1},
                ],
                ' . (!empty($do_sharding) ? '"prefix_sharding": true,' : "") . '
                ' . (!empty($cut_prefix) ? '"cut_prefix": true,' : "") . '
            },
        ],
    }
}'
);

function launchDaemon()
{
    systemOrDie("sh -c 'if ! ./lsd-test -c test/test.conf; then kill -TERM " . getmypid() . "; fi' >/dev/tty 2>/dev/tty </dev/null &");
    register_shutdown_function(
        function() {
            echo "killall lsd-test\n";
            system("killall lsd-test");
        }
    );
    mysleep(1);
}

if (empty($nolaunch)) launchDaemon();
