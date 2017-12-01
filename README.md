# LSD
LSD is a streaming daemon that has been developed as a replacement for facebook's [scribe](https://github.com/facebookarchive/scribe) (it has been abandoned)

# Features
 - extremely high availability, reliability and local buffering capabilities, limited only by producer's disk storage capacity
 - lightweight text-based data format
 - simple configuration, deployment and support
 - ability to create complex routing schemes for different data streams
 - in general you don't need any specific library to send events (just write to plain files)

# How to install
_ensure you have Linux or Mac OS operating system (Windows is not supported)_
```
go install github.com/badoo/lsd
$GOPATH/bin/lsd -c path_to_config
```

# Architecture

![Client-Server architecture](https://badoo.github.com/lsd/assets/lsd_architecture.jpg)

Producer application writes events (\n separated text lines of any format) into local files.

There is a local agent, called "LSD client" running on each host.

It watches these files and sends all new lines to remote receiver, called "LSD Server".

All events are split into categories based on file/directory name

## LSD Client

LSD Client uses inotify to watch for categories's files.

It also has rotate/delete mechanics to prevent superlarge/old files to appear.

### File name formats and write contract

```
lsd_dir/category_name.log
lsd_dir/category_name/*
```

All messages (lines) are considered to be less than PIPE_BUF (4k in Linux by default).

It provides a guarantee of atomic writes to prevent lines split and data corruption.

If you want to write lines larger than PIPE_BUF, you should use specific file names and flock(LOCK_EX) for writing.

Maximum size for single line is 512k 

```
lsd_dir/category_name/*_big
lsd_dir/category_name_big.log
lsd_dir/category_name_big.log
```

_You should also escape line breaks if they do appear inside you data_

### Rotate/delete flow

To keep workdir clean and small, LSD has to periodically delete data that has already been sent and delivered.

![Client rotate flow](https://badoo.github.io/lsd/assets/lsd_rotate.gif)

It does rotate on maxFileSize or periodically on fileRotateInterval, specified in config

Rotate algorithm:
1) move `lsd_dir/somecategory.log` => `lsd_dir/somecategory.log.old`
2) stream old and new files in parallel until every writer closes .old one
3) when nobody holds .old file (writers always open only .log) and it is fully streamed, LSD can safely delete .old and go to step 1.

_Keep in mind that writers have to periodically reopen current file in order to make LSD Client available to pass step 2_

### Best way to write to LSD

The most effective and reliable way is to write to small files, depending on current time
```
lsd_dir/category_name/year|month|day|hour|minute
```
for example
```
lsd_dir/category_name/201711031659
lsd_dir/category_name/201711031700
lsd_dir/category_name/201711031701
lsd_dir/category_name/201711031702
lsd_dir/category_name/201711031703
...
```

We have some client libraries that implement this logic:
- PHP
- Go 
- Java

(will be published soon)

_But you are free to just do open() => write_line() => close() and it will work fine_

## LSD Server

LSD Server accepts data from multiple LSD Clients and writes it to disk in separate chunks according to size/time threshold

![Server write flow](https://badoo.github.com/lsd/assets/lsd_server.gif)

```
lsd_server_dir/category_name/category_name-year-month-day_6 digits number (incrementing)
```

There is also a symlink that points to currently active file (which LSD Server writes to):
```
lsd_server_dir/category_name/category_name_current
```

Example:
```
lsd_server_dir/category_name/category_name-2016-12-01_000008
lsd_server_dir/category_name/category_name-2016-12-01_000009
lsd_server_dir/category_name/category_name-2016-12-01_000010
lsd_server_dir/category_name/category_name-2016-12-01_000011
lsd_server_dir/category_name/category_name_current => lsd_server_dir/category_name/category_name-2016-12-01_000011
```

When threshold comes and `_000012` file is created, LSD atomically switches symlink to it. 

In this case consumer can process all files younger than one that is pointed by  `_current` symlink
(because LSD is writing to it or has already created next one, but didn't switch symlink yet).

We have some consumer libraries (will be published soon):
- PHP
- Go 
- Java

# Configuration
Configuration is taken from `conf/lsd.conf` by default, but you can specify custom config with `-c <path>`

## Basic daemon config
Listen / logging / debug service parameters - self describing
```
"daemon_config": {
    "listen": [
        { "proto": "lsd-gpb",                  "address": "0.0.0.0:3701" },
        { "proto": "lsd-gpb/json",             "address": "0.0.0.0:3702" },
        { "proto": "service-stats-gpb",        "address": "0.0.0.0:3703" },
        { "proto": "service-stats-gpb/json",   "address": "0.0.0.0:3704" },
    ],
    "max_cpus": 0,
    "http_pprof_addr": "0.0.0.0:3705",
    "pid_file": "/tmp/lsd.pid",
    "log_file": "-",
    "log_level": "NOTICE",
    "service_name": "lsd",
    "service_instance_name": "<hostname>",
},
```

## Client config

Proto
```
message client_config_t {

    message receiver_t {
        required string addr = 1;
        required uint64 weight = 2;
        optional uint64 connect_timeout = 3 [default = 1];
        optional uint64 request_timeout = 4 [default = 30];
    };

    message routing_config_t {
        repeated string categories = 1;                               // category masks list (only "*" is supported as mask)
        repeated receiver_t receivers = 2;
        optional uint64 out_buffer_size_multiplier = 3 [default = 1]; // we can modify basic buffer size
        optional bool gzip = 4 [default = false];                     // gzip data sent over the network
    };

    required string source_dir           = 1;
    required string offsets_db           = 2;                     // path to file, storing per-file streamed offsets
    optional uint64 max_file_size        = 3 [default = 1000000]; // max file size for plain files before rotation
    optional uint64 file_rotate_interval = 4 [default = 120];     // interval of force file rotation (for long living ones)
    optional bool always_flock           = 5 [default = false];   // always flock files, used only for restreaming
    repeated routing_config_t routing    = 6;

    optional uint64 file_events_buffer_size = 7    [default = 500];    // buffer size for file events to each category (can be raised to smooth spikes, but will consume more memory)
    optional uint64 out_buffer_size = 8            [default = 50];     // buffer size to send to network (can be raised to smooth spikes, but will consume more memory)

    optional uint64 usage_check_interval = 9           [default = 60];    // file usage check in seconds (after rotate, before clean)
    optional uint64 offsets_save_interval = 10         [default = 1];     // save offsets db to disk in seconds
    optional uint64 traffic_stats_recalc_interval = 11 [default = 10];    // update traffic stats in seconds
    optional uint64 backlog_flush_interval = 12        [default = 10];    // flush of postponed events (due to overload) in seconds
    optional uint64 periodic_scan_interval = 13        [default = 600];   // full rescan to pick up orphan events and force consistency in seconds
};
```
Example

```
"client_config": {
    "source_dir": "/local/lsd-storage",
    "offsets_db": "/var/lsd/lsd-offsets.db",
    "max_file_size": 1000000,
    "usage_check_interval": 60,
    "routing": [
        {
          "receivers": [
            {"addr": "streamdefault.local:3706", "weight": 1},
          ],
        },
        {
          "categories": ["error_log"],
          "receivers": [
            {"addr": "logs1.local:3706", "weight": 1},
          ],
        },
        {
          "categories": ["some_event*"],
          "receivers": [
            {"addr": "otherhost.domain:3706", "weight": 1},
          ],
        },
    ],
}
```

## Server config

Proto
```
message server_config_t {

    message server_settings_t {
        repeated string categories = 1;                 // category masks list (only "*" is supported as mask)
        optional uint64 max_file_size = 2;              // max output file size in bytes
        optional uint64 file_rotate_interval = 3;       // output file rotate interval in seconds
        optional bool gzip = 4 [default = false];       // gzip data that is written to disk
        optional uint64 gzip_parallel = 5 [default = 1];// use this only if you are 100% sure that you need parallel gzip
    };
    // default settings that are redefined by more concrete ones (above)
    required string target_dir = 1;
    optional uint64 max_file_size = 2 [default = 5000000];           // max output file size in bytes
    optional uint64 file_rotate_interval = 3 [default = 5];          // output file rotate interval in seconds
    repeated server_settings_t per_category_settings = 4;            // per-category settings (max_file_size and file_rotate_interval)
    optional uint64 error_sleep_interval = 5 [default = 60];
    optional uint64 traffic_stats_recalc_interval = 6 [default = 10];// update traffic stats in seconds
};
```
Example
```
"server_config": {
    "target_dir": "/local/lsd-storage-server/",
    "max_file_size": 20000000,
    "file_rotate_interval": 60,
    "per_category_settings": [
        {
            "categories": ["error_log"],
            "max_file_size": 1000000,
            "file_rotate_interval": 10,
        },
        {
            "categories": ["some_specific_category*"],
            "max_file_size": 360000000,
            "file_rotate_interval": 2
        },
    ],
}
```

## Relay mode (probably you won't need it)

![Relay example](https://badoo.github.com/lsd/assets/lsd_relay.jpg)

If you have multiple datacenters or too much producer hosts, you may need some multiplexers (called relays)

In this mode LSD works as both client and server in single daemon

Relay is a "fan in" in source DC for all category's data

Router is a "fan out" in target DC (sends to appropriate local servers)

`always_flock` is required in this case
```
"server_config": {
    "target_dir": "/local/lsd-storage-relay/",
    "max_file_size": 100000000,
    "file_rotate_interval": 60,
},
"client_config": {
    "source_dir": "/local/lsd-storage-relay/",
    "offsets_db": "/var/lsd/lsd-offsets-relay.db",
    "max_file_size": 100000000,
    "usage_check_interval": 60,
    "always_flock": true,
    "routing": [{
        "receivers": [                  
            {"addr": "lsd-router1.d4:3716", "weight": 1},
            {"addr": "lsd-router1.d4:3716", "weight": 1},
            {"addr": "lsd-router1.d4:3716", "weight": 1},
            {"addr": "lsd-router1.d4:3716", "weight": 1},
            {"addr": "lsd-router1.d4:3716", "weight": 1},
            {"addr": "lsd-router1.d4:3716", "weight": 1},
            {"addr": "lsd-router1.d4:3716", "weight": 1},
            {"addr": "lsd-router1.d4:3716", "weight": 1},
        ],
    }],
},
``` 

# Exploitation

## Resource usage
Both client and server have low rusage (it grows in case of gzip: true), but consume memory, proportional to number of categories streamed.
You can reduce absolute memory usage by tuning buffer sizes in config, but with default settings in badoo's usecase it's average memory usage is less then 1gb RSS

## Healthchecks

You can run subcommand healthcheck to receive current LSD status
```
lsd healthcheck -c path_to_config
```
It returns current status of lsd daemon for given config.

Example:
```
lsd_experiment healthcheck -c /local/lsd/etc/lsd.conf | jq '.'
{
  "is_running": true,
  "client": {
    "undelivered": {
      "debug_mgalanin_cluster_priority": 0,
      "debug_mgalanin_stop_word_complaint": 0,
      "debug_movchinnikov_sanction_descent": 0,
      "error_log": 0,
      "fast_stats": 0,
      "local_compressed_uds_json_2": 7668,
      "local_uds_debug_gpb": 269,
      "local_uds_gpb_hdfs_1": 0,
      "local_uds_gpb_instant": 0,
      "mdk": 0,
      "spam_clustering_user_features": 0,
      "spam_clustering_user_features_6h": 0,
      "spam_generic": 118,
      "spam_messages": 258,
      "stat_events": 0
    },
    "backlog_size": 1,
    "errors": []
  },
  "server": {
    "categories": {
        "spam_stopword_text": {
            "count": 1,
            "oldest_ts": 1499350267
        },
        "spam_text": {
            "count": 11,
            "oldest_ts": 1510421666
        },
        "spam_timeout": {
            "count": 26,
            "oldest_ts": 1510421660
        },
        "spam_vpn_train_colle": {
            "count": 0,
            "oldest_ts": 0
        }
    },
    "errors": []
  }
}
```
It returns two sections for client and server part of config (they can run in a single daemon)

`client` contains number of undelivered bytes for each category

`server` contains count of files for each category (waiting to be processed)

Both sections also have array of generic errors.

## HDFS transfer
You can transfer files directly from LSD server's workdir to HDFS with specific sub command. 
```
lsd transfer-hdfs -dst-dir=hdfs_dst_dir -tmp-dir=temp_dir_for_upload -namenode=username@host:port -delete=true|false "category name pattern"
```
Category pattern has standard "bash glob" syntax.

All files from LSD server dirs will be transferred to hdfs with current_hostname_ prefix (to make files unique)

## GZIP
You can compress LSD traffic in two ways:
1) between client and server
2) from server to FS (also supports `gzip_parallel`)
With `gzip: true` in appropriate config section

# Internals
_At most daemon's code is self documented_

## Client

![Client internal architecture](https://badoo.github.io/lsd/assets/lsd_client_internals.jpg)

### FS router
Listens inotify for base dir, adds watches on subdirs if they appear

Extracts category/stat for each event and passes categoryEvent to specific categoryListener that is responsible for category

If some category and it's listener get stuck (too many events/network issues/etc), we should not block whole fs router in order to keep other categories streamed

That's why we have "backlog" - table with filenames that are postponed. Backlog is flushed into main events queue periodically   

We also have background check, that periodically scans all files in order to pick-up orphan/lost and restore consistency (should not be useful often)

### Category listener
Receives events for specific category. Stores all active files and associated info inside.

Reads file contents from disk and writes to network balancer's input channel

Also provides rotate mechanics and deletes old rotated files (with usageChecker)  

### Usage checker
Periodically scans procfs to detect "free" files (those are no longer opened by writers)

We don't use lsof because it takes too much time/resources to execute lsof for each watched file

### Traffic manager
Stores and exposes out/in traffic stats with expvar

### Network router
Just a proxy that stores all category => out network channel mappings

### Balancer
One balancer per category group in config. Stores all upstreams and balances events traffic between them.

Registers all sent events and confirms all events, that are accepted by remote LSD server (updates offsetsDB).

### Upstream
Sends event batches to LSD server on specific host.

## Server
LSD server is pretty simple, it accepts event batches from network and writes them to disk. Nothing else.

GPBHandler => listener => writer => FS
