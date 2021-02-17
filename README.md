# bitraft

![](https://github.com/prologic/bitraft/workflows/Coverage/badge.svg)
![](https://github.com/prologic/bitraft/workflows/Docker/badge.svg)
![](https://github.com/prologic/bitraft/workflows/Go/badge.svg)
![](https://github.com/prologic/bitraft/workflows/ReviewDog/badge.svg)

[![Go Report Card](https://goreportcard.com/badge/prologic/bitraft)](https://goreportcard.com/report/prologic/bitraft)
[![GitHub license](https://img.shields.io/github/license/prologic/bitraft.svg)](https://github.com/prologic/bitraft)
[![Github all releases](https://img.shields.io/github/downloads/prologic/bitraft/total.svg)](https://github.com/prologic/bitraft/releases)

A [Bitcask](https://github.com/prologic/bitcask) Distributed Key/Value store
using [Raft](https://github.com/hashicorp/raft) for concensus with a
[Redis](https://redis.org) compatible API written in [Go](https://golang.org).

Based off of [kvnode](https://github.com/tidwall/kvnode).
(See [LICENSE.old](/LICENSE.old))

- Redis compatible API
- Bitcask disk-based storage
- Raft support with [Finn](https://github.com/tidwall/finn) commands
- Compatible with existing Redis clients

## Usage

```#!bash
$ go get github.com/prologic/bitraft
$ bitraft
```

## Docker

You can also use the [Bitraft Docker Image](https://cloud.docker.com/u/prologic/repository/docker/prologic/bitraft):

```#!bash
$ docker pull prologic/bitraft
$ docker run -d -p 4920:4920 prologic/bitraft
```

## Example session

Commands:

```
SET key value
GET key
DEL key [key ...]
KEYS pattern
FLUSHDB
SHUTDOWN
```

## Backup and Restore

To backup data:
```
RAFTSNAPSHOT
```
This will creates a new snapshot in the `data/snapshots` directory.
Each snapshot contains two files, `meta.json` and `state.bin`.
The state file is the database in a compressed format. 
The meta file is details about the state including the term, index, crc, and size.

Ideally you call `RAFTSNAPSHOT` and then store the state.bin on some other server like S3.

To restore:
- Create a new raft cluster
- Download the state.bin snapshot
- Pipe the commands using the `bitraft --parse-snapshot` and `redis-cli --pipe` commands

Example:
```
bitraft --parse-snapshot state.bin | redis-cli -h 10.0.1.5 -p 4920 --pipe
```

This will execute all of the `state.bin` commands on the leader at `10.0.1.5:4920`


For information on the `redis-cli --pipe` command see [Redis Mass Insert](https://redis.io/topics/mass-insert).

## License

bitraft source code is available under the MIT [License](/LICENSE).

Previously based off of [kvnode](https://github.com/tidwall/kvnode).
(See [LICENSE.old](/LICENSE.old))
