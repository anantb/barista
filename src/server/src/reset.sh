#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
rm -rf $abspath/sqlpaxos_log.txt
python reset.py