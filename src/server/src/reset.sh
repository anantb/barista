#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
rm -rf /tmp/sqlpaxos
python reset.py