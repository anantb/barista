#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/../../.."; pwd)

dbt2-run-workload -a pgsql -d 300 -w 10 -o /tmp/result -c 1 -D postgres