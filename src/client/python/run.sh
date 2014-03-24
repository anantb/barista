#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/../../.."; pwd)

rm -rf $abspath/gen-py
thrift --gen py -o $abspath $basepath/src/barista.thrift
PYTHONPATH=$abspath/gen-py
export PYTHONPATH

python client.py
