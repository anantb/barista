#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/../../.."; pwd)

make clean
rm -rf $abspath/bin
rm -rf $abspath/gen-cpp

thrift --gen cpp -o $abspath $basepath/src/barista.thrift

make
