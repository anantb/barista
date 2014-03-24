#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/.."; pwd)

rm -rf $abspath/sec/gen-go
mkdir -p $abspath/src/gen-go/pkg
thrift --gen go -out $abspath/src/gen-go/src $basepath/src/barista.thrift
GOPATH=$basepath:$abspath/gen-go
export GOPATH

go run $abspath/main/server.go $abspath/main/handler.go