#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/../../.."; pwd)

rm -rf $abspath/gen-go
mkdir -p $abspath/gen-go/src
thrift --gen go -out $abspath/gen-go/src $basepath/src/barista.thrift
GOPATH=$abspath/gen-go:$basepath/src/server
export GOPATH

go test -test.run TestBasic
go test -test.run TestPartition
go test -test.run TestUnreliable
go test -test.run TestHole
go test -test.run TestManyPartition
