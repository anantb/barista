#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/../../.."; pwd)

rm -rf $abspath/gen-go
mkdir -p $abspath/gen-go/src
thrift --gen go -out $abspath/gen-go/src $basepath/src/barista.thrift
GOPATH=$basepath/src/server:$abspath/gen-go
export GOPATH

rm -rf $abspath/nohup.out
rm -rf $abspath/sqlpaxos_log.txt

process_line=`sudo lsof -i :9000 | tail -1`
if [ "$process_line" != "" ]; then
    process_name=`echo "$process_line" | awk '{print $1}'`
    echo "killing $process_name"
    sudo kill `echo "$process_line" | awk '{print $2}'`
fi

(nohup go run $abspath/main/server.go $abspath/main/handler.go &) &