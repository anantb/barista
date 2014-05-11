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


# setup if the databases get deleted again

# for x = 1 to 5 do the following

# sudo mkdir /var/lib/postgresql/9.1_x
# sudo mkdir /var/lib/postgresql/9.1_x/main

# sudo chown postgres.postgres /var/lib/postgresql/9.1_x
# sudo chown postgres.postgres /var/lib/postgresql/9.1_x/main

# su - postgres

# /var/lib/postgresql/9.1/bin/pg_ctl initdb -D /var/lib/postgresql/9.1_x/main -U postgres

# /var/lib/postgresql/9.1/bin/pg_ctl start -D /var/lib/postgresql/9.1_x/main -U postgres -o "-p (5433 + x)"

