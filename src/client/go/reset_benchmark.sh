#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/../../.."; pwd)

cd /project/barista/src/server/src
sh reset.sh 5432
sh run.sh
sudo su postgres

cd
export PATH=$PATH:/usr/lib/postgresql/9.1/bin
DBT2PORT=5432; export DBT2PORT
DBT2DBNAME=postgres; export DBT2DBNAME
DBT2PGDATA=${HOME}/local/dbt2/pgdata; export DBT2PGDATA
DBT2TSDIR=${HOME}/local/dbt2; export DBT2TSDIR
USE_PGPOOL=0; export USE_PGPOOL

dbt2-pgsql-build-db -w 10