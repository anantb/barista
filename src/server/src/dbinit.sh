#!/bin/sh
abspath=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

basepath=$(cd "$abspath/../../.."; pwd)

# setup if the databases get deleted again

# for x = 1 to 5 do the following

sudo mkdir /var/lib/postgresql/9.1_x
sudo mkdir /var/lib/postgresql/9.1_x/main

sudo chown postgres.postgres /var/lib/postgresql/9.1_x
sudo chown postgres.postgres /var/lib/postgresql/9.1_x/main

su - postgres

/var/lib/postgresql/9.1/bin/pg_ctl initdb -D /var/lib/postgresql/9.1_x/main -U postgres

/var/lib/postgresql/9.1/bin/pg_ctl start -D /var/lib/postgresql/9.1_x/main -U postgres -o "-p (5433 + x)"
