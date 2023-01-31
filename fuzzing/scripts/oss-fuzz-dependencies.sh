#!/bin/bash -eu

# Note that sqlite3 is required as a build dep of a plugin which is not
# currently part of fuzz testing. Once it is part of fuzz testing, sqlite will
# need to be built statically.
apt-get update && apt-get install -y libtool-bin make libsqlite3-dev
git clone https://github.com/DaveGamble/cJSON ${SRC}/cJSON
