#!/bin/bash -eu

# Build direct broker dependency - cJSON
# Note that other dependencies, i.e. sqlite are not yet built because they are
# only used by plugins and not currently otherwise used.
cd ${SRC}/cJSON
cmake -DBUILD_SHARED_LIBS=OFF -DENABLE_CJSON_TEST=OFF -DCMAKE_C_FLAGS=-fPIC .
make
make install

# Build broker and library static libraries
cd ${SRC}/mosquitto
make WITH_STATIC_LIBRARIES=yes WITH_DOCS=no WITH_FUZZING=yes
