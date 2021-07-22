#!/bin/bash

REDIS_VERSION=5.0

INSTALL_PREFIX=$HOME/.local
DIR_PREFIX=$HOME/.local/opt

# install tcl > 8.5 for running tests
sudo apt install -y tcl

mkdir -p ${DIR_PREFIX}
git clone -b ${REDIS_VERSION} https://github.com/redis/redis.git ${DIR_PREFIX}/redis
pushd ${DIR_PREFIX}/redis
make
# uncomment the line below to optionally run tests
# make test
sudo make install
sudo ./utils/install_server.sh
popd
