#!/bin/bash

source scl_source enable devtoolset-8
gcc -v

# cmake 
cd /tmp
wget https://github.com/Kitware/CMake/releases/download/v3.20.6/cmake-3.20.6-linux-x86_64.tar.gz
tar -zxvf cmake-3.20.6-linux-x86_64.tar.gz
cd /tmp/cmake-3.20.6-linux-x86_64/bin
CMAKE=/tmp/cmake-3.20.6-linux-x86_64/bin/cmake 

INSTALL_DIR=/usr

# install json
cd /tmp
git clone https://github.com/nlohmann/json
cd json
mkdir build
cd build
$CMAKE .. -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR
make -j
sudo make install

# install hi-redis, redis-plus-plus
cd /tmp
git clone https://github.com/redis/hiredis.git
cd hiredis
sudo make USE_SSL=1 PREFIX=$INSTALL_DIR install -j

cd /tmp
git clone https://github.com/sewenew/redis-plus-plus.git
cd redis-plus-plus
mkdir compile
cd compile
$CMAKE -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DREDIS_PLUS_PLUS_CXX_STANDARD=17 ..
make -j
sudo make install

# install arrow
cd /tmp
git clone https://github.com/oap-project/arrow.git
cd arrow
BRANCH=ape-dev
git checkout $BRANCH

cd cpp
mkdir build
cd build
$CMAKE -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_FILESYSTEM=ON -DARROW_PLASMA=ON \
      -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_ORC=ON -DProtobuf_SOURCE=BUNDLED -DARROW_JNI=ON -DARROW_WITH_BZ2=ON  \
      -DARROW_WITH_ZLIB=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_BROTLI=ON  ..
make -j
sudo make install


# install ape
cd /tmp
git clone https://github.com/oap-project/sql-ds-cache
cd sql-ds-cache
git checkout br-for-Tianchi-spark-3.1.2
cd oap-ape/ape-native
mkdir build
cd build
$CMAKE ..
make -j

ldd ./lib/libparquet_jni.so

sudo ldconfig
