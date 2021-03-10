sudo apt-get install -y -q --no-install-recommends \
        autoconf \
        ca-certificates \
        ccache \
        g++ \
        gcc \
        gdb \
        git \
        libbenchmark-dev \
        libboost-filesystem-dev \
        libboost-regex-dev \
        libboost-system-dev \
        libbrotli-dev \
        libbz2-dev \
        libcurl4-openssl-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        liblz4-dev \
        libprotobuf-dev \
        libprotoc-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        libutf8proc-dev \
        libzstd-dev \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        rapidjson-dev \
        tzdata && \
        sudo apt-get clean && \
        sudo rm -rf /var/lib/apt/lists*

cd /tmp
git clone https://github.com/oap-project/arrow.git
cd arrow
BRANCH=ape-dev
git checkout $BRANCH

cd cpp
mkdir build
cd build
ARROW_INSTALL_DIR=/usr
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_FILESYSTEM=ON -DARROW_PLASMA=ON -DARROW_DEPENDENCY_SOURCE=AUTO ..
make -j2
sudo make install
