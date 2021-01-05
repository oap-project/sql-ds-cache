PROJECT_ROOT = /your/oap/project/path

mkdir tmp
cd tmp
TMP_DIR=`pwd`
#install cmake

#build cpp

#build and install lib arrow
cd $TMP_DIR
git clone https://github.com/Intel-bigdata/arrow.git
cd arrow && git checkout branch-0.17.0-oap-1.0
mkdir -p cpp/release-build
cd cpp/release-build
ARROW_INSTALL_DIR=/usr
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_JNI=ON -DARROW_FILESYSTEM=ON -DARROW_ORC=ON -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
make -j
sudo make install

#build and install libjson
cd $TMP_DIR
git clone https://github.com/nlohmann/json
cd json
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr
make -j
sudo make install

# install libhdfs3.so
# it's easier to use conda to install.
# If you don't want to use conda, you can build it manually, it's much more complex.

#install manually
# you need install these libs in all nodes in your cluster.
cd $TMP_DIR
yum install -y libxml2-devel libgsasl-devel libuuid-devel krb5-devel # maybe need boost-devel, protobuf-devel
git clone https://github.com/erikmuttersbach/libhdfs3
cd libhdfs3
mkdir build
cd build
../bootstrap
make -j
ll src/libhdfs3.so.2.2.30

#install via conda
#conda install -c conda-forge libhdfs3
#ls -l ~/miniconda/envs/$(YOUR_ENV_NAME)/lib/libhdfs3.so/lib/libhdfs3.so

# replace lib in all nodes in cluster.
rm $HADOOP_HOME/lib/native/libhdfs.so
ln -f -s $TMP_DIR/libhdfs3/build/src/libhdfs3.so.2.2.30 $HADOOP_HOME/lib/native/libhdfs.so

#build native
cd $PROJECT_ROOT/oap-ape/ape-native
mkdir build
cd build
cmake ..
make -j
sudo cp lib/libparquet_jni.so /usr/lib

#build java

# install oap-common
cd $PROJECT_ROOT
mvn clean install -pl oap-common -am -fn

# build ape-java
cd $PROJECT_ROOT/oap-ape/ape-java
mvn clean package -am -fn

#copy $PROJECT_ROOT/oap-ape/ape-java/ape-common/target/ape-common-0.0.1-SNAPSHOT-jar-with-dependencies.jar
# and $PROJECT_ROOT/oap-ape/ape-java/ape-spark/target/ape-spark-0.0.1-SNAPSHOT.jar.
# Add them to spark-defaults.conf
# And remember to. Suggest to add to your ~/.bashrc profile, and restart hadoop.
export LIBHDFS3_CONF=$HADOOP_HOME/etc/hadoop/hdfs-site.xml

