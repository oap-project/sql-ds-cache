cd /tmp
git clone https://github.com/oap-project/pmem-common.git
cd pmem-common/
git checkout branch-1.1-spark-3.x
mvn install -am -q -DskipTests