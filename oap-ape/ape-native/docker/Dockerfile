FROM centos:7.6.1810

RUN yum install -y wget git vim gcc gcc-c++ numactl-devel libcurl-devel rpm-build zlib-devel sysstat pssh sudo openssh-server openssh-clients openssl-devel boost-devel\
    && yum groupinstall -y "Development Tools" "Development Libraries" \
    && yum install -y centos-release-scl \
    && yum install -y devtoolset-8-gcc devtoolset-8-gcc-c++ \
    && yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel.x86_64 \
    && source scl_source enable devtoolset-8 

RUN source scl_source enable devtoolset-8 && gcc -v

ENV JAVA_HOME "/usr/lib/jvm/java-1.8.0/"

ADD install.sh /install.sh
RUN /install.sh 
