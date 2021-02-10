# OAP Developer Guide

This document contains the instructions & scripts on installing necessary dependencies and building OAP. 
You can get more detailed information from OAP each module below.

* [SQL DS Cache](https://oap-project.github.io/sql-ds-cache/)
* [PMem Common](https://oap-project.github.io/pmem-common)
* [PMem Spill](https://oap-project.github.io/pmem-spill)
* [PMem Shuffle](https://oap-project.github.io/pmem-shuffle)
* [Remote Shuffle](https://oap-project.github.io/remote-shuffle)
* [OAP MLlib](https://oap-project.github.io/oap-mllib)
* [Arrow Data Source](https://oap-project.github.io/arrow-data-source)
* [Native SQL Engine](https://oap-project.github.io/native-sql-engine)

## Building OAP

### Prerequisites for Building

OAP is built with [Apache Maven](http://maven.apache.org/) and Oracle Java 8, and mainly required tools to install on your cluster are listed below.

- [Cmake](https://help.directadmin.com/item.php?id=494)
- [GCC > 7](https://gcc.gnu.org/wiki/InstallingGCC)
- [Memkind](https://github.com/memkind/memkind/tree/v1.10.1)
- [Vmemcache](https://github.com/pmem/vmemcache)
- [HPNL](https://github.com/Intel-bigdata/HPNL)
- [PMDK](https://github.com/pmem/pmdk)  
- [OneAPI](https://software.intel.com/content/www/us/en/develop/tools/oneapi.html)
- [Arrow](https://github.com/Intel-bigdata/arrow)

- **Requirements for PMem Shuffle**

If enable PMem Shuffle with RDMA, you can refer to [PMem Shuffle](https://oap-project.github.io/pmem-shuffle) to configure and validate RDMA in advance.

We provide scripts below to help automatically install dependencies above **except RDMA**, need change to **root** account, run:

```
# git clone -b <version> https://github.com/oap-project/oap-tools.git
# cd oap-tools
# sh dev/install-compile-time-dependencies.sh
```

Run the following command to learn more.

```
# sh dev/scripts/prepare_oap_env.sh --help
```

Run the following command to automatically install specific dependency such as Maven.

```
# sh dev/scripts/prepare_oap_env.sh --prepare_maven
```


### Building

To build OAP package, run command below then you can find a tarball named `oap-$VERSION-bin-spark-$VERSION.tar.gz` under directory `$OAP_TOOLS_HOME/dev/release-package `.
```
$ sh $OAP_TOOLS_HOME/dev/compile-oap.sh
```

Building Specified OAP Module, such as `sql-ds-cache`, run:
```
$ sh $OAP_TOOLS_HOME/dev/compile-oap.sh --sql-ds-cache
```
