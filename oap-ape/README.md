# How to build

java build
```
cd $OAP_ROOT_DIR/oap-ape/ape-java
mvn clean package -am
```

cpp build
```
cd $OAP_ROOT_DIR/oap-ape/ape-native/
mdkir build
cd build
cmake ..
make
```