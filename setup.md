# Test Setup

To test this _Go Driver_ we need the following:  
* Download `github.com/asifjalil/cli` source code on a server.
* Setup a sample Db2 database using docker.  
* Copy the `_TEST` directory from the source code to the Db2 Docker instance.  
* Download and setup Db2 ODBC/CLI driver where the source code is located.   
* Run tests from the source code.  

## Setup Db2 Sample Database using Docker

* Login to [Docker Hub](https://hub.docker.com) using docker id and password.  
* Locate [db2-developer-c](https://hub.docker.com/_/db2-developer-c-edition) Docker image.  
* Proceed to Checkout.  
* That will take us to the `Setup Instructions`.  
* Assuming we are running Docker on Linux and want the `x86_64` Docker image.  
* On Linux, login to Docker: `docker login`  
* Pull the db2server image.  
* Use the following .env_list:  

```
  LICENSE=accept  
  DB2INSTANCE=db2inst1  
  DB2INST1_PASSWORD=password  
  DBNAME=  
  BLU=false  
  ENABLE_ORACLE_COMPATIBILITY=false  
  UPDATEAVAIL=NO  
  TO_CREATE_SAMPLEDB=true  
  REPODB=false  
  IS_OSXFS=false  
  PERSISTENT_HOME=true  
  HADR_ENABLED=false  
  ETCD_ENDPOINT=  
  ETCD_USERNAME=  
  ETCD_PASSWORD=  
```

* Use `docker run` to setup `db2server` image:  

```shell
#!/bin/bash

docker run -h db2server_ \
        --name db2server --restart=always \
        --detach \
        --privileged=true \
        -p 50000:50000 -p 55000:55000 \
        --env-file .env_list \
        -v /database \
        store/ibmcorp/db2_developer_c:11.1.4.4-x86_64

rc=$?
if [[ rc -eq 0 ]]; then
    echo "success ..."
    echo "created container db2expc using image store/ibmcorp/db2_developer_c"
fi
```
  
* Use `docker logs -f db2server` to tail Db2 setup. `docker run`
will be setting up Db2 instance and the sample database.  
* Once the `db2server` setup is complete, copy `_TEST` directory from `github.com/asifjalil/cli`
to `db2inst1` id's home directory on `db2server` Docker image:  
```
  $ docker exec -ti db2server bash -c "su - db2inst1"  
  $ scp -r <username>@<server ip>:<dir>/_TEST .  
    * _<username>_ is the id that owns _github.com/asifjalil/cli_ files  
    * _<server ip>_ is the ip address of the server with the source code  
    * _<dir>_ is the directory where _TEST directory is located  
```

* Test also requires a UDF. Create it after copying the `_TEST` directory:  

```
  $ cd $HOME/_TEST  
  $ db2 "connect to sample"  
  $ db2 -tf sleep_proc.sql  
```

## Setup Db2 Driver
* This Go Db2 driver `cli` underneath uses _IBM DB2 ODBC/CLI
driver_ via **cgo**.  
* Before building _this Go driver_, you will need to
download [ibm_data_server_driver_for_odbc_cli_linuxx64_vxx.x.tar.gz](https://www-01.ibm.com/marketing/iwm/iwm/web/download.do?source=swg-idsoc97&pageType=urx&S_PKG=linuxAMD64). This requires an **IBM ID**.
* Create `/opt/ibm` directory:  
  `$ mkdir /opt/ibm`  
* Copy the download to `/opt/ibm`:  
  `$ cp ibm_data_server_driver_for_odbc_cli_linuxx64_v11.1.tar.gz /opt/ibm`  
* Unpack the tar.gz file:  
  `$ cd /opt/ibm; tar xvfz ibm_data_server_driver_for_odbc_cli_linuxx64_v11.1.tar.gz`  
* Setup and run `ldconfig` so Go can find the Db2 library:  
  `$ echo /opt/ibm/clidriver/lib/ > /etc/ld.so.conf.d/db2.conf ; ldconfig`  

## Run Test
* Go to the directory where `github.com/asifjalil/cli` source code is located.
* Run the following:
```
$ CGO_LDFLAGS=-L/opt/ibm/clidriver/lib \
      CGO_CFLAGS=-I/opt/ibm/clidriver/include \
      DATABASE_USER=db2inst1 \
      DATABASE_PASSWORD=password \
      DATABASE_HOMEDIR=/database/config/db2inst1 go test
```

