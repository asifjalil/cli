FROM ubuntu:18.04 AS build

# disable interactive functions
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -y dist-upgrade && apt-get -y install build-essential && apt-get clean && apt-get -y install golang-1.10 golang-go git

WORKDIR /app

ENV GOPATH /app

RUN mkdir -p /app/src/exampleProject

# . folder should contain the golang Project sourcecode + ibm_data_server_driver_for_odbc_cli_linuxx64_v11.1.tar.gz from IBM

ADD . /app/src/exampleProject

RUN mkdir -p /opt/ibm ; mv /app/src/exampleProject/ibm_data_server_driver_for_odbc_cli_linuxx64_v11.1.tar.gz /opt/ibm ; cd /opt/ibm ; tar xfvz ibm_data_server_driver_for_odbc_cli_linuxx64_v11.1.tar.gz ; rm -f /opt/ibm/ibm_data_server_driver_for_odbc_cli_linuxx64_v11.1.tar.gz

RUN echo /opt/ibm/clidriver/lib/ > /etc/ld.so.conf.d/db2.conf ; ldconfig
RUN cd /app/src/exampleProject; CGO_LDFLAGS=-L/opt/ibm/clidriver/lib CGO_CFLAGS=-I/opt/ibm/clidriver/include go get . ; 
RUN export CGO_ENABLED=1 ; export GOOS=linux; CGO_LDFLAGS=-L/opt/ibm/clidriver/lib CGO_CFLAGS=-I/opt/ibm/clidriver/include go build .
CMD /bin/sh

FROM ubuntu:18.04
WORKDIR /app
RUN cd /app
COPY --from=build /app/src/exampleProject/exampleProject /app/exampleProject
COPY --from=build /opt/ibm /opt/ibm
COPY --from=build /etc/ld.so.conf.d/db2.conf /etc/ld.so.conf.d/db2.conf
RUN ldconfig
RUN apt update && apt -y install libxml2
CMD ["/app/exampleProject"]
