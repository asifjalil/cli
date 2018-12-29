#!/bin/sh

DB2DIR=$HOME/sqllib
export CGO_LDFLAGS=-L${DB2DIR}/lib
export CGO_CFLAGS=-I${DB2DIR}/include

# use -v -x flags for debugging
#go build -v -x .
go build -ldflags '-linkmode external -extldflags "-static"' -x -a
#go test
