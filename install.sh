#!/bin/sh

DB2DIR=$HOME/sqllib
export CGO_LDFLAGS=-L${DB2DIR}/lib
export CGO_CFLAGS=-I${DB2DIR}/include

go install
