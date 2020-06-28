#!/bin/bash

testname=$1
goTest="go test -gcflags=all=-d=checkptr"

if [[ ! -z "$testname" ]]; then
    goTest="$goTest -run $testname"
fi

CGO_LDFLAGS=-L/opt/ibm/clidriver/lib \
      CGO_CFLAGS=-I/opt/ibm/clidriver/include \
      DATABASE_USER=db2inst1 \
      DATABASE_PASSWORD=password \
      DATABASE_HOMEDIR=/database/config/db2inst1 $goTest
