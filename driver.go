// Package **cli** provides access to a **DB2 database** using DB2 Call Level Interface (**CLI**) API.
// This requires **cgo** and DB2 _cli/odbc_ driver **libdb2.so**.  It is not possible to use this driver to create a statically linked Go package because
// IBM doesn't provide the DB2 _cli/odbc_ driver as _libdb2.a_ static library.
// On **Windows**, DB2 _cli/odbc_ library is not compatiable with **gcc**, but **cgo** requires **gcc**. Hence, this driver is not
// supported on Windows.
//
// **cli** is based on *alexbrainman's* odbc package: https://github.com/alexbrainman/odbc.
//
// This package registers a driver for the standard Go **database/sql** package and used through the
// **database/sql** API.
//
//	import _ "github.com/asifjalil/cli"
//
// ###Error Handling
// The package has no exported API except two functions-**SQLCode()** and **SQLState()**-for inspecting
// DB2 CLI error. The function signature is as follows:
//	func (e *cliError) SQLCode() int
//	func (e *cliError) SQLState() string
//
// Since package **cli** is imported for side-effects only, use the following code
// pattern to access SQLCode() and SQLState():
//	func checkError(err error) {
//		type sqlcode interface {
//			SQLCode() int
//		}
//		if err != nil {
//			if err, ok := err.(sqlcode); ok {
//				log.Println(err.SQLCode())
//			}
//			log.Fatal(err)
//		}
//	}
// The local interface can include SQLState() also for inspecting SQLState from DB2 CLI.
//
// **SQLCODE** is a return code from a IBM DB2 SQL operation.
// This code can be zero (0), negative, or positive.
//      0 means successful execution.
//      Negative means unsuccessful execution with an error.
//      For example -911 means a lock timeout occurred with a rollback.
//      Positive means successful execution with a warning.
//      For example +100 means no rows found or end of table.
//
// Search "SQL messages" in DB2 Information Center to find out more about SQLCODE.
//
// **SQLSTATE** is a return code like SQLCODE.
// But instead of a number, it is a five character error code that is consistent across all IBM database products.
// SQLSTATE follows this format: ccsss, where cc indicates class and sss indicates subclass.
// Search "SQLSTATE Messages" in DB2 Information Center for more detail.
//
// ###Connection String
// This driver uses DB2 CLI function **SQLConnect** and **SQLDriverConnect** in driver.Open(...).
// To use **SQLConnect**, start the name or the DSN string with keyword sqlconnect. This keyword is case insensitive.
// The connection string needs to follow this syntax to be valid:
//
//      "sqlconnect;[DATABASE=<database_name>;][UID=<user_id>;][PWD=<password>;]"
//
// [...] means optional. If a database_name is not provided, then SAMPLE is
// used as the database name. Also note that each keyword and value ends with
// a semicolon. The keyword "sqlconnect" doesn't take a value but ends with a semi-colon.
// Examples:
//	db, err := sql.Open("cli", "sqlconnect;")
//	db, err := sql.Open("cli", "sqlconnect; DATABASE=\"SAMPLE\";")
//
// Any other connection string must follow the connection string rule that is
// valid with SQLDriverConnect. For example, this is a valid dsn/connection string
// for SQLDriverConnect:
//
//      "DSN=Sample; UID=asif; PWD=secrect; AUTOCOMMIT=0; CONNECTTYPE=1;"
// Examples:
//	db, err := sql.Open("cli", "DSN=Sample; UID=asif; PWD=secrect; AUTOCOMMIT=0; CONNECTTYPE=1;")
//	db, err := sql.Open("cli", "DATABASE=db; HOSTNAME=dbhost; PORT=40000; PROTOCOL=TCPIP; UID=me; PWD=secret;")
//
// Search **SQLDriverConnect** in DB2 LUW *Information Center* for more detail.
//
// ## Installation
// IBM DB2 for Linux, Unix and Windows (DB2 LUW) implements its own ODBC driver.
// This package uses the DB2 ODBC/CLI driver through cgo.
// As such this package requires DB2 C headers and libraries for compilation.
// If you don't have DB2 LUW installed on the system, then you can install
// the free, community DB2 version *DB2 Express-C*.
// You can also use *IBM Data Server Driver package*. It includes the required headers and libraries
// but not a DB2 database manager.
//
// To install, download this package by running the following:
//	got get -d github.com/asifjalil/cli
// Go to the following directory:
//	$GOPATH/src/github.com/asifjalil/cli
// In that directory run the following to install the package:
//	./install.sh
// This script only works on Mac OS and Linux. For Windows, please
// set the CGO_LDFLAGS and CGO_CFLAGS, so cgo can locate the DB2 CLI
// library and CLI C header files.
// Then run:
//	go install
//
// ##Usage
// See `example_test.go`.
package cli

/*
#cgo LDFLAGS: -ldb2

#include <sqlcli1.h>
*/
import "C"

import (
	"database/sql"
	"fmt"
)

var drv impl

type (
	sql_DATE_STRUCT struct {
		year  C.SQLSMALLINT
		month C.SQLUSMALLINT
		day   C.SQLUSMALLINT
	}

	sql_TIMESTAMP_STRUCT struct {
		year     C.SQLSMALLINT
		month    C.SQLUSMALLINT
		day      C.SQLUSMALLINT
		hour     C.SQLUSMALLINT
		minute   C.SQLUSMALLINT
		second   C.SQLUSMALLINT
		fraction C.SQLUINTEGER
	}

	sql_TIME_STRUCT struct {
		hour   C.SQLUSMALLINT
		minute C.SQLUSMALLINT
		second C.SQLUSMALLINT
	}
)

type impl struct {
	henv C.SQLHANDLE // environment handle
}

func initDriver() error {
	// Allocate environment handle
	ret := C.SQLAllocHandle(C.SQL_HANDLE_ENV, C.SQL_NULL_HANDLE, &drv.henv)
	if !success(ret) {
		return fmt.Errorf("database/sql/driver: [asifjalil][CLI driver]Failed to allocate environment handle; rc: %d ", int(ret))
	}

	//use ODBC v3
	ret = C.SQLSetEnvAttr(C.SQLHENV(drv.henv),
		C.SQL_ATTR_ODBC_VERSION,
		C.SQLPOINTER(uintptr(C.SQL_OV_ODBC3)), 0)

	if !success(ret) {
		defer C.SQLFreeHandle(C.SQL_HANDLE_ENV, drv.henv)
		return formatError(C.SQL_HANDLE_ENV, drv.henv)
	}

	return nil
}

func init() {
	err := initDriver()
	if err != nil {
		panic(err)
	}
	sql.Register("cli", &drv)
}
