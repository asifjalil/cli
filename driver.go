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
