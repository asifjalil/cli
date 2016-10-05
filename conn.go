package cli

/*
#cgo LDFLAGS: -ldb2

#include <sqlcli1.h>
*/
import "C"
import (
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"unsafe"
)

type conn struct {
	hdbc   C.SQLHANDLE // connection handle
	closed bool
	// if true then autocommit is off and driver.Tx will commit or rollback
	tx bool
}

func (d *impl) Open(dsn string) (driver.Conn, error) {
	var hdbc C.SQLHANDLE
	re := regexp.MustCompile(`(?i:sqlconnect)\s*;`)

	ret := C.SQLAllocHandle(C.SQL_HANDLE_DBC, d.henv, &hdbc)
	if !success(ret) {
		return nil, formatError(C.SQL_HANDLE_ENV, d.henv)
	}
	if re.MatchString(dsn) {
		m := make(map[string]string)
		// init with defaults
		m["DATABASE"] = "SAMPLE"
		m["UID"] = ""
		m["PWD"] = ""

		s := strings.TrimLeft(dsn, re.FindString(dsn))
		ss := strings.Split(s, ";")
		for _, pair := range ss {
			if pair != "" {
				z := strings.Split(pair, "=")
				key := strings.ToUpper(strings.Trim(z[0], " "))
				value := strings.Trim(z[1], " ")
				m[key] = value
			}
		}
		ret = C.SQLConnectW(C.SQLHDBC(hdbc),
			(*C.SQLWCHAR)(unsafe.Pointer(stringToUTF16Ptr(m["DATABASE"]))),
			C.SQL_NTS,
			(*C.SQLWCHAR)(unsafe.Pointer(stringToUTF16Ptr(m["UID"]))),
			C.SQL_NTS,
			(*C.SQLWCHAR)(unsafe.Pointer(stringToUTF16Ptr(m["PWD"]))),
			C.SQL_NTS)
	} else {
		ret = C.SQLDriverConnectW(C.SQLHDBC(hdbc),
			C.SQLHWND(unsafe.Pointer(uintptr(0))),
			(*C.SQLWCHAR)(unsafe.Pointer(stringToUTF16Ptr(dsn))),
			C.SQL_NTS,
			nil,
			0,
			nil,
			C.SQL_DRIVER_NOPROMPT)
	}

	if !success(ret) {
		defer C.SQLFreeHandle(C.SQL_HANDLE_DBC, hdbc)
		return nil, formatError(C.SQL_HANDLE_DBC, hdbc)
	}

	return &conn{hdbc: hdbc}, nil
}

func (c *conn) Close() error {
	if c.closed {
		panic("database/sql/driver: [asifjalil][CLI Driver]: multiple connection Close")
	}

	c.closed = true

	// disconnect from the database
	ret := C.SQLDisconnect(C.SQLHDBC(c.hdbc))
	if !success(ret) {
		return formatError(C.SQL_HANDLE_DBC, c.hdbc)
	}

	// free the connection handle
	ret = C.SQLFreeHandle(C.SQL_HANDLE_DBC, c.hdbc)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_DBC, c.hdbc)
	}

	return nil
}

func (c *conn) Prepare(sql string) (driver.Stmt, error) {
	var hstmt C.SQLHANDLE // stmt handle
	wsql := stringToUTF16Ptr(sql)

	if c.closed {
		panic("database/sql/driver: [asifjalil][CLI Driver]: Prepare after conn Close")
	}

	// allocate statement handle
	ret := C.SQLAllocHandle(C.SQL_HANDLE_STMT, c.hdbc, &hstmt)
	if !success(ret) {
		return nil, formatError(C.SQL_HANDLE_DBC, c.hdbc)
	}

	// prepare the query
	ret = C.SQLPrepareW(C.SQLHSTMT(hstmt),
		(*C.SQLWCHAR)(unsafe.Pointer(wsql)), C.SQL_NTS)
	if !success(ret) {
		err := formatError(C.SQL_HANDLE_STMT, hstmt)
		// free the statement handle before returning
		C.SQLFreeHandle(C.SQL_HANDLE_STMT, hstmt)
		return nil, err
	}

	return &stmt{
		conn:  c,
		hstmt: hstmt,
		sql:   sql}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	if c.tx {
		panic("database/sql/driver: [asifjalil][CLI driver]: multiple Tx")
	}
	// turn off autocommit
	err := c.setAutoCommitAttr(C.SQL_AUTOCOMMIT_OFF)
	if err != nil {
		return nil, err
	}
	c.tx = true
	return &tx{c}, nil
}

// Turns autocommit on and off for a connection.
func (c *conn) setAutoCommitAttr(a uintptr) error {
	ret := C.SQLSetConnectAttr(C.SQLHDBC(c.hdbc), C.SQL_ATTR_AUTOCOMMIT,
		C.SQLPOINTER(a), C.SQL_IS_UINTEGER)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, c.hdbc)
	}
	return nil
}

// Commits or rollsback a transaction and turns on auto-commit for the connection
func (c *conn) endTx(commit bool) error {
	if !c.tx {
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: commit/rollback when not in a transaction")
	}
	c.tx = false

	var howToEnd C.SQLSMALLINT

	if commit {
		howToEnd = C.SQL_COMMIT
	} else {
		howToEnd = C.SQL_ROLLBACK
	}

	ret := C.SQLEndTran(C.SQL_HANDLE_DBC, c.hdbc, howToEnd)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_DBC, c.hdbc)
	}
	err := c.setAutoCommitAttr(C.SQL_AUTOCOMMIT_ON)
	if err != nil {
		return err
	}

	return nil
}
