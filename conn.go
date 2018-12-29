package cli

/*
#include <sqlcli1.h>
*/
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
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
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: Called close on already closed connection")
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

// Prepare allocates a statement handle and associates the sql string with the handle.
func (c *conn) Prepare(sql string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), sql)
}

// Similar to Prepare but additionally uses a context. If the context is cancelled then
// the function returns an error and a nil statement handle.
func (c *conn) PrepareContext(ctx context.Context, sql string) (driver.Stmt, error) {
	var hstmt C.SQLHANDLE = C.SQL_NULL_HSTMT // stmt handle

	if c.closed {
		return nil, errors.New("database/sql/driver: [asifjalil][CLI Driver]: called Prepare but the conn is closed")
	}

	// allocates a stmt handle to hstmt
	wsql := stringToUTF16Ptr(sql)
	// allocate stmt handle
	ret := C.SQLAllocHandle(C.SQL_HANDLE_STMT, c.hdbc, &hstmt)
	if !success(ret) {
		return nil, formatError(C.SQL_HANDLE_DBC, c.hdbc)
	}

	// According to DB2 LUW manual, deferred prepare is on by default.
	// The prepare request is not sent to the server until either
	// SQLDescribeParam(), SQLExecute(), SQLNumResultCols(), SQLDescribeCol(), or
	// SQLColAttribute() is called using the same statement handle as the prepared statement.
	// That means SQLPrepareW should be quick and no need to cancel it.
	// Also, search for SQL_ATTR_ASYNC_ENABLE to see the CLI functions that can be called asynchronously
	// and can be cancelled using SQLCancel. SQLPrepareW is not one of them.
	// Prepare the query.
	ret = C.SQLPrepareW(C.SQLHSTMT(hstmt),
		(*C.SQLWCHAR)(unsafe.Pointer(wsql)), C.SQL_NTS)
	if !success(ret) {
		C.SQLFreeHandle(C.SQL_HANDLE_STMT, hstmt)
		return nil, formatError(C.SQL_HANDLE_STMT, hstmt)
	}

	select {
	default:
	case <-ctx.Done():
		if hstmt != C.SQL_NULL_HSTMT {
			C.SQLFreeHandle(C.SQL_HANDLE_STMT, hstmt)
		}
		return nil, ctx.Err()
	}
	return &stmt{
		conn:  c,
		hstmt: hstmt,
		sql:   sql}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx {
		return nil, errors.New("database/sql/driver: [asifjalil][CLI driver]: called BeginTx on already active transaction")
	}

	// turn off autocommit
	err := c.setAutoCommitAttr(C.SQL_AUTOCOMMIT_OFF)
	if err != nil {
		return nil, err
	}

	// Set ReadOnly
	if opts.ReadOnly {
		ret := C.SQLSetConnectAttr(C.SQLHDBC(c.hdbc), C.SQL_ATTR_ACCESS_MODE,
			C.SQLPOINTER(uintptr(C.SQL_MODE_READ_ONLY)), C.SQL_IS_INTEGER)
		if !success(ret) {
			return nil, formatError(C.SQL_HANDLE_STMT, c.hdbc)
		}
	}

	// Set isolation level
	err = nil
	switch level := sql.IsolationLevel(opts.Isolation); level {
	case sql.LevelDefault, sql.LevelReadCommitted:
		// nothing to do
	case sql.LevelReadUncommitted:
		err = c.setIsolationLevel(C.SQL_TXN_READ_UNCOMMITTED)
	case sql.LevelWriteCommitted:
		err = errors.New("database/sql/driver: [asifjalil][CLI Driver]: sql.LevelWriteCommitted isolation level is not supported")
	case sql.LevelRepeatableRead:
		// RS (read-stability) isolation
		err = c.setIsolationLevel(C.SQL_TXN_REPEATABLE_READ)
	case sql.LevelSnapshot:
		err = errors.New("database/sql/driver: [asifjalil][CLI Driver]: sql.LevelSnapshot isolation level is not supported")
	case sql.LevelSerializable:
		// RR isolation
		err = c.setIsolationLevel(C.SQL_TXN_SERIALIZABLE)
	case sql.LevelLinearizable:
		err = errors.New("database/sql/driver: [asifjalil][CLI Driver]: sql.LevelLinearizable isolation level is not supported")
	default:
		err = fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: isolation level %v is not supported",
			sql.IsolationLevel(opts.Isolation))
	}

	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		c.Close()
		return nil, ctx.Err()
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

// Sets Isolation Level for a connection
func (c *conn) setIsolationLevel(a uintptr) error {
	ret := C.SQLSetConnectAttr(C.SQLHDBC(c.hdbc), C.SQL_ATTR_TXN_ISOLATION,
		C.SQLPOINTER(a), C.SQL_NTS)
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
