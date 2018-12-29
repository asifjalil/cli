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
	"io"
	"reflect"
	"time"
	"unsafe"
)

type stmt struct {
	conn   *conn
	hstmt  C.SQLHANDLE // statement handle
	sql    string
	closed bool
	rows   bool
	cols   []*column
	outs   []*out
}

func (s *stmt) Close() error {
	if s.rows {
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: stmt Close with active Rows")
	}
	if s.closed {
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: double Close of Stmt")
	}
	s.closed = true
	ret := C.SQLFreeHandle(C.SQL_HANDLE_STMT, s.hstmt)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, s.hstmt)
	}

	s.cols = nil
	s.outs = nil

	return nil
}

func (s *stmt) NumInput() int {
	var paramCount C.SQLSMALLINT
	if s.closed {
		return -1
	}
	ret := C.SQLNumParams(C.SQLHSTMT(s.hstmt), &paramCount)
	if !success(ret) {
		return -1
	}
	return int(paramCount)
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.exec(context.Background(), args)
}

// go1.8+
// ExecContext implements driver.StmtExecContext interface
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dargs := make([]driver.Value, len(args))
	for n, param := range args {
		dargs[n] = param.Value
	}

	return s.exec(ctx, dargs)
}

// CheckNamedValue implementes driver.NamedValueChecker.
func (s *stmt) CheckNamedValue(nv *driver.NamedValue) (err error) {
	switch nv.Value.(type) {
	case sql.Out:
		err = nil
	default:
		nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
	}
	return err
}

// exec is created to handle both Exec(...) and ExecContext(...)
func (s *stmt) exec(ctx context.Context, args []driver.Value) (driver.Result, error) {
	err := s.sqlexec(ctx, args)
	if err != nil {
		return nil, err
	}

	r, err := s.rowsAffected()
	if err != nil {
		return nil, err
	}

	return &result{rows: r}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.query(context.Background(), args)
}

// go1.8+
// QueryContext implements driver.StmtQueryContext interface
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs := make([]driver.Value, len(args))
	for n, param := range args {
		dargs[n] = param.Value
	}

	return s.query(ctx, dargs)
}

// query is created to handle both Query(...) and QueryContext(...)
func (s *stmt) query(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	err := s.sqlexec(ctx, args)
	if err != nil {
		return nil, err
	}

	// attach the statement to the result set columns
	err = s.bindColumns()
	if err != nil {
		return nil, err
	}
	s.rows = true
	return &rows{s, true}, nil
}

// bindParam binds a driver.Value (Go value) to a parameter marker in an SQL statement
func (s *stmt) bindParam(idx int, v driver.Value) error {
	var ctype, sqltype, decimal C.SQLSMALLINT
	var size C.SQLULEN
	var buflen C.SQLLEN
	var plen *C.SQLLEN
	var buf unsafe.Pointer
	var inputOutputType C.SQLSMALLINT = C.SQL_PARAM_INPUT

	switch d := v.(type) {
	case nil:
		var dataType, decimalDigits, nullable C.SQLSMALLINT
		var parameterSize C.SQLULEN
		var ind C.SQLLEN = C.SQL_NULL_DATA

		// nil has no type, so use SQLDescribeParam to determine the
		// parameter type.
		ret := C.SQLDescribeParam(C.SQLHSTMT(s.hstmt), C.SQLUSMALLINT(idx+1),
			&dataType, &parameterSize, &decimalDigits, &nullable)
		if !success(ret) {
			return formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}

		ctype = C.SQL_C_DEFAULT
		sqltype = dataType
		buf = nil
		size = parameterSize
		decimal = decimalDigits
		buflen = 0
		plen = &ind
	case string:
		ctype = C.SQL_C_WCHAR
		sqltype = C.SQL_WCHAR
		b := stringToUTF16(d)
		buf = unsafe.Pointer(&b[0])
		l := len(b)
		l -= 1 //remove terminating 0
		colSize := C.SQLULEN(l)
		if colSize < 1 {
			// size cannot be less than 1 even for empty field
			colSize = 1
		}
		l *= 2 // every char takes 2 bytes
		buflen = C.SQLLEN(l)
		plen = &buflen
	case int64:
		ctype = C.SQL_C_SBIGINT
		sqltype = C.SQL_BIGINT
		buf = unsafe.Pointer(&d)
		size = 8
	case bool:
		var b byte
		if d {
			b = 1
		}
		ctype = C.SQL_C_BIT
		sqltype = C.SQL_BIT
		buf = unsafe.Pointer(&b)
		size = 1
	case float64:
		ctype = C.SQL_C_DOUBLE
		sqltype = C.SQL_DOUBLE
		buf = unsafe.Pointer(&d)
		size = 8
	case time.Time:
		ctype = C.SQL_C_TYPE_TIMESTAMP
		sqltype = C.SQL_TYPE_TIMESTAMP
		y, m, day := d.Date()
		b := sql_TIMESTAMP_STRUCT{
			year:     C.SQLSMALLINT(y),
			month:    C.SQLUSMALLINT(m),
			day:      C.SQLUSMALLINT(day),
			hour:     C.SQLUSMALLINT(d.Hour()),
			minute:   C.SQLUSMALLINT(d.Minute()),
			second:   C.SQLUSMALLINT(d.Second()),
			fraction: C.SQLUINTEGER(d.Nanosecond()),
		}
		buf = unsafe.Pointer(&b)
		// based on DB2 manual: SQLBindParameter
		// The precision of a time timestamp value is the number of digits
		// to the right of the decimal point in the string representation
		// of a time or timestamp (for example, the scale of yyyy-mm-dd hh:mm:ss.fff is 3)
		decimal = 3
		size = 20 + C.SQLULEN(decimal)
	case []byte:
		ctype = C.SQL_C_BINARY
		sqltype = C.SQL_BINARY
		b := make([]byte, len(d))
		copy(b, d)
		// handle empty binary field
		if len(b) > 0 {
			buf = unsafe.Pointer(&b[0])
		}
		buflen = C.SQLLEN(len(b))
		plen = &buflen
		size = C.SQLULEN(len(b))
	case sql.Out:
		var dataType, decimalDigits, nullable C.SQLSMALLINT
		var parameterSize C.SQLULEN
		ret := C.SQLDescribeParam(C.SQLHSTMT(s.hstmt), C.SQLUSMALLINT(idx+1),
			&dataType, &parameterSize, &decimalDigits, &nullable)
		if !success(ret) {
			return formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}
		sqltype = dataType
		ctype = sqlTypeToCType(sqltype)
		size = parameterSize
		decimal = decimalDigits
		inputOutputType = C.SQL_PARAM_OUTPUT
		b := make([]byte, parameterSize)
		if len(b) > 0 {
			buf = unsafe.Pointer(&b[0])
		}
		buflen = C.SQLLEN(len(b))
		plen = &buflen
		s.outs = append(s.outs, &out{
			sqlOut:  &d,
			idx:     idx + 1,
			data:    b,
			ctype:   ctype,
			sqltype: sqltype,
			len:     buflen,
		})
	default:
		return fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: unsupported bind param. type %T at position %d", v, idx+1)
	}
	ret := C.SQLBindParameter(C.SQLHSTMT(s.hstmt), C.SQLUSMALLINT(idx+1),
		inputOutputType, ctype, sqltype, size, decimal,
		C.SQLPOINTER(buf), buflen, plen)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, s.hstmt)
	}
	return nil
}

// sqlexec executes any prepared statement
func (s *stmt) sqlexec(ctx context.Context, args []driver.Value) error {
	if s.closed {
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: Query after stmt Close")
	}
	if s.rows {
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: new Query but the stmt has active Rows")
	}

	// Check if the context has a deadline
	// Update: a context can be cancelled and may not have deadline. Handle that.
	// if _, ok := ctx.Deadline(); ok {
	/*
		// Initially used the timeout seconds from the context
		// as the timeout value for SQL_ATTR_QRY_TIMEOUT.
		// But DB2 didn't terminate the statement on time as expected.
		// As a result stmt.Close(...) function waited until the query is done
		// before freeing the stmt handle.
		timeout := deadline.Sub(time.Now())
		timeoutSec := C.SQLINTEGER(timeout.Seconds())
		fmt.Printf("timeout(sec): %t\n", timeoutSec)

		ret := C.SQLSetStmtAttr(C.SQLHSTMT(s.hstmt),
			C.SQL_ATTR_QUERY_TIMEOUT,
			C.SQLPOINTER(uintptr(timeoutSec)),
			C.SQL_IS_UINTEGER)
		if !success(ret) {
			return formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}

		var realTimeout C.SQLINTEGER
		ret = C.SQLGetStmtAttr(C.SQLHSTMT(s.hstmt),
			C.SQL_ATTR_QUERY_TIMEOUT,
			C.SQLPOINTER(&realTimeout),
			0, nil)

		if !success(ret) {
			return formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}

		fmt.Printf("real timeout(sec): %t\n", realTimeout)
	*/

	// Enabling option to call SQLExecute asynchronously.
	// This way, we can use SQLCancel to cancel the query when
	// the context times out.
	// Update: It seems that there is no need to enable ASYNC to use SQLCancel.
	// According to
	// https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.cli.doc/doc/r0000567.html
	// *Canceling functions in multithread applications*
	// In a multithread application, the application can cancel a function that is running synchronously on a statement.
	// To cancel the function, the application calls SQLCancel() with the same statement handle as that used by the target
	// function, but on a different thread. How the function is canceled depends upon the operating system.
	// The return code of the SQLCancel() call indicates only whether CLI processed the request successfully.
	// Only SQL_SUCCESS or SQL_ERROR can be returned; no SQLSTATEs are returned.
	// If the original function is canceled, it returns SQL_ERROR and SQLSTATE HY008 (Operation was Canceled.).
	/*
		ret := C.SQLSetStmtAttr(C.SQLHSTMT(s.hstmt),
			C.SQL_ATTR_ASYNC_ENABLE,
			C.SQLPOINTER(uintptr(C.SQL_ASYNC_ENABLE_ON)),
			0)
		if !success(ret) {
			return formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}
	*/
	// }

	// bind values to parameters
	for i, a := range args {
		err := s.bindParam(i, a)
		if err != nil {
			return err
		}
	}

	// execute the statement in a separate go routine. That way if the context is cancelled
	// or the context deadline/timeout reached, we can call SQLCancel in the main thread.
	qry := make(chan C.SQLRETURN)
	go func() {
		var ret C.SQLRETURN
		ret = C.SQLExecute(C.SQLHSTMT(s.hstmt))
		qry <- ret
		close(qry)
	}()

	select {
	case <-ctx.Done():
		ret := C.SQLCancel(C.SQLHSTMT(s.hstmt))
		if !success(ret) {
			return formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}

		errStr := ctx.Err().Error()
		return &cliError{sqlcode: 0,
			sqlstate: "HY008",
			message:  errStr + ": SQL Operation was cancelled."}
	case ret := <-qry:
		// if ret == C.SQL_NO_DATA_FOUND {
		// may this is a searched UPDATE/DELETE and no row satisfied the search condition
		// }
		if !success(ret) {
			return formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}
		for _, o := range s.outs {
			if err := o.convertAssign(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *stmt) rowsAffected() (int64, error) {
	var c C.SQLLEN
	ret := C.SQLRowCount(C.SQLHSTMT(s.hstmt), &c)
	if !success(ret) {
		return 0, formatError(C.SQL_HANDLE_STMT, s.hstmt)
	}

	return int64(c), nil
}

func (s *stmt) bindColumns() error {
	var n C.SQLSMALLINT
	// count number of columns
	ret := C.SQLNumResultCols(C.SQLHSTMT(s.hstmt), &n)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, s.hstmt)
	}
	if n < 1 {
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: driver.Stmt.Query(...) did not create a result set")
	}
	// fetch column descriptions
	s.cols = make([]*column, n)
	for i := range s.cols {
		c, err := newColumn(C.SQLHSTMT(s.hstmt), i)
		if err != nil {
			return err
		}
		s.cols[i] = c
	}
	return nil
}

// [ -- driver.Rows ]
type rows struct {
	s *stmt
	// Used to implement HasNextResultSet()
	hasNextResultSet bool
}

func (r *rows) Columns() []string {
	names := make([]string, len(r.s.cols))
	for i := 0; i < len(names); i++ {
		names[i] = r.s.cols[i].name
	}
	return names
}

func (r *rows) Close() error {
	ret := C.SQLFreeStmt(C.SQLHSTMT(r.s.hstmt), C.SQL_UNBIND)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, r.s.hstmt)
	}

	ret = C.SQLFreeStmt(C.SQLHSTMT(r.s.hstmt), C.SQL_RESET_PARAMS)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, r.s.hstmt)
	}

	ret = C.SQLFreeStmt(C.SQLHSTMT(r.s.hstmt), C.SQL_CLOSE)
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, r.s.hstmt)
	}

	for i := range r.s.cols {
		r.s.cols[i] = nil
	}

	r.s.rows = false
	r.s = nil
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.s == nil {
		return errors.New("database/sql/driver: [asifjalil][CLI Driver]: Next on closed Rows")
	}
	ret := C.SQLFetch(C.SQLHSTMT(r.s.hstmt))
	if ret == C.SQL_NO_DATA {
		return io.EOF
	}
	if !success(ret) {
		return formatError(C.SQL_HANDLE_STMT, C.SQLHANDLE(r.s.hstmt))
	}

	for i := range dest {
		v, err := r.s.cols[i].value()
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

// [ -- driver.Rows Go 1.8+ features --]
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.s.cols[index].typeName()
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.s.cols[index].typeNullable()
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return r.s.cols[index].typePrecisionScale()
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	return r.s.cols[index].typeLength()
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	return r.s.cols[index].scanType()
}

func (r *rows) HasNextResultSet() bool {
	return r.hasNextResultSet
}

func (r *rows) NextResultSet() error {
	switch ret := C.SQLMoreResults(C.SQLHSTMT(r.s.hstmt)); ret {
	case C.SQL_SUCCESS:
		err := r.s.bindColumns()
		return err
	case C.SQL_NO_DATA_FOUND:
		r.hasNextResultSet = false
		return io.EOF
	default:
		return formatError(C.SQL_HANDLE_STMT, C.SQLHANDLE(r.s.hstmt))
	}
}

// [ -- driver.Result --]
type result struct {
	id   int64
	rows int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.id, errors.New("not implemented")
}

func (r *result) RowsAffected() (int64, error) {
	return r.rows, nil
}
