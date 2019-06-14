package cli

/*
#include <sqlcli1.h>
*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
	"unsafe"
)

type param struct {
	// buf points to driver.Value.
	buf unsafe.Pointer
	// plen is the buf length.
	plen *C.SQLLEN
	// inout is a special case driver.Value.
	// When driver.Value is of type sql.Out
	// it requires some extra processing.
	inout *out
}

// bindParam binds a driver.Value (Go value) to a parameter marker in an SQL statement.
// That bound value is returned as *param. Go code needs to keep a reference to
// to this bound value so Go GC won't remove it before SQLExecute is called.
func bindParam(s *stmt, idx int, v driver.Value) (*param, error) {
	var (
		ctype, sqltype, decimal C.SQLSMALLINT
		size                    C.SQLULEN
		buflen                  C.SQLLEN
		plen                    *C.SQLLEN
		buf                     unsafe.Pointer
		inputOutputType         C.SQLSMALLINT = C.SQL_PARAM_INPUT
		inout                   *out
	)

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
			return nil, formatError(C.SQL_HANDLE_STMT, s.hstmt)
		}

		ctype = C.SQL_C_DEFAULT
		sqltype = dataType
		buf = nil
		size = parameterSize
		decimal = decimalDigits
		buflen = 0
		plen = &ind
	case string:
		var ind C.SQLLEN = C.SQL_NTS
		ctype = C.SQL_C_WCHAR
		sqltype = C.SQL_WCHAR
		b := stringToUTF16(d)
		buf = unsafe.Pointer(&b[0])
		l := len(b)
		if l == 0 {
			// size cannot be less than 1 even for empty field
			l = 1
		}
		l *= 2 // every char takes 2 bytes
		buflen = C.SQLLEN(l)
		// use SQL_NTS to indicate that the string is null terminated
		plen = &ind
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
		var err error
		inout, err = newOut(s.hstmt, &d, idx)
		if err != nil {
			return nil, err
		}
		sqltype = inout.sqltype
		ctype = inout.ctype
		size = inout.parameterSize
		decimal = inout.decimalDigits
		inputOutputType = inout.inputOutputType
		b := inout.data
		if len(b) > 0 {
			buf = unsafe.Pointer(&b[0])
		}
		buflen = inout.buflen
		plen = inout.plen
		//s.outs = append(s.outs, o)
	default:
		return nil, fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: unsupported bind param. type %T at index %d", v, idx+1)
	}
	ret := C.SQLBindParameter(C.SQLHSTMT(s.hstmt), C.SQLUSMALLINT(idx+1),
		inputOutputType, ctype, sqltype, size, decimal,
		C.SQLPOINTER(buf), buflen, plen)
	if !success(ret) {
		return nil, formatError(C.SQL_HANDLE_STMT, s.hstmt)
	}
	return &param{plen: plen, buf: buf, inout: inout}, nil
}
