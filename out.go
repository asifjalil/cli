package cli

/*
#include <sqlcli1.h>
*/
import "C"
import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

// out is used to handle a INPUT/OUTPUT value parameter from stored procedures.
// Value/result from the database is stored at data.
// Once convertAssign is called, the data from the database
// is copied to sql.Out's Dest.
type out struct {
	sqlOut          *sql.Out
	idx             int // 1 based
	ctype           C.SQLSMALLINT
	sqltype         C.SQLSMALLINT
	decimalDigits   C.SQLSMALLINT
	nullable        C.SQLSMALLINT
	inputOutputType C.SQLSMALLINT
	parameterSize   C.SQLULEN
	data            []byte
	// BufferLength from SQLBindParam DB2 CLI function
	// Applies to database character and binary data only
	buflen C.SQLLEN
	// StrLen_or_IndPtr from SQLBindParam DB2 CLI function
	plen *C.SQLLEN
}

// newOut creates a out struct for handling a INPUT/OUTPUT parameter from a stored procedure.
// If the parameter type is INPUT_OUTPUT then out.data holds the input data from a
// Go application, and after a database execution holds database result or data.
// If the parameter type is OUTPUT only then out.data initially doesn't hold any data.
// After database execution, it holds the database result or data.
func newOut(hstmt C.SQLHANDLE, sqlOut *sql.Out, idx int) (*out, error) {
	var ctype, sqltype, decimalDigits, nullable, inputOutputType C.SQLSMALLINT
	var parameterSize C.SQLULEN
	var buflen C.SQLLEN
	var plen *C.SQLLEN
	var data []byte

	if sqlOut.In {
		inputOutputType = C.SQL_PARAM_INPUT_OUTPUT
		//convert sql.Out.Dest to a driver.Value so the number of possible type is limited.
		dv, err := driver.DefaultParameterConverter.ConvertValue(sqlOut.Dest)
		if err != nil {
			return nil, fmt.Errorf("%v : failed to convert Dest in sql.Out to driver.Value", err)
		}

		// use case with one type only. Otherwise d will turn into some other type and extract
		// will give incorrect result
		switch d := dv.(type) {
		case nil:
			var ind C.SQLLEN = C.SQL_NULL_DATA
			// nil has no type, so use SQLDescribeParam
			ret := C.SQLDescribeParam(C.SQLHSTMT(hstmt), C.SQLUSMALLINT(idx+1),
				&sqltype, &parameterSize, &decimalDigits, &nullable)
			if !success(ret) {
				return nil, formatError(C.SQL_HANDLE_STMT, hstmt)
			}
			// input value might be nil but the output value may not be
			// so allocate buffer for output
			data = make([]byte, parameterSize)
			ctype = sqlTypeToCType(sqltype)
			buflen = C.SQLLEN(len(data))
			plen = &ind
		case string:
			var ind C.SQLLEN = C.SQL_NTS
			// data buffer size can't be based on the input size
			// because it can be smaller than the output size.
			// So use SQLDescribeParam for the parameter size on the database.
			ret := C.SQLDescribeParam(C.SQLHSTMT(hstmt), C.SQLUSMALLINT(idx+1),
				&sqltype, &parameterSize, &decimalDigits, &nullable)
			if !success(ret) {
				return nil, formatError(C.SQL_HANDLE_STMT, hstmt)
			}
			// Using WCHAR instead of CHAR type because Go string is utf-8 coded.
			// That maps to WCHAR in ODBC/CLI.
			ctype = C.SQL_C_WCHAR
			sqltype = C.SQL_WCHAR
			// https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.cli.doc/doc/c0006840.html
			// The Unicode string arguments must be in UCS-2 encoding (native-endian format).
			s16 := stringToUTF16(d)
			b := extractUTF16Str(s16)
			// utf16 uses 2 bytes per character, plus the 2 byte null terminator
			data = make([]byte, (parameterSize*2)+2)
			if len(b) > len(data) {
				return nil,
					fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]:"+
						"At param. index %d INOUT utf16 string %q size %d is greater than the allocated OUT buffer size %d",
						idx+1, d, len(b), len(data))
			}
			copy(data, b)
			buflen = C.SQLLEN(len(data))
			// use SQL_NTS to indicate that the string is null terminated
			plen = &ind
		case int64:
			ctype = C.SQL_C_SBIGINT
			sqltype = C.SQL_BIGINT
			data = extract(unsafe.Pointer(&d), unsafe.Sizeof(d))
			parameterSize = 8
		case float64:
			ctype = C.SQL_C_DOUBLE
			sqltype = C.SQL_DOUBLE
			data = extract(unsafe.Pointer(&d), unsafe.Sizeof(d))
			parameterSize = 8
		case bool:
			var b byte
			if d {
				b = 1
			}
			ctype = C.SQL_C_BIT
			sqltype = C.SQL_BIT
			data = extract(unsafe.Pointer(&b), unsafe.Sizeof(b))
			parameterSize = 1
		case time.Time:
			ctype = C.SQL_C_TYPE_TIMESTAMP
			sqltype = C.SQL_TYPE_TIMESTAMP
			y, m, day := d.Date()
			t := sql_TIMESTAMP_STRUCT{
				year:     C.SQLSMALLINT(y),
				month:    C.SQLUSMALLINT(m),
				day:      C.SQLUSMALLINT(day),
				hour:     C.SQLUSMALLINT(d.Hour()),
				minute:   C.SQLUSMALLINT(d.Minute()),
				second:   C.SQLUSMALLINT(d.Second()),
				fraction: C.SQLUINTEGER(d.Nanosecond()),
			}
			data = extract(unsafe.Pointer(&t), unsafe.Sizeof(t))
			decimalDigits = 3
			parameterSize = 20 + C.SQLULEN(decimalDigits)
		case []byte:
			ret := C.SQLDescribeParam(C.SQLHSTMT(hstmt), C.SQLUSMALLINT(idx+1),
				&sqltype, &parameterSize, &decimalDigits, &nullable)
			if !success(ret) {
				return nil, formatError(C.SQL_HANDLE_STMT, hstmt)
			}
			ctype = sqlTypeToCType(sqltype)
			data = make([]byte, parameterSize)
			copy(data, d)
			buflen = C.SQLLEN(len(data))
			plen = &buflen
		default:
			return nil, fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: unsupported type %T in sql.Out.Dest at index %d",
				d, idx+1)
		}
	} else {
		inputOutputType = C.SQL_PARAM_OUTPUT
		ret := C.SQLDescribeParam(C.SQLHSTMT(hstmt), C.SQLUSMALLINT(idx+1),
			&sqltype, &parameterSize, &decimalDigits, &nullable)
		if !success(ret) {
			return nil, formatError(C.SQL_HANDLE_STMT, hstmt)
		}
		if sqltype == C.SQL_WCHAR {
			// Output is a utf16 string that requires 2 bytes per character
			// and 2 byte null terminator
			data = make([]byte, (parameterSize*2)+2)
		} else {
			data = make([]byte, parameterSize)
		}
		ctype = sqlTypeToCType(sqltype)
		buflen = C.SQLLEN(len(data))
		plen = &buflen
	}

	return &out{
		sqlOut:          sqlOut,
		idx:             idx + 1,
		ctype:           ctype,
		sqltype:         sqltype,
		decimalDigits:   decimalDigits,
		nullable:        nullable,
		inputOutputType: inputOutputType,
		parameterSize:   parameterSize,
		data:            data,
		buflen:          buflen,
		plen:            plen,
	}, nil
}

// value converts database data to driver.Value.
func (o *out) value() (driver.Value, error) {
	var p unsafe.Pointer
	buf := o.data

	if (o.plen != nil && *(o.plen) == C.SQL_NULL_DATA) || len(buf) == 0 {
		return nil, nil
	}
	p = unsafe.Pointer(&buf[0])

	switch o.ctype {
	case C.SQL_C_BIT:
		return buf[0] != 0, nil
	case C.SQL_C_SHORT, C.SQL_C_LONG:
		return *((*int32)(p)), nil
	case C.SQL_C_SBIGINT:
		return *((*int64)(p)), nil
	case C.SQL_C_DOUBLE, C.SQL_C_FLOAT:
		return *((*float64)(p)), nil
	case C.SQL_C_CHAR:
		// handle DECFLOAT whose default C type is CHAR
		if o.sqltype == C.SQL_DECFLOAT || o.sqltype == C.SQL_DECIMAL || o.sqltype == C.SQL_NUMERIC {
			s := string(buf[:o.buflen])
			f, err := strconv.ParseFloat(s, 64)
			return f, err
		}
		return buf[:o.buflen], nil
	case C.SQL_C_WCHAR, C.SQL_C_DBCHAR:
		if p == nil {
			return nil, nil
		}
		s := (*[1 << 28]uint16)(p)[: len(buf)/2 : len(buf)/2]
		// s := (*[1 << 20]uint16)(p)[:len(buf)/2]
		return utf16ToUTF8(s), nil
	case C.SQL_C_TYPE_TIMESTAMP:
		t := (*sql_TIMESTAMP_STRUCT)(p)
		r := time.Date(int(t.year),
			time.Month(t.month),
			int(t.day),
			int(t.hour),
			int(t.minute),
			int(t.second),
			int(t.fraction),
			time.Local)
		return r, nil
	case C.SQL_C_TYPE_DATE:
		t := (*sql_DATE_STRUCT)(p)
		r := time.Date(int(t.year),
			time.Month(t.month),
			int(t.day),
			0, 0, 0, 0, time.Local)
		return r, nil
	case C.SQL_C_TYPE_TIME:
		t := (*sql_TIME_STRUCT)(p)
		r := time.Date(0, 0, 0,
			int(t.hour),
			int(t.minute),
			int(t.second),
			0,
			time.Local)
		return r, nil
	case C.SQL_C_BINARY:
		// We allocated columnsized byte slice for o.data
		// That's the max size. Returned data can be shorter
		// than that, so remove null bytes.
		b := bytes.Trim(buf, "\x00")
		if len(b) == 0 {
			return nil, nil
		}
		return b, nil
	}
	return nil, fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: unsupported ctype %d (sqltype: %d) for stored procedure OUTPUT parameter value at index %d",
		o.ctype, o.sqltype, o.idx)
}

// convertAssign copies database data at data to Dest in sql.Out.
// It first converts the byte slice at data to driver.Value and then copies and converts to the type in sql.Out Dest.
func (o *out) convertAssign() error {
	if o.sqlOut == nil {
		return fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: sql.Out is nil at OUTPUT param index %d", o.idx)
	}

	if o.sqlOut.Dest == nil {
		return fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: Dest is nil in sql.Out at OUTPUT param index %d", o.idx)
	}

	dest_info := reflect.ValueOf(o.sqlOut.Dest)
	if dest_info.Kind() != reflect.Ptr {
		return fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: Dest in sql.Out at OUTPUT param index %d is not a pointer", o.idx)
	}

	dv, err := o.value()
	if err != nil {
		return err
	}

	return convertAssign(o.sqlOut.Dest, dv)
}
