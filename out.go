package cli

/*
#include <sqlcli1.h>
*/
import "C"
import (
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
			ctype = C.SQL_C_WCHAR
			sqltype = C.SQL_WCHAR
			s16 := stringToUTF16(d)
			data = extract(unsafe.Pointer(&s16[0]), unsafe.Sizeof(s16))
			l := len(data)
			if l == 0 {
				l = 1 // size cannot be negative
			}
			l *= 2 // every char on the database takes 2 bytes
			buflen = C.SQLLEN(l)
			// use SQL_NTS to indicate that the string null terminated
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
			ctype = C.SQL_C_BINARY
			sqltype = C.SQL_BINARY
			data = make([]byte, len(d))
			copy(data, d)
			buflen = C.SQLLEN(len(data))
			plen = &buflen
			parameterSize = C.SQLULEN(len(data))
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
		data = make([]byte, parameterSize)
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
	case C.SQL_C_DOUBLE:
		return *((*float64)(p)), nil
	case C.SQL_C_CHAR:
		// handle DECFLOAT whose default C type is CHAR
		if o.sqltype == C.SQL_DECFLOAT {
			s := string(buf[:o.buflen])
			f, err := strconv.ParseFloat(s, 64)
			return f, err
		}
		return buf[:o.buflen], nil
	case C.SQL_C_WCHAR:
		if p == nil {
			return nil, nil
		}
		s := (*[1 << 20]uint16)(p)[:len(buf)/2]
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
		return buf[:o.buflen], nil
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

// https://tylerchr.blog/golang-arbitrary-memory
// extract reads arbitrary memory.
func extract(ptr unsafe.Pointer, size uintptr) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = *((*byte)(unsafe.Pointer(uintptr(ptr) + uintptr(i))))
	}
	return out
}
