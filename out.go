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

// out is used to handle OUTPUT value parameters from stored procedures.
// Data from the database is stored at data.
// Once convertAssign is called, the data from the database
// is copied to sql.Out's Dest.
type out struct {
	sqlOut  *sql.Out
	idx     int
	data    []byte
	ctype   C.SQLSMALLINT
	sqltype C.SQLSMALLINT
	len     C.SQLLEN // StrLen_or_IndPtr; Indicates the size of the data fetched into data
}

// value converts database data to driver.Value.
func (o *out) value() (driver.Value, error) {
	var p unsafe.Pointer
	buf := o.data

	if o.len == C.SQL_NULL_DATA || len(buf) == 0 {
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
			s := string(buf[:o.len])
			f, err := strconv.ParseFloat(s, 64)
			return f, err
		}
		return buf[:o.len], nil
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
		return buf[:o.len], nil
	}
	return nil, fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: unsupported ctype %d for stored procedure OUTPUT parameter value at index %d", o.ctype, o.idx)
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
