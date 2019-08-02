package cli

/*
#include <sqlcli1.h>
*/
import "C"
import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

type column struct {
	h                C.SQLHSTMT
	idx              int    // column position; starts from 0
	name             string // column name
	databaseTypeName string // column type name
	nullable         bool   // true if the column value can be null; otherwise false
	size             int64  // precision of column
	scale            int64  // scale of column
	sqltype          C.SQLSMALLINT
	ctype            C.SQLSMALLINT
	data             []byte   // data returned from the database
	len              C.SQLLEN // StrLen_or_IndPtr; Indicates the size of the data fetched into data
}

func (c *column) getData() ([]byte, error) {
	/*
		https://www.ibm.com/support/knowledgecenter/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.cli.doc/doc/r0000604.html

		Upon each SQLGetData() function call, if the data available for return is greater than or equal to the
		BufferLength argument value, the data truncation occurs. Truncation is indicated by a function return code of
		SQL_SUCCESS_WITH_INFO coupled with an SQLSTATE denoting data truncation. The application can call the
		SQLGetData() function again, with the same ColumnNumber value, to get subsequent data from the same
		unbound column starting at the point of truncation. To obtain the entire column, the application repeats
		such calls until the function returns SQL_SUCCESS. The next call to the SQLGetData() function returns SQL_NO_DATA_FOUND.
	*/

	var total []byte
	buf := make([]byte, 1024)

loop:
	for {
		/*
			get len(buf) bytes at a time.
			After C.SQLGetData is called, c.len indicates number of bytes remaining.
			It needs to be changed to "total" size at the end because
			we return data as buf[:c.len]. If c.len is the remaining bytes
			then we will truncate the data.
		*/
		ret := C.SQLGetData(c.h,
			C.SQLUSMALLINT(c.idx+1), c.ctype,
			C.SQLPOINTER(unsafe.Pointer(&buf[0])), C.SQLLEN(len(buf)),
			&c.len)
		switch ret {
		case C.SQL_SUCCESS:
			if c.len == C.SQL_NULL_DATA {
				return nil, nil
			}
			total = append(total, buf[:c.len]...)
			break loop
		case C.SQL_SUCCESS_WITH_INFO:
			err := formatError(C.SQL_HANDLE_STMT, C.SQLHANDLE(c.h))
			if cliErr, ok := err.(*cliError); ok && cliErr.SQLState() != "01004" {
				return nil, err
			}
			// buf is not big enough; data has been truncated
			// save the partial data
			total = append(total, buf...)
		default:
			return nil, formatError(C.SQL_HANDLE_STMT, C.SQLHANDLE(c.h))
		}
	}
	// set c.len to total size to avoid truncation
	c.len = C.SQLLEN(len(total))
	return total, nil
}

func (c *column) typeName() string {
	switch c.sqltype {
	case C.SQL_BIT:
		return "BIT"
	case C.SQL_TINYINT, C.SQL_SMALLINT:
		return "SMALLINT"
	case C.SQL_INTEGER:
		return "INTEGER"
	case C.SQL_BIGINT:
		return "BIGINT"
	case C.SQL_DOUBLE:
		return "DOUBLE"
	case C.SQL_DECIMAL:
		return "DECIMAL"
	case C.SQL_NUMERIC:
		return "NUMERIC"
	case C.SQL_FLOAT:
		return "FLOAT"
	case C.SQL_REAL:
		return "REAL"
	case C.SQL_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case C.SQL_TYPE_DATE:
		return "DATE"
	case C.SQL_TYPE_TIME:
		return "TIME"
	case C.SQL_CHAR, C.SQL_WCHAR:
		return "CHARACTER"
	case C.SQL_VARCHAR, C.SQL_WVARCHAR:
		return "VARCHAR"
	case C.SQL_CLOB:
		return "CLOB"
	case C.SQL_BLOB:
		return "BLOB"
	case C.SQL_BINARY:
		return "BINARY"
	case C.SQL_VARBINARY:
		return "VARBINARY"
	case C.SQL_XML:
		return "XML"
	case C.SQL_DECFLOAT:
		return "DECFLOAT"
	default:
		return "UNKNOWN"
	}
}

func (c *column) typeNullable() (nullable, ok bool) {
	return c.nullable, true
}

func (c *column) typePrecisionScale() (precision, scale int64, ok bool) {
	return c.size, c.scale, c.scale > 0
}

func (c *column) typeLength() (length int64, ok bool) {
	switch c.sqltype {
	case C.SQL_VARCHAR, C.SQL_WVARCHAR, C.SQL_CLOB, C.SQL_BLOB,
		C.SQL_VARBINARY, C.SQL_XML:
		ok = true
	}
	return c.size, ok
}

func (c *column) scanType() reflect.Type {
	switch c.ctype {
	case C.SQL_C_BIT:
		return reflect.TypeOf(false)
	case C.SQL_C_LONG:
		return reflect.TypeOf(int32(0))
	case C.SQL_C_SBIGINT:
		return reflect.TypeOf(int64(0))
	case C.SQL_C_DOUBLE:
		return reflect.TypeOf(float64(0.0))
	case C.SQL_C_CHAR, C.SQL_C_WCHAR:
		// default C type for DECFLOAT is CHAR
		// That CHAR needs to be converted to float64
		if c.sqltype == C.SQL_DECFLOAT {
			return reflect.TypeOf(float64(0.0))
		}
		return reflect.TypeOf(string(""))
	case C.SQL_C_TYPE_DATE, C.SQL_C_TYPE_TIME, C.SQL_C_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case C.SQL_C_BINARY:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf(new(interface{}))
	}
}

func (c *column) value() (driver.Value, error) {
	var p unsafe.Pointer
	var err error
	buf := c.data

	// nil slice b/c SQLBindColumn is not supported for this column.
	// Need to use SQLGetData to fetch the data from the database.
	if len(buf) == 0 {
		buf, err = c.getData()
		if err != nil {
			return nil, err
		}
	}

	// c.len is set after calling SQLFetch
	if c.len == C.SQL_NULL_DATA {
		return nil, nil
	}
	p = unsafe.Pointer(&buf[0])

	switch c.ctype {
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
		if c.sqltype == C.SQL_DECFLOAT || c.sqltype == C.SQL_DECIMAL || c.sqltype == C.SQL_NUMERIC {
			s := string(buf[:c.len])
			f, err := strconv.ParseFloat(s, 64)
			return f, err
		}
		return buf[:c.len], nil
	case C.SQL_C_WCHAR, C.SQL_C_DBCHAR:
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
		return buf[:c.len], nil
	}
	return nil, fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: unsupported column ctype %d", c.ctype)
}

func describeColumn(h C.SQLHSTMT, idx int, namebuf []uint16) (namelen int,
	sqltype C.SQLSMALLINT, size C.SQLULEN, ret C.SQLRETURN, nullOK bool, scale C.SQLSMALLINT) {
	var l, nullable C.SQLSMALLINT

	ret = C.SQLDescribeColW(h, C.SQLUSMALLINT(idx+1),
		(*C.SQLWCHAR)(unsafe.Pointer(&namebuf[0])),
		C.SQLSMALLINT(len(namebuf)),
		&l, &sqltype, &size, &scale, &nullable)

	return int(l), sqltype, size, ret, nullable == C.SQL_NULLABLE, scale
}

func newColumn(h C.SQLHSTMT, idx int) (*column, error) {
	namebuf := make([]uint16, 150)
	namelen, sqltype, size, ret, nullable, scale := describeColumn(h, idx, namebuf)
	if ret == C.SQL_SUCCESS_WITH_INFO && namelen > len(namebuf) {
		namebuf = make([]uint16, namelen)
		namelen, sqltype, size, ret, nullable, scale = describeColumn(h, idx, namebuf)
	}
	if !success(ret) {
		return nil, formatError(C.SQL_HANDLE_STMT, C.SQLHANDLE(h))
	}
	col := &column{
		h:        h,
		idx:      idx,
		name:     utf16ToString(namebuf[:namelen]),
		nullable: nullable,
		size:     int64(size),
		scale:    int64(scale),
	}

	// [set column C-Type and allocate byte buffer to hold value from the database]
	col.sqltype = sqltype
	switch sqltype {
	case C.SQL_BIT:
		col.ctype = C.SQL_C_BIT
		col.data = make([]byte, 1)
	case C.SQL_TINYINT, C.SQL_SMALLINT, C.SQL_INTEGER:
		col.ctype = C.SQL_C_LONG
		col.data = make([]byte, 4)
	case C.SQL_BIGINT:
		col.ctype = C.SQL_C_SBIGINT
		col.data = make([]byte, 8)
	case C.SQL_NUMERIC, C.SQL_DECIMAL, C.SQL_FLOAT, C.SQL_REAL, C.SQL_DOUBLE:
		col.ctype = C.SQL_C_DOUBLE
		col.data = make([]byte, 8)
	case C.SQL_TYPE_TIMESTAMP:
		var v sql_TIMESTAMP_STRUCT
		col.ctype = C.SQL_C_TYPE_TIMESTAMP
		col.data = make([]byte, int(unsafe.Sizeof(v)))
	case C.SQL_TYPE_DATE:
		var v sql_DATE_STRUCT
		col.ctype = C.SQL_C_TYPE_DATE
		col.data = make([]byte, int(unsafe.Sizeof(v)))
	case C.SQL_TYPE_TIME:
		var v sql_TIME_STRUCT
		col.ctype = C.SQL_C_TYPE_TIME
		col.data = make([]byte, int(unsafe.Sizeof(v)))
	case C.SQL_CHAR, C.SQL_VARCHAR, C.SQL_CLOB, C.SQL_DECFLOAT:
		// According to _SQL symbolic and default data types for CLI applications_
		// in DB2 IBM Knowledge Center, default C type for SQL_DECFLOAT is CHAR
		// https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.cli.doc/doc/r0000526.html
		l := int(size)
		l += 1 // room for null-termination character
		col.ctype = C.SQL_C_CHAR
		col.data = make([]byte, l)
	case C.SQL_WCHAR, C.SQL_WVARCHAR:
		l := int(size)
		l += 1 // for null-termination character
		l *= 2 // wchars are 2 bytes each
		col.ctype = C.SQL_C_WCHAR
		col.data = make([]byte, l)
	case C.SQL_BINARY, C.SQL_VARBINARY, C.SQL_BLOB:
		col.ctype = C.SQL_C_BINARY
		col.data = make([]byte, size)
	case C.SQL_XML:
		col.ctype = C.SQL_C_BINARY
		// Size is 0, so can't allocate a byte buffer for SQLBindCol.
		// Check out "Data type length (CLI) table" in DB2 Information Center
		// http://www.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/com.ibm.db2.luw.apdv.cli.doc/doc/r0006844.html
	default:
		return nil, fmt.Errorf("database/sql/driver: [asifjalil][CLI Driver]: unsupported database column type %d", sqltype)
	}
	// only use SQLBindCol if we were able to allocate a byte buffer for the column
	if len(col.data) > 0 {
		ret = C.SQLBindCol(h, C.SQLUSMALLINT(idx+1),
			col.ctype, C.SQLPOINTER(unsafe.Pointer(&col.data[0])),
			C.SQLLEN(len(col.data)), &col.len)
		if !success(ret) {
			return nil, formatError(C.SQL_HANDLE_STMT, C.SQLHANDLE(h))
		}
	}
	return col, nil
}
