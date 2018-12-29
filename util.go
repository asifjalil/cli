package cli

/*
#include <sqlcli1.h>
*/
import "C"

// https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.cli.doc/doc/r0000526.html
func sqlTypeToCType(sqltype C.SQLSMALLINT) C.SQLSMALLINT {
	var ctype C.SQLSMALLINT = C.SQL_C_DEFAULT

	switch sqltype {
	case C.SQL_BIGINT:
		ctype = C.SQL_C_SBIGINT
	case C.SQL_BLOB:
	case C.SQL_BINARY:
	case C.SQL_BIT:
	case C.SQL_LONGVARBINARY:
	case C.SQL_VARBINARY:
	case C.SQL_XML:
		ctype = C.SQL_C_BINARY
	case C.SQL_BLOB_LOCATOR:
		ctype = C.SQL_C_BLOB_LOCATOR
	case C.SQL_CHAR:
	case C.SQL_CLOB:
	case C.SQL_LONGVARCHAR:
	case C.SQL_VARCHAR:
		ctype = C.SQL_C_CHAR
	case C.SQL_TINYINT:
		ctype = C.SQL_C_TINYINT
	case C.SQL_CLOB_LOCATOR:
		ctype = C.SQL_C_CLOB_LOCATOR
	case C.SQL_CURSORHANDLE:
		ctype = C.SQL_C_CURSORHANDLE
	case C.SQL_TYPE_DATE:
		ctype = C.SQL_C_TYPE_DATE
	case C.SQL_TYPE_TIME:
		ctype = C.SQL_C_TYPE_TIME
	case C.SQL_TYPE_TIMESTAMP:
		ctype = C.SQL_C_TYPE_TIMESTAMP
	case C.SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
		ctype = C.SQL_C_TYPE_TIMESTAMP_EXT_TZ
	case C.SQL_DBCLOB:
	case C.SQL_LONGVARGRAPHIC:
	case C.SQL_WLONGVARCHAR:
	case C.SQL_VARGRAPHIC:
	case C.SQL_WVARCHAR:
	case C.SQL_GRAPHIC:
		ctype = C.SQL_C_DBCHAR
	case C.SQL_DBCLOB_LOCATOR:
		ctype = C.SQL_C_DBCLOB_LOCATOR
	case C.SQL_DECIMAL:
	case C.SQL_DECFLOAT:
	case C.SQL_NUMERIC:
		ctype = C.SQL_C_CHAR
	case C.SQL_DOUBLE:
		ctype = C.SQL_C_DOUBLE
	case C.SQL_INTEGER:
		ctype = C.SQL_C_LONG
	case C.SQL_REAL:
		ctype = C.SQL_C_FLOAT
	case C.SQL_WCHAR:
		ctype = C.SQL_C_WCHAR
	}
	return ctype
}
