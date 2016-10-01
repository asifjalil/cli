// Package cli provides access to the DB2 database using DB2 Call Level Interface (CLI).
//
// The package has no exported API.
// It registers a driver for the standard Go database/sql package.
//
//	import _ "github.com/asifjalil/cli"
package cli

/*
#cgo LDFLAGS: -ldb2

#include <sqlcli1.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// CliError is an error from a DB2 CLI function call.
type CliError struct {
	SqlState string
	SqlCode  int
	Message  string
}

func (e *CliError) Error() string {
	return fmt.Sprintf("database/sql/driver: [asifjalil][CLI driver]:[ SQLCODE: %d|SQLSTATE: %s ] - %s", e.SqlCode, e.SqlState, e.Message)
}

func success(ret C.SQLRETURN) bool {
	return int(ret) == C.SQL_SUCCESS || int(ret) == C.SQL_SUCCESS_WITH_INFO
}

func formatError(ht C.SQLSMALLINT, h C.SQLHANDLE) (err *CliError) {
	sqlState := make([]uint16, 6)
	var sqlCode C.SQLINTEGER
	messageText := make([]uint16, C.SQL_MAX_MESSAGE_LENGTH)
	var textLength C.SQLSMALLINT
	err = &CliError{}
	for i := 1; ; i++ {
		ret := C.SQLGetDiagRecW(C.SQLSMALLINT(ht),
			h,
			C.SQLSMALLINT(i),
			(*C.SQLWCHAR)(unsafe.Pointer(&sqlState[0])),
			&sqlCode,
			(*C.SQLWCHAR)(unsafe.Pointer(&messageText[0])),
			C.SQL_MAX_MESSAGE_LENGTH,
			&textLength)

		if ret == C.SQL_INVALID_HANDLE || ret == C.SQL_NO_DATA {
			break
		}
		if i == 1 { // first error message save the SQLSTATE.
			err.SqlState = utf16ToString(sqlState)
			err.SqlCode = int(sqlCode)
		}
		err.Message += utf16ToString(messageText)
	}

	return err
}
