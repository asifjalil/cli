package cli

/*
#include <sqlcli1.h>
*/
import "C"
import (
	"database/sql/driver"
	"fmt"
	"strings"
	"unsafe"
)

// CLIError represents an error from a IBM DB2 Database Manager.
//
// SQLCode is a return code from a IBM DB2 SQL operation.
// This code can be zero (0), negative, or positive.
//	0 means successful execution.
//	Negative means unsuccessful execution with an error.
//	For example -911 means a lock timeout occurred with a rollback.
//	Positive means successful execution with a warning.
//	For example +100 means no rows found or end of table.
//
// SQLState is a return code like SQLCode.
// But instead of a number, it is a five character error code that is consistent across all IBM database products.
// SQLState follows this format: ccsss, where cc indicates class and sss indicates subclass.
// Search "SQLSTATE Messages" in DB2 Information Center for more detail.
//
// Message is a text that explains the error code.
type cliError struct {
	sqlcode  int
	sqlstate string
	message  string
}

// Error returns the Message from CLIError.
// The text includes SQLCode, SQLState, and a error message.
func (e *cliError) Error() string {
	return fmt.Sprintf("database/sql/driver: %s", e.message)
}

func success(ret C.SQLRETURN) bool {
	return int(ret) == C.SQL_SUCCESS
}

func formatError(ht C.SQLSMALLINT, h C.SQLHANDLE) error {
	sqlState := make([]uint16, 6)
	var sqlCode C.SQLINTEGER
	messageText := make([]uint16, C.SQL_MAX_MESSAGE_LENGTH)
	var textLength C.SQLSMALLINT
	err := &cliError{}
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
			err.sqlstate = utf16ToString(sqlState)
			err.sqlcode = int(sqlCode)
		}
		err.message += utf16ToString(messageText)
	}
	if err.message != "" {
		err.message = strings.TrimSpace(err.message)
	}

	// https://www.ibm.com/support/knowledgecenter/en/SSEPGG_11.1.0/com.ibm.db2.luw.messages.cli.doc/com.ibm.db2.luw.messages.cli.doc-gentopic1.html
	if strings.Contains(err.message, "CLI0106E") ||
		strings.Contains(err.message, "CLI0107E") ||
		strings.Contains(err.message, "CLI0108E") ||
		// http://www-01.ibm.com/support/docview.wss?uid=swg21164785
		strings.Contains(err.message, "SQL30081N") {
		return driver.ErrBadConn
	}

	return err
}

func (e *cliError) SQLState() string {
	return e.sqlstate
}
func (e *cliError) SQLCode() int {
	return e.sqlcode
}

type rowsAffectedError struct {
	rowsAffected int64
}

func (ra *rowsAffectedError) Error() string {
	return fmt.Sprintf("asifjalil/cli: number of rows affected %d", ra.rowsAffected)
}

// RowsAffected returns the number of rows affected by an
// update, insert, merge, or delete. -1 indicates a statement
// other than update, insert, delete, or a merge.
func (ra *rowsAffectedError) RowsAffected() int64 {
	return ra.rowsAffected
}
