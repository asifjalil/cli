package cli

/*
#include <sqlcli1.h>
*/
import "C"
import (
	"unsafe"
)

// param is for input type parameter only.
// param is used to store the following:
// - a reference to the buffer that holds
//   the input value
// - length of the input value
//
// We do this so Go GC doesn't remove the
// buffer between bind and execution.
// See issue #12
type param struct {
	plen *C.SQLLEN
	buf  unsafe.Pointer
}
