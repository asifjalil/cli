package cli

/*
#include <sqlcli1.h>
*/
import "C"

type tx struct {
	c *conn
}

func (t *tx) Commit() error {
	if t.c == nil {
		panic("database/sql/driver: [asifjalil][CLI Driver]: extra Commit")
	}
	err := t.c.endTx(true)
	t.c = nil

	return err
}

func (t *tx) Rollback() error {
	if t.c == nil {
		panic("database/sql/driver: [asifjalil][CLI Driver]: extra Rollback")
	}
	err := t.c.endTx(false)
	t.c = nil

	return err
}
