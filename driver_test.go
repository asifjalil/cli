package cli_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/asifjalil/cli"
)

type testDB struct {
	*sql.DB
}

func newTestDB() *testDB {
	config := struct {
		database string
		uid      string
		pwd      string
	}{
		database: "sample",
		uid:      "",
		pwd:      "",
	}

	if os.Getenv("DATABASE_NAME") != "" {
		config.database = os.Getenv("DATABASE_NAME")
	}

	if os.Getenv("DATABASE_USER") != "" {
		config.uid = os.Getenv("DATABASE_USER")
	}

	if os.Getenv("DATABASE_PASSWORD") != "" {
		config.pwd = os.Getenv("DATABASE_PASSWORD")
	}

	connStr := fmt.Sprintf("sqlconnect;DATABASE = %s; UID = %s; PWD = %s",
		config.database, config.uid, config.pwd)

	log.Println("Connecting to the database ...")
	log.Printf("connect str: %s\n", connStr)
	db, err := sql.Open("cli", connStr)
	if err != nil {
		panic(err)
	}

	return &testDB{db}
}

func (db *testDB) close() {
	log.Println("Disconnecting from the database ...")
	db.DB.Close()
}

func TestQueryRow(t *testing.T) {
	cid := 1000
	var info string

	db := newTestDB()
	defer db.close()

	err := db.QueryRow("SELECT info FROM customer where cid=?",
		cid).Scan(&info)
	switch {
	case err == sql.ErrNoRows:
		t.Logf("No information for cid: %d", cid)
	case err != nil:
		t.Error(err)
	default:
		t.Log(info)
	}
}
