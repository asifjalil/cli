package cli_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/asifjalil/cli"
)

func Example() {
	ExampleOpen()
	ExampleLoad()
	// Skip ExampleProc because the output is not always the same.
	ExampleProc()

	//Output:
	//1.1
	//Hello
	//World
}

func ExampleOpen() {
	var val float64

	connStr := "SQLConnect; Database = Sample;" // trailing semi-colon is required
	qry := "SELECT double(1.1) FROM sysibm.sysdummy1"

	log.Println(strings.Repeat("#", 30))
	log.Println("Shows how to connect, query, and disconnect from a DB2 database using the \"cli\" driver.")
	log.Printf("sql.Open(\"cli\",\"%s\")\n", connStr)
	db, err := sql.Open("cli", connStr)
	checkError(err)
	defer db.Close()

	log.Println("Connected...")

	// run a dummy query
	log.Printf("db.QueryRow(\"%s\").Scan(&val)\n", qry)
	err = db.QueryRow(qry).Scan(&val)
	checkError(err)
	log.Println(val)
	fmt.Println(val)

	log.Println("Disconnecting...")
	log.Printf("db.Close()")
}

func ExampleCurrentSchema() {
	connStr := "DSN = sample; CurrentSchema = SYSIBM"
	qry := "Select 100 from systables fetch first 1 row only"

	log.Println(strings.Repeat("#", 30))
	log.Println("Shows how to set current schema in the connection string")

	log.Printf("sql.Open(\"cli\",\"%s\")\n", connStr)
	db, err := sql.Open("cli", connStr)
	checkError(err)
	defer db.Close()

	var val int
	err = db.QueryRow(qry).Scan(&val)
	checkError(err)

	log.Printf("Expecting 100, got %v\n", val)
	fmt.Println(val)
	// Output: 100
}

func ExampleProc() {
	connStr := "DSN = sample"
	var (
		snapTime   time.Time
		dbsize     int64
		dbcapacity int64
	)

	procStmt := "call sysproc.get_dbsize_info(?, ?, ?, 0)"
	db, err := sql.Open("cli", connStr)
	checkError(err)
	defer db.Close()
	log.Println(strings.Repeat("#", 30))
	log.Println("Shows how to use a stored procedure with OUTPUT parameters")
	log.Printf("Running %q\n", procStmt)
	_, err = db.ExecContext(context.Background(), procStmt,
		sql.Out{Dest: &snapTime},
		sql.Out{Dest: &dbsize},
		sql.Out{Dest: &dbcapacity})
	checkError(err)
	log.Printf("snapshot time: %v, dbsize: %d, dbcapacity: %d\n", snapTime, dbsize, dbcapacity)
	log.Println("success ...")
}

func ExampleLoad() {
	tabname := "loadtable"
	createStmt := fmt.Sprintf("CREATE TABLE %s (Col1 VARCHAR(30))", tabname)
	dropStmt := fmt.Sprintf("DROP TABLE %s", tabname)
	connStr := "SQLConnect; Database = Sample;"

	log.Println(strings.Repeat("#", 30))
	log.Println("Shows how to load a table using SYSPROC.ADMIN_CMD and the \"cli\" driver.")

	tmpflName := prepData(tabname)

	db, err := sql.Open("cli", connStr)
	checkError(err)
	defer os.Remove(tmpflName)
	defer db.Close()

	// setup
	log.Println("Table to load: ", createStmt)
	_, err = db.Exec(createStmt)
	checkError(err)

	// load
	var (
		rows_read, rows_skipped,
		rows_loaded, rows_rejected, rows_deleted,
		rows_committed, rows_partitioned, num_agentinfo_entries sql.NullInt64

		msg_retrieval, msg_removal sql.NullString
	)
	admin_cmd := fmt.Sprintf("CALL SYSPROC.ADMIN_CMD('LOAD FROM %s"+
		" OF DEL REPLACE INTO %s NONRECOVERABLE')", tmpflName, tabname)
	log.Println("load command: ", admin_cmd)

	rows, err := db.Query(admin_cmd)
	checkError(err)

	// only get the first result set
	rows.Next()

	err = rows.Scan(&rows_read, &rows_skipped, &rows_loaded,
		&rows_rejected, &rows_deleted, &rows_committed,
		&rows_partitioned, &num_agentinfo_entries,
		&msg_retrieval, &msg_removal)
	checkError(err)

	log.Println("Rows Read     : ", rows_read.Int64)
	log.Println("Rows Skipped  : ", rows_skipped.Int64)
	log.Println("Rows Loaded   : ", rows_loaded.Int64)
	log.Println("Rows Rejected : ", rows_rejected.Int64)
	log.Println("Rows Deleted  : ", rows_deleted.Int64)
	log.Println("Rows Committed: ", rows_committed.Int64)
	log.Println("Msg Retrieval : ", msg_retrieval.String)
	log.Println("Msg Removal   : ", msg_removal.String)

	err = rows.Close()
	checkError(err)

	// check the data
	var col1 string
	selectStmt := fmt.Sprintf("SELECT Col1 FROM %s", tabname)
	log.Println("select: ", selectStmt)
	rows, err = db.Query(selectStmt)
	checkError(err)
	for rows.Next() {
		err = rows.Scan(&col1)
		checkError(err)
		log.Println(col1)
		fmt.Println(col1)
	}
	checkError(rows.Err())
	err = rows.Close()
	checkError(err)

	// cleanup
	log.Println("Cleanup: ", dropStmt)
	_, err = db.Exec(dropStmt)
	checkError(err)
}

func prepData(filePrefix string) string {
	wContents := []byte("Hello\nWorld\n")

	tmpfile, err := ioutil.TempFile("", filePrefix)
	checkError(err)

	// write sample data for loading
	_, err = tmpfile.Write(wContents)
	checkError(err)

	// close filehandler
	err = tmpfile.Close()
	checkError(err)

	return tmpfile.Name()
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
