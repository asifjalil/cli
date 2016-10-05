package cli_test

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/asifjalil/cli"
)

// Open connects to a database using CLI function SQLDriverConnect or SQLConnect.
//
// To use SQLConnect, start the dsn with keyword sqlconnect. This keyword is case insensitive.
// The connection string needs to follow this syntax to be valid:
//
//	sqlconnect;[DATABASE=<database_name>;][UID=<user_id>;][PWD=<password>;]
//
// [...] means optional. If a database_name is not provided, then SAMPLE is
// used as the database name. Also note that each keyword and value ends with
// a semicolon. The keyword "sqlconnect" doesn't take a value but ends with a semi-colon.
//
// Any other connection string must follow the connection string rule that is
// valid with SQLDriverConnect. For example, this is a valid dsn/connection string
// for SQLDriverConnect:
//
//	DSN=Sample; UID=asif; PWD=secrect; AUTOCOMMIT=0; CONNECTTYPE=1;
//
// Search SQLDriverConnect in DB2 LUW Information Center for more detail.
// ExampleOpen demonstrates how to connect and disconnect from DB2 database.
func Example_Open() {
	var val float64

	connStr := "SQLConnect; Database = Sample;" // trailing semi-colon is required
	qry := "SELECT double(1.1) FROM sysibm.sysdummy1"

	log.Println("Shows how to connect, query, and disconnect from a DB2 database using the \"cli\" driver")
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
	// Output: 1.1
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
