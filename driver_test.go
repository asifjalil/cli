package cli_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/asifjalil/cli"
)

type testDB struct {
	*sql.DB
}

func newTestDB() (*testDB, error) {
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

	db, err := sql.Open("cli", connStr)
	if err != nil {
		return nil, err
	}

	return &testDB{db}, nil
}

func (db *testDB) close() {
	db.DB.Close()
}

func TestScan(t *testing.T) {
	var (
		s1 string
		s2 sql.NullString
		i1 int
		f1 float64
	)

	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()
	err = db.QueryRow("values('hello', NULL, 12345, 12345.6789)").Scan(&s1,
		&s2, &i1, &f1)
	switch {
	case err != nil:
		t.Error(err)
	case s1 != "hello" || s2.String != "" || i1 != 12345 ||
		f1 != 12345.6789:
		t.Errorf("Expected: s1:\"hello\", s2:\"\", i1: 12345, f1: 12345.6789|Got: s1:%s, s2:%s, i1: %d, f1: %f",
			s1, s2.String, i1, f1)
	default:
		t.Log("All Ok!")
	}
}

func TestTimeStamp(t *testing.T) {
	// Database timestamp accuracy is up to a microsecond or 6 digits.
	// But Go timestamp accuracy is up to a nanosecond or 9 digits.
	// So the last 3 digits in 9 digits must be 0.
	ts := time.Date(2009, time.November, 10, 23, 6, 29, 10011001000, time.UTC)
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	// start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// insert value
	_, err = tx.Exec(`INSERT INTO in_tray(received, source, subject, note_text) 
		VALUES(?, ?, ?, ?)`, ts, "TEST", nil, nil)
	if err != nil {
		t.Error(err)
	}

	// check that the data is in the table
	var db_ts time.Time
	err = tx.QueryRow("SELECT received FROM in_tray WHERE source = ?",
		"TEST").Scan(&db_ts)

	switch {
	case err == sql.ErrNoRows:
		t.Error("No new timestamp in table IN_TRAY - insert didn't work")
	case err != nil:
		t.Error(err)
	default:
		// Timestamps are stored as is but without the timezone information.
		// When a timestamp is returned from the database, the driver/Go assumes
		// local timezone.
		t.Log("database timestamp with local timezone:", db_ts)
		// In this case we used UTC, so change the local timezone to UTC.
		db_ts = time.Date(db_ts.Year(),
			db_ts.Month(),
			db_ts.Day(),
			db_ts.Hour(),
			db_ts.Minute(),
			db_ts.Second(),
			db_ts.Nanosecond(),
			time.UTC)
		if !ts.Equal(db_ts) {
			t.Error("Expected: ", ts, "|Got:", db_ts)
		}
	}

	// cleanup
	err = tx.Rollback()
	if err != nil {
		t.Error(err)
	}
}

func TestXML(t *testing.T) {
	testCases := []struct {
		qry string
		val string
		got string
	}{
		{qry: `SELECT info FROM Customer c
		WHERE XMLEXISTS('$INFO//addr[pcode-zip = $zip]'
			passing c.INFO as "d",
			CAST(? AS VARCHAR(128)) AS "zip") `, val: "M6W 1E6"},

		{qry: `SELECT XMLQUERY ('$d/customerinfo/addr' passing c.INFO as "d")
		FROM Customer as c
		WHERE XMLEXISTS('$d//addr[city=$cityName]'
			passing c.INFO as "d",
			CAST (? AS VARCHAR(128)) AS "cityName")`, val: "Aurora"},
	}

	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Testcase %d", i), func(t *testing.T) {
			err := db.QueryRow(tc.qry, tc.val).Scan(&tc.got)
			switch {
			case err == sql.ErrNoRows:
				t.Log("No rows for query: ", tc.qry)
			case err != nil:
				t.Error(err)
			default:
				t.Log(tc.got)
			}
		})
	}
}

func TestLob(t *testing.T) {
	testCases := []struct {
		qry string
		val []string
		got []byte
	}{
		{qry: `SELECT picture
				FROM emp_photo
				WHERE empno  = ? AND photo_format = ?`, val: []string{"000140", "bitmap"}},
		{qry: `SELECT resume
                FROM emp_resume
				WHERE empno = ? AND resume_format = ?`, val: []string{"000140", "ascii"}},
	}

	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Testcase %d", i), func(t *testing.T) {
			err := db.QueryRow(tc.qry, tc.val[0], tc.val[1]).Scan(&tc.got)
			switch {
			case err == sql.ErrNoRows:
				t.Log("No match for query: ", tc.qry)
			case err != nil:
				t.Error(err)
			default:
				t.Logf("Got a LOB value of %d bytes!", len(tc.got))
			}
		})
	}
}
