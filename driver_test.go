package cli_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/asifjalil/cli"
)

type testDB struct {
	*sql.DB
}

func getDB2Error(sqlerr error) (int, string, bool) {
	type sqlcode interface {
		SQLCode() int
		SQLState() string
	}

	if err, ok := sqlerr.(sqlcode); ok {
		return err.SQLCode(), err.SQLState(), ok
	}

	return 0, "", false
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

	connStr := fmt.Sprintf("DATABASE = %s; UID = %s; PWD = %s;",
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
		die(t, "failed because %v", err)
	}
	defer db.close()
	err = db.QueryRowContext(context.Background(), "values('hello', NULL, 12345, 12345.6789)").Scan(&s1,
		&s2, &i1, &f1)
	switch {
	case err != nil:
		die(t, "error: %v", err)
	case s1 != "hello" || s2.String != "" || i1 != 12345 ||
		f1 != 12345.6789:
		die(t, "Expected: s1:\"hello\", s2:\"\", i1: 12345, f1: 12345.6789|Got: s1:%s, s2:%s, i1: %d, f1: %f",
			s1, s2.String, i1, f1)
	default:
		info(t, "All Ok!")
	}
}

func TestTimeStamp(t *testing.T) {
	// Database timestamp accuracy is up to a microsecond or 6 digits.
	// But Go timestamp accuracy is up to a nanosecond or 9 digits.
	// So the last 3 digits in 9 digits must be 0.
	ts := time.Date(2009, time.November, 10, 23, 6, 29, 10011001000, time.UTC)
	db, err := newTestDB()
	if err != nil {
		die(t, "failed to create db object: %v", err)
	}
	defer db.close()

	// start transaction
	tx, err := db.Begin()
	if err != nil {
		die(t, "transaction begin failed because: %v", err)
	}

	// insert value
	_, err = tx.Exec(`INSERT INTO in_tray(received, source, subject, note_text) 
		VALUES(?, ?, ?, ?)`, ts, "TEST", nil, nil)
	if err != nil {
		die(t, "insert failed because %v", err)
	}

	_, err = tx.ExecContext(context.Background(), `INSERT INTO in_tray(received, source, subject, note_text) 
		VALUES(?, ?, ?, ?)`, ts, "TEST", nil, nil)
	if err != nil {
		die(t, "insert failed because %v", err)
	}

	// check that the data is in the table
	var db_ts time.Time
	err = tx.QueryRow("SELECT received FROM in_tray WHERE source = ?",
		"TEST").Scan(&db_ts)

	switch {
	case err == sql.ErrNoRows:
		die(t, "No new timestamp in table IN_TRAY - insert didn't work")
	case err != nil:
		die(t, "insert into IN_TRAY failed because %v", err)
	default:
		// Timestamps are stored as is but without the timezone information.
		// When a timestamp is returned from the database, the driver/Go assumes
		// local timezone.
		info(t, "database timestamp with local timezone: %v", db_ts)
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
			die(t, "Expected: %v| Got: %v", ts, db_ts)
		}
	}

	// cleanup
	err = tx.Rollback()
	if err != nil {
		die(t, "rollback failed because %v", err)
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

// for issue #8
func TestLgXML(t *testing.T) {
	tabname := "testxml"
	delFile := "_TEST/large.del"
	xmlFile := "_TEST/large.xml"
	createStmt := fmt.Sprintf("CREATE TABLE %s (Col1 XML)", tabname)
	dropStmt := fmt.Sprintf("DROP TABLE %s", tabname)
	queryStmt := fmt.Sprintf("SELECT col1 FROM %s", tabname)

	if home := os.Getenv("DATABASE_HOMEDIR"); home != "" {
		delFile = home + "/" + delFile
	} else if dir, err := os.Getwd(); err != nil {
		die(t, "failed to lookup current directory: %v", err)
	} else {
		delFile = dir + "/" + delFile
	}

	if dir, err := os.Getwd(); err != nil {
		die(t, "failed to lookup current directory: %v", err)
	} else {
		xmlFile = dir + "/" + xmlFile
	}

	b, err := ioutil.ReadFile(xmlFile)
	if err != nil {
		die(t, "Failed to read xml from file %s: %v", xmlFile, err)
	}
	wantXML := string(b)

	importStmt := fmt.Sprintf("CALL SYSPROC.ADMIN_CMD('IMPORT FROM %s"+
		" OF DEL XMLPARSE PRESERVE WHITESPACE REPLACE INTO %s')", delFile, tabname)
	db, err := newTestDB()
	if err != nil {
		die(t, "Failed to connect to database: %v", err)
	}
	defer db.close()

	// create test table
	_, err = db.Exec(createStmt)
	if err != nil {
		die(t, "Failed to create table %s: %v", tabname, err)
	}
	defer func() {
		db.Exec(dropStmt)
	}()

	// load xml data
	_, err = db.Exec(importStmt)
	if err != nil {
		die(t, "Failed to run %q: %v", importStmt, err)
	}
	var gotXML string
	err = db.QueryRow(queryStmt).Scan(&gotXML)
	gotXML += "\n"
	switch {
	case err == sql.ErrNoRows:
		die(t, "Expected 1 row; Found 0")
	case err != nil:
		die(t, "error: %v", err)
	case wantXML != gotXML:
		ioutil.WriteFile("want_xml.txt", []byte(wantXML), 0644)
		ioutil.WriteFile("got_xml.txt", []byte(gotXML), 0644)
		t.Error("wantXML doesn't match gotXML")
	default:
		info(t, "All OK")
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

func TestRowsColumnTypes(t *testing.T) {
	testCases := []struct {
		qry             string
		colTypes        []string
		colNullables    []bool
		colIsVarLengths []bool
		colScales       []int64
	}{
		{qry: `SELECT current timestamp, current date, current time, ' A  ', 100, 1.101, cast(NULL as INT), cast(NULL as DECFLOAT)
		FROM sysibm.sysdummy1`,
			colTypes:        []string{"TIMESTAMP", "DATE", "TIME", "VARCHAR", "INTEGER", "DECIMAL", "INTEGER", "DECFLOAT"},
			colNullables:    []bool{false, false, false, false, false, false, true, true},
			colIsVarLengths: []bool{false, false, false, true, false, false, false, false},
			colScales:       []int64{6, 0, 0, 0, 0, 3, 0, 0},
		},
	}
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Testcase %d", i), func(t *testing.T) {
			rows, err := db.QueryContext(context.Background(), tc.qry)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			ct, err := rows.ColumnTypes()
			if err != nil {
				t.Fatalf("ColumnTypes: %v", err)
			}
			for i := range ct {
				colType := ct[i].DatabaseTypeName()
				if colType != tc.colTypes[i] {
					t.Error("Expected ColType: ", tc.colTypes[i], ", Got ColType: ", colType)
				}

				nullable, _ := ct[i].Nullable()
				if nullable != tc.colNullables[i] {
					t.Error("Expected Col Nullability: ", tc.colNullables[i], ", Got Col Nullability: ", nullable)
				}

				length, isVarLength := ct[i].Length()
				if isVarLength != tc.colIsVarLengths[i] {
					t.Error("For column type ", colType, " Expected variable length to be: ", tc.colIsVarLengths[i], ", Got variable length to be: ", isVarLength)
				}

				precision, scale, _ := ct[i].DecimalSize()
				t.Log("Type: ", ct[i].DatabaseTypeName(), ", length(precision): ", length, "(", precision, "), scale: ", scale)

				if scale != tc.colScales[i] {
					t.Error("Expected Col Scale: ", tc.colScales[i], ", Got Col Scale: ", scale)
				}

			}
		})
	}
}

func TestQueryTimeout(t *testing.T) {
	var rc int
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The test works if a separate connecton puts a exclusive lock on a table.
	// For example: db2 +c "LOCK TABLE asif.ACT IN EXCLUSIVE MODE"
	// Then count the number of rows.
	// err = db.QueryRowContext(ctx, "select count(*) from asif.ACT").Scan(&rc)

	// The test doesn't work with SLEEP C UDF.
	// With the C udf SLEEP function DB2 CLI driver doesn't respond to SQLCancel.
	// err = db.QueryRowContext(ctx, "VALUES(SLEEP(60))").Scan(&rc)
	// Use a CPU intensive, naive SQL based sleep function instead.
	err = db.QueryRowContext(ctx, "CALL SLEEP_PROC(60)").Scan(&rc)

	switch {
	case err != nil:
		if sqlcode, sqlstate, ok := getDB2Error(err); ok {
			switch {
			case sqlstate == "42884" || sqlcode == -1646:
				t.Skip("SLEEP function is missing. Skip the test.")
			case sqlstate == "HY008":
				t.Log("All Ok!")
			default:
				t.Errorf("Unexpected CLI error: %s\n", err)
			}
		} else {
			t.Errorf("Expected CLI error with SQLCode and SqlState; instead got this error: %s\n", err)
		}
	default:
		t.Log("Expected the query to fail, but it didn't.")
	}
}

func TestQueryCancel(t *testing.T) {
	var rc int
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	ctx, cancel := context.WithCancel(context.Background())
	// use a goroutine to cancel the query in 5 seconds
	go func(cancel context.CancelFunc) {
		time.Sleep(5 * time.Second)
		cancel()
	}(cancel)

	err = db.QueryRowContext(ctx, "CALL SLEEP_PROC(60)").Scan(&rc)
	switch {
	case err != nil:
		if sqlcode, sqlstate, ok := getDB2Error(err); ok {
			switch {
			case sqlstate == "42884" || sqlcode == -1646:
				t.Skip("SLEEP function is missing. Skip the test.")
			case sqlstate == "HY008":
				t.Log("Query was cancelled as expected.")
			default:
				t.Errorf("Unexpected CLI error: %s\n", err)
			}
		} else if err == context.Canceled {
			// The goroutine may have cancelled the context before the query even started.
			t.Log("Context was cancelled before the query even started. That's expected also.")
		} else {
			t.Errorf("Expected CLI error with SQLCode and SqlState; instead got this error: %s\n", err)
		}
	default:
		t.Log("Expected the query to fail, but it didn't.")
	}
}

func TestTxPrepare(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		die(t, "%s", err)
	}

	stmt, err := tx.PrepareContext(context.Background(), "select 11 from abcd")
	if err == nil {
		stmt.Close()
		die(t, "Expected PrepareContext to fail with SQL0204N and SQLSTATE=42704")
	}
	info(t, "%s", err)
	tx.Commit()
}

func TestTxContext(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	opts := sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  true,
	}
	tx, err := db.BeginTx(context.Background(), &opts)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.PrepareContext(context.Background(), "select count(*) from syscat.tables")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := stmt.Query()
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	stmt.Close()
	tx.Commit()
}

func TestDouble(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	tests := []struct {
		name string
		val  sql.NullString // need to use string to pass smallest/larget double
		want sql.NullFloat64
	}{
		{
			name: "null value",
			val:  sql.NullString{String: "", Valid: false},
			want: sql.NullFloat64{Float64: 0, Valid: false},
		},
		{
			name: "zero value",
			val:  sql.NullString{String: "0.0", Valid: true},
			want: sql.NullFloat64{Float64: 0.0, Valid: true},
		},
		{
			name: "negative zero value",
			val:  sql.NullString{String: "-0.0", Valid: true},
			want: sql.NullFloat64{Float64: -0.0, Valid: true},
		},
		{
			name: "smallest positive",
			val:  sql.NullString{String: "+2.225E-307", Valid: true},
			want: sql.NullFloat64{Float64: +2.225E-307, Valid: true},
		},
		{
			name: "largest positive",
			val:  sql.NullString{String: "+1.79769E+308", Valid: true},
			want: sql.NullFloat64{Float64: +1.79769E+308, Valid: true},
		},
		{
			name: "smallest negative",
			val:  sql.NullString{String: "-1.79769E+308", Valid: true},
			want: sql.NullFloat64{Float64: -1.79769E+308, Valid: true},
		},
		{
			name: "largest negative",
			val:  sql.NullString{String: "-2.225E-307", Valid: true},
			want: sql.NullFloat64{Float64: -2.225E-307, Valid: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullFloat64
			err := db.QueryRow("VALUES (CAST (? AS DOUBLE))", tt.val).Scan(&got)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("want %f, got %f", tt.want.Float64, got.Float64)
			}
		})
	}
}

func TestDecFloat(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	tests := []struct {
		name string
		val  sql.NullString // need to use string to pass smallest/larget decfloat
		want sql.NullFloat64
	}{
		{
			name: "null value",
			val:  sql.NullString{String: "", Valid: false},
			want: sql.NullFloat64{Float64: 0, Valid: false},
		},
		{
			name: "zero value",
			val:  sql.NullString{String: "0.0", Valid: true},
			want: sql.NullFloat64{Float64: 0.0, Valid: true},
		},
		{
			name: "negative zero value",
			val:  sql.NullString{String: "-0.0", Valid: true},
			want: sql.NullFloat64{Float64: -0.0, Valid: true},
		},
		{
			name: "smallest positive",
			val:  sql.NullString{String: "5e-324", Valid: true},
			want: sql.NullFloat64{Float64: math.SmallestNonzeroFloat64, Valid: true},
		},
		{
			name: "largest positive",
			val:  sql.NullString{String: "1.7976931348623157e+308", Valid: true},
			want: sql.NullFloat64{Float64: math.MaxFloat64, Valid: true},
		},
		{
			name: "smallest negative",
			val:  sql.NullString{String: "-5e-324", Valid: true},
			want: sql.NullFloat64{Float64: -math.SmallestNonzeroFloat64, Valid: true},
		},
		{
			name: "largest negative",
			val:  sql.NullString{String: "-1.7976931348623157e+308", Valid: true},
			want: sql.NullFloat64{Float64: -math.MaxFloat64, Valid: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullFloat64
			err := db.QueryRow("VALUES (CAST (? AS DECFLOAT))", tt.val).Scan(&got)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("want %f, got %f", tt.want.Float64, got.Float64)
			}
		})
	}
}

func TestInt(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	tests := []struct {
		name string
		want sql.NullInt64
	}{
		{
			name: "null value",
			want: sql.NullInt64{Int64: 0, Valid: false},
		},
		{
			name: "zero value",
			want: sql.NullInt64{Int64: 0, Valid: true},
		},
		{
			name: "negative zero value",
			want: sql.NullInt64{Int64: -0, Valid: true},
		},
		{
			name: "smallest",
			want: sql.NullInt64{Int64: -2147483648, Valid: true},
		},
		{
			name: "largest",
			want: sql.NullInt64{Int64: 2147483647, Valid: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullInt64
			err := db.QueryRow("VALUES (CAST (? AS INT))", tt.want).Scan(&got)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("wanted %v, got %v", tt.want, got)
			}
		})
	}
}

func TestBigInt(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	tests := []struct {
		name string
		want sql.NullInt64
	}{
		{
			name: "null value",
			want: sql.NullInt64{Int64: 0, Valid: false},
		},
		{
			name: "zero value",
			want: sql.NullInt64{Int64: 0, Valid: true},
		},
		{
			name: "negative zero value",
			want: sql.NullInt64{Int64: -0, Valid: true},
		},
		{
			name: "smallest",
			want: sql.NullInt64{Int64: -9223372036854775808, Valid: true},
		},
		{
			name: "largest",
			want: sql.NullInt64{Int64: 9223372036854775807, Valid: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullInt64
			err := db.QueryRow("VALUES (CAST (? AS BIGINT))", tt.want).Scan(&got)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("wanted %v, got %v", tt.want, got)
			}
		})
	}
}

func TestString(t *testing.T) {
	password := "Pac1f1c"
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	_, err = db.Exec("CREATE TABLE STRINGS(COL1 VARCHAR(50) FOR BIT DATA)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Exec("DROP TABLE STRINGS")
	}()

	tests := []struct {
		name string
		want sql.NullString
	}{
		{
			name: "null value",
			want: sql.NullString{String: "", Valid: false},
		},
		{
			name: "empty value",
			want: sql.NullString{String: "", Valid: true},
		},
		{
			name: "unicode value",
			want: sql.NullString{String: "Hello, 世界", Valid: true},
		},
		{
			name: "alphanumeric value",
			want: sql.NullString{String: " 289-46-8832 AB ", Valid: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullString
			err := db.QueryRow(`SELECT DECRYPT_CHAR(COL1, ?) FROM FINAL TABLE (
			INSERT INTO STRINGS VALUES ENCRYPT(?, ?)
		)`, password, tt.want, password).Scan(&got)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tt.want, got) {
				t.Errorf("wanted %q, got %q\n", tt.want.String, got.String)
			}
		})
	}
}

// for issue #2
// Empty character strings from the db get represented as a byte-slice with all 0s.
func TestEmptyString(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	var emptyString string
	err = db.QueryRow("SELECT '' FROM sysibm.sysdummy1").Scan(&emptyString)
	if err != nil {
		t.Fatal(err)
	}
	if emptyString != "" {
		t.Fatalf("Expected '' got %v\n", emptyString)
	}
	if len([]byte(emptyString)) > 0 {
		t.Fatalf("Expected empty byte slice but got %v\n", []byte(emptyString))
	}
}

type NullByte struct {
	Byte  []byte
	Valid bool
}

func (nb *NullByte) Scan(value interface{}) error {
	if value == nil {
		nb.Byte, nb.Valid = nil, false
		return nil
	}
	if _, ok := value.([]byte); !ok {
		return fmt.Errorf("Unsupported value type %T in NullByte.Scan", value)
	}
	nb.Valid = true
	bv := value.([]byte)
	if nb.Byte == nil {
		nb.Byte = make([]byte, len(bv))
	}
	copy(nb.Byte, bv)
	return nil
}

func (nb NullByte) Value() (driver.Value, error) {
	if !nb.Valid {
		return nil, nil
	}
	return nb.Byte, nil
}

func (nb NullByte) String() string {
	if !nb.Valid {
		return "-"
	}
	return string(nb.Byte)
}

// for issue #2
// Empty VarBinary causes panic.
func TestVarBinary(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	_, err = db.Exec(`CREATE TABLE binaries
	( NAME varchar(64)
	, PASSWORDHASH varbinary(255))`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Exec("DROP TABLE binaries")
	}()

	tests := []struct {
		name string
		want NullByte
	}{
		{
			name: "null varbinary",
			want: NullByte{Byte: nil, Valid: false},
		},
		{
			name: "empty varbinary",
			want: NullByte{Byte: []byte(""), Valid: true},
		},
		{
			name: "regular varbinary",
			want: NullByte{Byte: []byte("myhint\t"), Valid: true},
		},
		{
			name: "unicode varbinary",
			want: NullByte{Byte: []byte("Hello, 世界 \n"), Valid: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got sql.NullString
			err := db.QueryRow(`SELECT HEX(PASSWORDHASH) FROM FINAL TABLE
	(INSERT INTO binaries (NAME, PASSWORDHASH) VALUES (?, ?))`, "ABCD", tt.want).Scan(&got)
			if err != nil {
				t.Error(err)
			}
			decoded, err := hex.DecodeString(got.String)
			if err != nil {
				t.Fatal(err)
			}
			if string(tt.want.Byte) != string(decoded) {
				t.Errorf("want %q, got %q\n", tt.want.Byte, decoded)
			} else {
				t.Logf("want %q, got %q\n", tt.want.Byte, decoded)
			}

		})
	}
}

// Tests that INOUT option works with DB2 Stored Procedure.
func TestSPInOut(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	// for real data type test use float32 instead of float64
	var f32 float32 = 1.99999
	ts := time.Date(2009, time.November, 10, 23, 6, 29, 10011001000, time.UTC)
	createSp := `CREATE OR REPLACE PROCEDURE test_inout(
		INOUT p_a %s)
	LANGUAGE SQL
	SPECIFIC test_inout
	BEGIN
		SET p_a = p_a ;
	END
	 `
	callSp := "call test_inout(?)"
	dropSp := "drop procedure test_inout"
	tests := []struct {
		paramType string
		want      sql.Out
		got       sql.Out
	}{
		{
			paramType: "int",
			want:      sql.Out{Dest: &sql.NullInt64{Valid: false}, In: true},
			got:       sql.Out{Dest: &sql.NullInt64{Valid: false}, In: true},
		},
		{
			paramType: "int",
			want:      sql.Out{Dest: &sql.NullInt64{Int64: -2147483648, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullInt64{Int64: -2147483648, Valid: true}, In: true},
		},
		{
			paramType: "int",
			want:      sql.Out{Dest: &sql.NullInt64{Int64: 2147483647, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullInt64{Int64: 2147483647, Valid: true}, In: true},
		},
		{
			paramType: "bigint",
			want:      sql.Out{Dest: &sql.NullInt64{Int64: -9223372036854775808, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullInt64{Int64: -9223372036854775808, Valid: true}, In: true},
		},
		{
			paramType: "bigint",
			want:      sql.Out{Dest: &sql.NullInt64{Int64: 9223372036854775807, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullInt64{Int64: 9223372036854775807, Valid: true}, In: true},
		},
		{
			paramType: "varchar(1000)",
			want:      sql.Out{Dest: &sql.NullString{Valid: false}, In: true},
			got:       sql.Out{Dest: &sql.NullString{Valid: false}, In: true},
		},

		{
			paramType: "varchar(1000)",
			want:      sql.Out{Dest: &sql.NullString{String: "", Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullString{String: "", Valid: true}, In: true},
		},
		{
			paramType: "varchar(1000)",
			want:      sql.Out{Dest: &sql.NullString{String: "Hello World", Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullString{String: "Hello World", Valid: true}, In: true},
		},
		{
			paramType: "double",
			want:      sql.Out{Dest: &sql.NullFloat64{Valid: false}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Valid: false}, In: true},
		},
		{
			paramType: "double",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: 1.999999, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: 1.999999, Valid: true}, In: true},
		},
		{
			paramType: "double",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: -1.999999, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: -1.999999, Valid: true}, In: true},
		},
		{
			paramType: "float",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: 1.999999, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: 1.999999, Valid: true}, In: true},
		},
		{
			paramType: "float",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: -1.999999, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: -1.999999, Valid: true}, In: true},
		},
		{
			paramType: "real",
			want:      sql.Out{Dest: &f32, In: true},
			got:       sql.Out{Dest: &f32, In: true},
		},
		{
			paramType: "decfloat",
			want:      sql.Out{Dest: &sql.NullFloat64{Valid: false}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Valid: false}, In: true},
		},
		{
			paramType: "decfloat",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: 1.999999, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: 1.999999, Valid: true}, In: true},
		},
		{
			paramType: "decfloat",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: -1.999999, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: -1.999999, Valid: true}, In: true},
		},
		{
			paramType: "decimal(7,1)",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: 199999.9, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: 199999.9, Valid: true}, In: true},
		},
		{
			paramType: "decimal(7,1)",
			want:      sql.Out{Dest: &sql.NullFloat64{Float64: -199999.9, Valid: true}, In: true},
			got:       sql.Out{Dest: &sql.NullFloat64{Float64: -199999.9, Valid: true}, In: true},
		},
		{
			paramType: "varbinary(255)",
			want:      sql.Out{Dest: &NullByte{Byte: nil, Valid: false}, In: true},
			got:       sql.Out{Dest: &NullByte{Byte: nil, Valid: false}, In: true},
		},
		{
			paramType: "varbinary(255)",
			want:      sql.Out{Dest: &NullByte{Byte: []byte(" "), Valid: true}, In: true},
			got:       sql.Out{Dest: &NullByte{Byte: []byte(" "), Valid: true}, In: true},
		},
		{
			paramType: "varbinary(255)",
			want:      sql.Out{Dest: &NullByte{Byte: []byte("myhint"), Valid: true}, In: true},
			got:       sql.Out{Dest: &NullByte{Byte: []byte("myhint"), Valid: true}, In: true},
		},
		{
			paramType: "varbinary(255)",
			want:      sql.Out{Dest: &NullByte{Byte: []byte("Hello, 世界"), Valid: true}, In: true},
			got:       sql.Out{Dest: &NullByte{Byte: []byte("Hello, 世界"), Valid: true}, In: true},
		},
		{
			paramType: "timestamp",
			want:      sql.Out{Dest: &ts, In: true},
			got:       sql.Out{Dest: &ts, In: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.paramType, func(t *testing.T) {
			paramType, want, got := tt.paramType, tt.want, tt.got
			// create the SP
			_, err := db.Exec(fmt.Sprintf(createSp, paramType))
			if err != nil {
				die(t, "%v", err)
			}

			// call the SP
			_, err = db.Exec(callSp, got)
			if err != nil {
				die(t, "%v", err)
			}

			// drop the SP
			_, err = db.Exec(dropSp)
			if err != nil {
				die(t, "%v", err)
			}

			if !reflect.DeepEqual(want, got) {
				die(t, "want %v, got %v\n", want, got)
			} else {
				info(t, "want %v, got %v\n", want, got)
			}
		})
	}
}

func TestSPStringInOut(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	// Out is bigger than In
	createSp := `CREATE OR REPLACE PROCEDURE test_inout(
		INOUT p_a %s
	)
		LANGUAGE SQL
		SPECIFIC test_inout
		BEGIN
			SET p_a = repeat(p_a, 2);
		END
		`
	callSp := "call test_inout(?)"
	dropSp := "drop procedure test_inout"

	tests := []struct {
		paramType string
		want      string
		inout     sql.Out
	}{
		{
			paramType: "varchar(25)",
			want:      "Hello, World Hello, World",
			inout:     sql.Out{Dest: &sql.NullString{String: "Hello, World ", Valid: true}, In: true},
		},
		{
			paramType: "clob(2)",
			want:      "11",
			inout:     sql.Out{Dest: &sql.NullString{String: "1", Valid: true}, In: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.paramType, func(t *testing.T) {
			paramType, want, inout := tt.paramType, tt.want, tt.inout
			// create the SP
			_, err := db.Exec(fmt.Sprintf(createSp, paramType))
			if err != nil {
				die(t, "Failed to run %v because %v", createSp, err)
			}

			// call the SP
			_, err = db.Exec(callSp, inout)
			if err != nil {
				die(t, "%v", err)
			}

			// drop the SP
			_, err = db.Exec(dropSp)
			if err != nil {
				die(t, "%v", err)
			}

			switch g := inout.Dest.(type) {
			case *sql.NullString:
				if (*g).String != want {
					die(t, "want %q, got %q\n", want, (*g).String)
				} else {
					info(t, "want %q, got %q\n", want, (*g).String)
				}
			default:
				die(t, "Unknown type %T\n", g)
			}
		})
	}
}

func TestSPClobOut(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	_, err = db.Exec(`
	 CREATE OR REPLACE PROCEDURE out_param_clob(IN empno char(6)
		, IN resume_format varchar(10)
		, OUT resume clob)
	LANGUAGE SQL
	SPECIFIC out_param_clob
	BEGIN
		select resume into resume from emp_resume
		where empno = empno and resume_format = resume_format
		fetch first 1 row only;
	END
	 `)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Exec("DROP PROCEDURE out_param_clob")
	}()

	var resume string
	procStmt := "CALL out_param_clob('000140', 'ascii', ?)"
	_, err = db.ExecContext(context.Background(), procStmt,
		sql.Out{Dest: &resume})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s returned %s\n", procStmt, resume)
}

// To check if we can use sql.Query to run a non-select statement
func TestDDLQuery(t *testing.T) {
	tabname := "test"
	createStmt := fmt.Sprintf("create table %s(col1 smallint)", tabname)
	insertStmt := fmt.Sprintf("insert into %s values(1)", tabname)
	selectStmt := fmt.Sprintf("select col1 from %s", tabname)
	dropStmt := fmt.Sprintf("drop table %s", tabname)

	type rowsAffected interface {
		RowsAffected() int64
	}

	db, err := newTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.close()

	_, err = db.Query(createStmt)
	if affected, ok := err.(rowsAffected); ok {
		info(t, "%q: row affected %d", createStmt, affected.RowsAffected())
	} else {
		die(t, "%q failed: %v\n", createStmt, err)
	}

	_, err = db.Exec(insertStmt)
	if err != nil {
		die(t, "%q failed: %v\n", insertStmt, err)
	}

	var val int
	err = db.QueryRow(selectStmt).Scan(&val)
	if err != nil {
		die(t, "%q failed: %v\n", selectStmt, err)
	}
	if val != 1 {
		die(t, "Expected 1, got %d\n", val)
	}

	_, err = db.Exec(dropStmt)
	if err != nil {
		die(t, "%q failed: %v\n", dropStmt, err)
	}
}

// Tests rowsAffectedError from sql.Query for statements
// that don't produce rows/resultset
func TestQueryExec(t *testing.T) {
	var val int
	qry := "SELECT 1 FROM syscat.tables where tabschema='abcd'"

	db, err := newTestDB()
	if err != nil {
		die(t, "db connection failed: %v", err)
	}
	defer db.close()

	info(t, "Testing QueryRow.Scan()")
	info(t, strings.Repeat("#", 40))
	err = db.QueryRow(qry).Scan(&val)
	switch {
	case err == sql.ErrNoRows:
		info(t, "%q: returned empty resultset\n", qry)
	case err != nil:
		die(t, "%q failed: %v", err)
	default:
		info(t, "%q: got %d\n", qry, val)
	}

	info(t, "Testing Query")
	info(t, strings.Repeat("#", 40))
	rows, err := db.Query(qry)
	switch {
	case err == sql.ErrNoRows:
		info(t, "%q: returned empty resultset\n", qry)
	case err != nil:
		die(t, "%q failed: %v\n", qry, err)
	default:
		for rows.Next() {
			if err := rows.Scan(&val); err != nil {
				die(t, "%q: rows.Scan failed: %v\n", qry, err)
			}
			info(t, "%q: got %d\n", qry, val)
		}
	}

	if err := rows.Close(); err != nil {
		die(t, "%q: rows.Close failed: %v\n", err)
	}

	execStmts := []string{
		"create table test(col1 smallint)",
		"insert into test values(1), (2), (3)",
		"drop table test",
	}
	info(t, "Testing DDL")
	info(t, strings.Repeat("#", 40))

	type rowsaff interface {
		RowsAffected() int64
	}

	for _, s := range execStmts {
		_, err = db.Query(s)
		switch {
		case err == sql.ErrNoRows:
			info(t, "%q returned empty result set", s)
		default:
			if i, ok := err.(rowsaff); ok {
				info(t, "%q: rows affected %d\n", s, i.RowsAffected())
			} else {
				die(t, "%q failed: %v", s, err)
			}
		}
	}
}

func TestErrorNewLine(t *testing.T) {
	errStmt := `
                select case col1
                        when 1 then 'one'
                        else raise_error('70001', 'Dummy')
                end
                from (values(0)) as t(col1)`

	db, err := newTestDB()
	if err != nil {
		die(t, "Failed to connect to db: %v", err)
	}
	defer db.Close()

	var msg string
	err = db.QueryRow(errStmt).Scan(&msg)
	if err == nil {
		die(t, "msg: %s:Expecting error, got nil error", msg)
	}
	if strings.HasSuffix(err.Error(), "\n") {
		die(t, "DB error message shouldn't end in new line: |%v|\n")
	}
	info(t, "|%s|", err.Error())
}

// for issue #10
// overflow error is not reported
func TestOverflow(t *testing.T) {
	db, err := newTestDB()
	if err != nil {
		die(t, "Failed to connect to db: %v", err)
	}
	defer db.Close()

	if err != nil {
		t.Error(err)
	}

	// this statement should report an overflow error
	// when casting the second value (2147483648) to an INT
	rows, err := db.Query("SELECT INT(val) FROM (VALUES (2147483647), (2147483648), (2147483647) ) t (val)")
	if err != nil {
		die(t, "Query failed: %v", err)
	}

	var values []string
	for rows.Next() {
		var value string
		rows.Scan(&value)
		values = append(values, value)
	}

	if rows.Err() == nil {
		die(t, "Expected overflow error; got nil error instead")
	}

	info(t, "values: %+v err: %+v", values, rows.Err())
}

func logf(t *testing.T, format string, a ...interface{}) {
	t.Logf(format, a...)
}

func info(t *testing.T, format string, a ...interface{}) {
	logf(t, fmt.Sprintf("%-10s", "ok")+format, a...)
}

func die(t *testing.T, format string, a ...interface{}) {
	t.Errorf(fmt.Sprintf("%-10s", "---FAIL:")+format, a...)
}
