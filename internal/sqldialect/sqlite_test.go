package sqldialect

import "testing"

// TestSQLiteCollationIsReadFromTheCreateText covers the COLLATE a column of its
// own carries in sqlite_master's text: names quoted every way, a CHECK with
// commas and parentheses before it, a collation on another column only.
func TestSQLiteCollationIsReadFromTheCreateText(t *testing.T) {
	create := `CREATE TABLE "t" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "code" VARCHAR(20) NOT NULL COLLATE NOCASE,
    [name] TEXT DEFAULT 'a, b' COLLATE "BINARY",
    ` + "`note`" + ` TEXT CONSTRAINT "ck_note" CHECK (length("note") > 0 AND "note" <> ',') COLLATE RTRIM,
    plain VARCHAR(10) DEFAULT 'COLLATE NOCASE',
    "n" INTEGER CONSTRAINT "ck_n" CHECK ("n" >= -128 AND "n" <= 127)
)`

	for column, want := range map[string]string{"code": "NOCASE", "name": "BINARY", "note": "RTRIM", "plain": "", "n": "", "id": "", "missing": ""} {
		if got := sqliteCollation(create, column); got != want {
			t.Errorf("%s: collation %q, want %q", column, got, want)
		}
	}
}
