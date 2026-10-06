package sqldialect

import (
	"database/sql"
	"slices"
	"strings"
	"testing"
)

func TestDDLColumnTypesEquivalent(t *testing.T) {
	tests := []struct {
		name    string
		dialect Dialect
		left    Column
		right   ColumnSpec
		want    bool
	}{
		{
			name:    "postgres text raw type round trip",
			dialect: PostgresDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{RawType: "TEXT"}}, NativeType: "text"},
			right:   ColumnSpec{Type: ColumnType{RawType: "TEXT"}},
			want:    true,
		},
		{
			name:    "postgres declared char matches native character",
			dialect: PostgresDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{Kind: KindString, Size: 10}}, NativeType: "character(10)"},
			right:   ColumnSpec{Type: ColumnType{RawType: "CHAR(10)"}},
			want:    true,
		},
		{
			name:    "postgres declared numeric matches native numeric",
			dialect: PostgresDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{Kind: KindFloat, Bits: 64}}, NativeType: "numeric(10,2)"},
			right:   ColumnSpec{Type: ColumnType{RawType: "DECIMAL(10, 2)"}},
			want:    true,
		},
		{
			name:    "mysql declared TEXT matches inspected text column",
			dialect: MySQLDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{Kind: KindString, Size: mysqlMaxVarcharChars + 1}}, NativeType: "text"},
			right:   ColumnSpec{Type: ColumnType{RawType: "TEXT"}},
			want:    true,
		},
		{
			name:    "mysql declared DECIMAL matches inspected decimal column",
			dialect: MySQLDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{Kind: KindFloat, Bits: 64}}, NativeType: "decimal(10,2)"},
			right:   ColumnSpec{Type: ColumnType{RawType: "DECIMAL(10,2)"}},
			want:    true,
		},
		{
			name:    "sqlite declared TEXT matches native TEXT",
			dialect: SQLiteDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{Kind: KindString}}, NativeType: "TEXT"},
			right:   ColumnSpec{Type: ColumnType{RawType: "TEXT"}},
			want:    true,
		},
		{
			name:    "nullability does not affect type equivalence",
			dialect: PostgresDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{Kind: KindString, Size: 120, Nullable: true}}},
			right:   ColumnSpec{Type: ColumnType{Kind: KindString, Size: 120}},
			want:    true,
		},
		{
			name:    "different rendered types are not equivalent",
			dialect: PostgresDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{Kind: KindInt, Bits: 64}}, NativeType: "bigint"},
			right:   ColumnSpec{Type: ColumnType{Kind: KindString, Size: 255}},
			want:    false,
		},
		{
			name:    "declared raw type differing from native type is drift",
			dialect: PostgresDialect{},
			left:    Column{ColumnSpec: ColumnSpec{Type: ColumnType{RawType: "TEXT"}}, NativeType: "text"},
			right:   ColumnSpec{Type: ColumnType{RawType: "JSONB"}},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SameColumnType(tt.dialect, tt.left, tt.right); got != tt.want {
				t.Fatalf("SameColumnType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSQLiteCreateSQLDeclaresAutoincrement(t *testing.T) {
	tests := []struct {
		name      string
		createSQL string
		column    string
		want      bool
	}{
		{
			name:      "tsq quoted ddl",
			createSQL: `CREATE TABLE "users" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" VARCHAR(120))`,
			column:    "id",
			want:      true,
		},
		{
			name:      "handwritten unquoted lowercase ddl",
			createSQL: `create table users (id integer primary key autoincrement, name text)`,
			column:    "id",
			want:      true,
		},
		{
			name:      "bracket quoted ddl",
			createSQL: `CREATE TABLE users ([id] INTEGER PRIMARY KEY AUTOINCREMENT)`,
			column:    "id",
			want:      true,
		},
		{
			name:      "column name suffix of another column does not match",
			createSQL: `CREATE TABLE users (uid INTEGER PRIMARY KEY AUTOINCREMENT)`,
			column:    "id",
			want:      false,
		},
		{
			name:      "plain integer primary key is not autoincrement",
			createSQL: `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
			column:    "id",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sqliteCreateSQLDeclaresAutoincrement(strings.ToUpper(tt.createSQL), tt.column)
			if got != tt.want {
				t.Fatalf("sqliteCreateSQLDeclaresAutoincrement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePostgresDDLColumnTypeTextKeepsRawType(t *testing.T) {
	desc, err := parsePostgresColumnType("text", "text", "text", sql.NullInt64{})
	if err != nil {
		t.Fatalf("parsePostgresColumnType() error = %v", err)
	}

	if desc.RawType != "TEXT" {
		t.Fatalf("expected TEXT raw type to round-trip, got %+v", desc)
	}
}

func TestMySQLDDLAlterColumnStatementsDoesNotRepeatPrimaryKey(t *testing.T) {
	d := MySQLDialect{}
	before := ColumnSpec{
		Name:          "id",
		Type:          ColumnType{Kind: KindInt, Bits: 32},
		PrimaryKey:    true,
		AutoIncrement: true,
	}
	after := ColumnSpec{
		Name:          "id",
		Type:          ColumnType{Kind: KindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
	}

	statements := d.AlterColumnSQL("users", Column{ColumnSpec: before}, after)
	if len(statements) != 1 {
		t.Fatalf("expected a single MODIFY statement, got %v", statements)
	}

	want := "ALTER TABLE `users` MODIFY COLUMN `id` BIGINT NOT NULL AUTO_INCREMENT;"
	if statements[0] != want {
		t.Fatalf("unexpected statement:\n got: %s\nwant: %s", statements[0], want)
	}

	if strings.Contains(statements[0], "PRIMARY KEY") {
		t.Fatal("MODIFY COLUMN must not restate PRIMARY KEY (MySQL error 1068)")
	}
}

func TestMySQLDDLAlterColumnStatementsKeepsDefaultForRegularColumn(t *testing.T) {
	d := MySQLDialect{}
	after := ColumnSpec{
		Name:    "version",
		Type:    ColumnType{Kind: KindInt, Bits: 64},
		Default: "1",
	}

	statements := d.AlterColumnSQL("users", Column{Name: "version"}, after)
	want := "ALTER TABLE `users` MODIFY COLUMN `version` BIGINT NOT NULL DEFAULT 1;"

	if len(statements) != 1 || statements[0] != want {
		t.Fatalf("unexpected statements:\n got: %v\nwant: %s", statements, want)
	}
}

func TestPostgresDDLAlterColumnStatementsNullabilityOnlySkipsAlterType(t *testing.T) {
	d := PostgresDialect{}
	before := Column{
		Name: "name",
		Type: ColumnType{Kind: KindString, Size: 120, Nullable: true}, NativeType: "character varying(120)",
	}
	after := ColumnSpec{
		Name: "name",
		Type: ColumnType{Kind: KindString, Size: 120},
	}

	// No ALTER TYPE; the rows holding NULL are filled before SET NOT NULL, which
	// would fail on them.
	statements := d.AlterColumnSQL("users", before, after)
	want := []string{
		`UPDATE "users" SET "name" = '' WHERE "name" IS NULL;`,
		`ALTER TABLE "users" ALTER COLUMN "name" SET NOT NULL;`,
	}

	if !slices.Equal(statements, want) {
		t.Fatalf("statements = %v; want %v", statements, want)
	}
}

// TestPostgresAlterColumnDoesNotTruncate covers a shorter VARCHAR written with
// USING col::VARCHAR(n), an explicit cast that cuts longer values without an
// error, and a current-time default set as CURRENT_TIMESTAMP, the session's local
// time where CREATE TABLE writes UTC.
func TestPostgresAlterColumnDoesNotTruncate(t *testing.T) {
	d := PostgresDialect{}

	shrink := d.AlterColumnSQL("t",
		Column{Name: "name", Type: ColumnType{Kind: KindString, Size: 128}},
		ColumnSpec{Name: "name", Type: ColumnType{Kind: KindString, Size: 8}})
	if len(shrink) != 1 || shrink[0] != `ALTER TABLE "t" ALTER COLUMN "name" TYPE VARCHAR(8);` {
		t.Fatalf("shrink = %v; want no USING, so PostgreSQL refuses a value that does not fit", shrink)
	}

	kind := d.AlterColumnSQL("t",
		Column{Name: "flag", Type: ColumnType{Kind: KindBool}},
		ColumnSpec{Name: "flag", Type: ColumnType{Kind: KindInt, Bits: 32}})
	if len(kind) != 1 || !strings.Contains(kind[0], `USING "flag"::INTEGER`) {
		t.Fatalf("bool to int = %v; want USING, which has no assignment cast", kind)
	}

	stamped := d.AlterColumnSQL("t",
		Column{Name: "seen_at", Type: ColumnType{Kind: KindTime, Nullable: true}},
		ColumnSpec{Name: "seen_at", Type: ColumnType{Kind: KindTime}, Default: "CURRENT_TIMESTAMP"})
	joined := strings.Join(stamped, " ")
	if !strings.Contains(joined, "SET DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')") || strings.Contains(joined, "SET DEFAULT CURRENT_TIMESTAMP;") {
		t.Fatalf("stamped = %v; want the UTC default CREATE TABLE writes", stamped)
	}
}

// TestPostgresAlterColumnDropsTheDefaultBeforeTheType covers a column with a
// default changing kind: the server casts the default to the new type itself,
// not through USING, and refused the change where it could not ("default for
// column cannot be cast automatically to type boolean").
func TestPostgresAlterColumnDropsTheDefaultBeforeTheType(t *testing.T) {
	d := PostgresDialect{}

	retyped := d.AlterColumnSQL("t",
		Column{Name: "flag", Type: ColumnType{Kind: KindString, Size: 40, Nullable: true}, Default: "'0'"},
		ColumnSpec{Name: "flag", Type: ColumnType{Kind: KindBool, Nullable: true}, Default: "0"})
	want := []string{
		`ALTER TABLE "t" ALTER COLUMN "flag" DROP DEFAULT;`,
		`ALTER TABLE "t" ALTER COLUMN "flag" TYPE BOOLEAN USING "flag"::BOOLEAN;`,
		`ALTER TABLE "t" ALTER COLUMN "flag" SET DEFAULT FALSE;`,
	}
	if strings.Join(retyped, "\n") != strings.Join(want, "\n") {
		t.Fatalf("retyped with a default:\n%s\nwant:\n%s", strings.Join(retyped, "\n"), strings.Join(want, "\n"))
	}

	// Dropped once: a default that goes with the change is not dropped twice.
	gone := d.AlterColumnSQL("t",
		Column{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 64, Nullable: true}, Default: "5"},
		ColumnSpec{Name: "n", Type: ColumnType{Kind: KindString, Size: 40, Nullable: true}})
	if len(gone) != 2 || !strings.HasSuffix(gone[0], "DROP DEFAULT;") || !strings.Contains(gone[1], "USING") {
		t.Fatalf("retyped without a default = %v; want one DROP DEFAULT, then the type", gone)
	}

	// A change within one kind casts the default on its own.
	widened := d.AlterColumnSQL("t",
		Column{Name: "s", Type: ColumnType{Kind: KindString, Size: 8, Nullable: true}, Default: "'x'"},
		ColumnSpec{Name: "s", Type: ColumnType{Kind: KindString, Size: 80, Nullable: true}, Default: "'x'"})
	if len(widened) != 1 || !strings.Contains(widened[0], "TYPE VARCHAR(80);") {
		t.Fatalf("widened = %v; want the type change alone", widened)
	}

	// BOOLEAN has a cast to INTEGER and none to a floating-point type.
	fraction := d.AlterColumnSQL("t",
		Column{Name: "flag", Type: ColumnType{Kind: KindBool}},
		ColumnSpec{Name: "flag", Type: ColumnType{Kind: KindFloat, Bits: 64}})
	if len(fraction) != 1 || !strings.Contains(fraction[0], `USING "flag"::INTEGER::DOUBLE PRECISION`) {
		t.Fatalf("bool to a fraction = %v; want a cast through INTEGER", fraction)
	}
}

// TestBooleanDefaultsAreSpelledAsPostgresTakesThem covers default:1 and
// default:0 on a bool field, which PostgreSQL refuses as "of type integer".
func TestBooleanDefaultsAreSpelledAsPostgresTakesThem(t *testing.T) {
	flag := ColumnSpec{Name: "flag", Type: ColumnType{Kind: KindBool, Nullable: true}}

	for declared, want := range map[string]map[Name]string{
		"1":     {Postgres: "TRUE", MySQL: "1", SQLite: "1"},
		"0":     {Postgres: "FALSE", MySQL: "0", SQLite: "0"},
		"TRUE":  {Postgres: "TRUE", MySQL: "TRUE", SQLite: "TRUE"},
		"false": {Postgres: "false", MySQL: "false", SQLite: "false"},
	} {
		flag.Default = declared

		for name, spelled := range want {
			d, _ := For(name)
			if got := DefaultSQL(d, flag); got != spelled {
				t.Errorf("default %s on %s = %s, want %s", declared, name, got, spelled)
			}
		}
	}

	// A raw type is the declaration's own business.
	raw := ColumnSpec{Name: "flag", Type: ColumnType{RawType: "BOOLEAN", Nullable: true}, Default: "1"}
	if got := DefaultSQL(PostgresDialect{}, raw); got != "1" {
		t.Errorf("raw boolean default = %s, want it untouched", got)
	}
}

func TestPostgresDDLAlterColumnStatementsKeepsAutoIncrementDefault(t *testing.T) {
	d := PostgresDialect{}
	before := Column{
		Name:          "id",
		Type:          ColumnType{Kind: KindInt, Bits: 32},
		PrimaryKey:    true,
		AutoIncrement: true,
		Default:       "nextval('users_id_seq'::regclass)", NativeType: "integer",
	}
	after := ColumnSpec{
		Name:          "id",
		Type:          ColumnType{Kind: KindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
	}

	// The column and its sequence are widened; the SERIAL default is kept.
	statements := d.AlterColumnSQL("users", before, after)
	want := []string{
		`ALTER TABLE "users" ALTER COLUMN "id" TYPE BIGINT;`,
		`DO $$ BEGIN EXECUTE format('ALTER SEQUENCE %s AS BIGINT', pg_get_serial_sequence('"users"', 'id')); END $$;`,
	}

	if !slices.Equal(statements, want) {
		t.Fatalf("statements = %v, want %v", statements, want)
	}

	for _, statement := range statements {
		if strings.Contains(statement, "DROP DEFAULT") {
			t.Fatal("reconcile must never drop the sequence default of an auto-increment column")
		}
	}
}
