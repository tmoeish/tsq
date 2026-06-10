package dialect

import (
	"database/sql"
	"strings"
	"testing"
)

func TestDDLColumnTypesEquivalent(t *testing.T) {
	tests := []struct {
		name    string
		dialect Dialect
		left    DDLColumnSpec
		right   DDLColumnSpec
		want    bool
	}{
		{
			name:    "postgres text raw type round trip",
			dialect: PostgresDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{RawType: "TEXT"}, NativeType: "text"},
			right:   DDLColumnSpec{Type: DDLColumnType{RawType: "TEXT"}},
			want:    true,
		},
		{
			name:    "postgres declared char matches native character",
			dialect: PostgresDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindString, Size: 10}, NativeType: "character(10)"},
			right:   DDLColumnSpec{Type: DDLColumnType{RawType: "CHAR(10)"}},
			want:    true,
		},
		{
			name:    "postgres declared numeric matches native numeric",
			dialect: PostgresDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindFloat, Bits: 64}, NativeType: "numeric(10,2)"},
			right:   DDLColumnSpec{Type: DDLColumnType{RawType: "DECIMAL(10, 2)"}},
			want:    true,
		},
		{
			name:    "mysql declared TEXT matches inspected text column",
			dialect: MySQLDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindString, Size: mysqlMaxVarcharChars + 1}, NativeType: "text"},
			right:   DDLColumnSpec{Type: DDLColumnType{RawType: "TEXT"}},
			want:    true,
		},
		{
			name:    "mysql declared DECIMAL matches inspected decimal column",
			dialect: MySQLDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindFloat, Bits: 64}, NativeType: "decimal(10,2)"},
			right:   DDLColumnSpec{Type: DDLColumnType{RawType: "DECIMAL(10,2)"}},
			want:    true,
		},
		{
			name:    "sqlite declared TEXT matches native TEXT",
			dialect: SQLiteDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindString}, NativeType: "TEXT"},
			right:   DDLColumnSpec{Type: DDLColumnType{RawType: "TEXT"}},
			want:    true,
		},
		{
			name:    "nullability does not affect type equivalence",
			dialect: PostgresDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindString, Size: 120, Nullable: true}},
			right:   DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindString, Size: 120}},
			want:    true,
		},
		{
			name:    "different rendered types are not equivalent",
			dialect: PostgresDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindInt, Bits: 64}, NativeType: "bigint"},
			right:   DDLColumnSpec{Type: DDLColumnType{Kind: DDLColumnKindString, Size: 255}},
			want:    false,
		},
		{
			name:    "declared raw type differing from native type is drift",
			dialect: PostgresDialect{},
			left:    DDLColumnSpec{Type: DDLColumnType{RawType: "TEXT"}, NativeType: "text"},
			right:   DDLColumnSpec{Type: DDLColumnType{RawType: "JSONB"}},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DDLColumnTypesEquivalent(tt.dialect, tt.left, tt.right); got != tt.want {
				t.Fatalf("DDLColumnTypesEquivalent() = %v, want %v", got, tt.want)
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
	desc, err := parsePostgresDDLColumnType("text", "text", "text", sql.NullInt64{})
	if err != nil {
		t.Fatalf("parsePostgresDDLColumnType() error = %v", err)
	}

	if desc.RawType != "TEXT" {
		t.Fatalf("expected TEXT raw type to round-trip, got %+v", desc)
	}
}

func TestMySQLDDLAlterColumnStatementsDoesNotRepeatPrimaryKey(t *testing.T) {
	d := MySQLDialect{}
	before := DDLColumnSpec{
		Name:          "id",
		Type:          DDLColumnType{Kind: DDLColumnKindInt, Bits: 32},
		PrimaryKey:    true,
		AutoIncrement: true,
	}
	after := DDLColumnSpec{
		Name:          "id",
		Type:          DDLColumnType{Kind: DDLColumnKindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
	}

	statements := d.DDLAlterColumnStatements("users", before, after)
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
	after := DDLColumnSpec{
		Name:    "version",
		Type:    DDLColumnType{Kind: DDLColumnKindInt, Bits: 64},
		Default: "1",
	}

	statements := d.DDLAlterColumnStatements("users", DDLColumnSpec{Name: "version"}, after)
	want := "ALTER TABLE `users` MODIFY COLUMN `version` BIGINT NOT NULL DEFAULT 1;"

	if len(statements) != 1 || statements[0] != want {
		t.Fatalf("unexpected statements:\n got: %v\nwant: %s", statements, want)
	}
}

func TestPostgresDDLAlterColumnStatementsNullabilityOnlySkipsAlterType(t *testing.T) {
	d := PostgresDialect{}
	before := DDLColumnSpec{
		Name:       "name",
		Type:       DDLColumnType{Kind: DDLColumnKindString, Size: 120, Nullable: true},
		NativeType: "character varying(120)",
	}
	after := DDLColumnSpec{
		Name: "name",
		Type: DDLColumnType{Kind: DDLColumnKindString, Size: 120},
	}

	statements := d.DDLAlterColumnStatements("users", before, after)
	want := `ALTER TABLE "users" ALTER COLUMN "name" SET NOT NULL;`

	if len(statements) != 1 || statements[0] != want {
		t.Fatalf("expected only a SET NOT NULL statement, got %v", statements)
	}
}

func TestPostgresDDLAlterColumnStatementsKeepsAutoIncrementDefault(t *testing.T) {
	d := PostgresDialect{}
	before := DDLColumnSpec{
		Name:          "id",
		Type:          DDLColumnType{Kind: DDLColumnKindInt, Bits: 32},
		PrimaryKey:    true,
		AutoIncrement: true,
		Default:       "nextval('users_id_seq'::regclass)",
		NativeType:    "integer",
	}
	after := DDLColumnSpec{
		Name:          "id",
		Type:          DDLColumnType{Kind: DDLColumnKindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
	}

	statements := d.DDLAlterColumnStatements("users", before, after)
	want := `ALTER TABLE "users" ALTER COLUMN "id" TYPE BIGINT;`

	if len(statements) != 1 || statements[0] != want {
		t.Fatalf("expected only an ALTER TYPE statement, got %v", statements)
	}

	for _, statement := range statements {
		if strings.Contains(statement, "DROP DEFAULT") {
			t.Fatal("reconcile must never drop the sequence default of an auto-increment column")
		}
	}
}
