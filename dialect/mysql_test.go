package dialect

import (
	"database/sql"
	"strings"
	"testing"
)

func TestMySQLDialectDDLColumnTypeUsesTextFamilyForLargeStrings(t *testing.T) {
	t.Parallel()

	dialect := MySQLDialect{}
	tests := []struct {
		name string
		size int
		want string
	}{
		{name: "default varchar", size: 0, want: "VARCHAR(255)"},
		{name: "varchar", size: 128, want: "VARCHAR(128)"},
		{name: "varchar at limit", size: mysqlMaxVarcharChars, want: "VARCHAR(16383)"},
		{name: "mediumtext past varchar limit", size: mysqlMaxVarcharChars + 1, want: "MEDIUMTEXT"},
		{name: "mediumtext at limit", size: mysqlMaxMediumTextChars, want: "MEDIUMTEXT"},
		{name: "longtext past mediumtext limit", size: mysqlMaxMediumTextChars + 1, want: "LONGTEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := dialect.ColumnTypeSQL(ColumnType{
				Kind: KindString,
				Size: tt.size,
			})
			if got != tt.want {
				t.Fatalf("DDLColumnType(size=%d) = %q, want %q", tt.size, got, tt.want)
			}
		})
	}
}

func TestDDLColumnTypeUsesVarcharForDefaultStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
		want    string
	}{
		{name: "mysql", dialect: MySQLDialect{}, want: "VARCHAR(255)"},
		{name: "postgres", dialect: PostgresDialect{}, want: "VARCHAR(255)"},
		{name: "sqlite", dialect: SQLiteDialect{}, want: "VARCHAR(255)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.dialect.ColumnTypeSQL(ColumnType{Kind: KindString})
			if got != tt.want {
				t.Fatalf("DDLColumnType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDDLCreateIndexTerminatesStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
	}{
		{name: "mysql", dialect: MySQLDialect{}},
		{name: "postgres", dialect: PostgresDialect{}},
		{name: "sqlite", dialect: SQLiteDialect{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.dialect.CreateIndexSQL("task", "idx_task_state", []string{tt.dialect.QuoteIdent("state")}, false)
			if !strings.HasSuffix(got, ";") {
				t.Fatalf("CreateIndexSQL() = %q, want statement terminator", got)
			}
		})
	}
}

func TestDDLColumnTypeUsesRawTypeOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect Dialect
		rawType string
	}{
		{name: "mysql", dialect: MySQLDialect{}, rawType: "JSON"},
		{name: "postgres", dialect: PostgresDialect{}, rawType: "JSONB"},
		{name: "sqlite", dialect: SQLiteDialect{}, rawType: "JSON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.dialect.ColumnTypeSQL(ColumnType{
				Kind:    KindString,
				RawType: tt.rawType,
				Size:    255,
			})
			if got != tt.rawType {
				t.Fatalf("DDLColumnType(raw=%q) = %q, want %q", tt.rawType, got, tt.rawType)
			}
		})
	}
}

func TestDDLColumnParsersPreserveUnknownTypes(t *testing.T) {
	t.Parallel()

	mysqlDesc, err := parseMySQLColumnType("json", "json", sql.NullInt64{})
	if err != nil {
		t.Fatalf("parseMySQLColumnType() error = %v", err)
	}
	if mysqlDesc.RawType != "json" {
		t.Fatalf("parseMySQLColumnType() raw = %q, want %q", mysqlDesc.RawType, "json")
	}

	postgresDesc, err := parsePostgresColumnType("USER-DEFINED", "jsonb", "jsonb", sql.NullInt64{})
	if err != nil {
		t.Fatalf("parsePostgresColumnType() error = %v", err)
	}
	if postgresDesc.RawType != "jsonb" {
		t.Fatalf("parsePostgresColumnType() raw = %q, want %q", postgresDesc.RawType, "jsonb")
	}

	sqliteDesc, err := parseSQLiteColumnType("JSON")
	if err != nil {
		t.Fatalf("parseSQLiteColumnType() error = %v", err)
	}
	if sqliteDesc.RawType != "JSON" {
		t.Fatalf("parseSQLiteColumnType() raw = %q, want %q", sqliteDesc.RawType, "JSON")
	}
}
