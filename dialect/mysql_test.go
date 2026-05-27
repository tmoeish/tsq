package dialect

import (
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

			got := dialect.DDLColumnType(DDLColumnType{
				Kind: DDLColumnKindString,
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

			got := tt.dialect.DDLColumnType(DDLColumnType{Kind: DDLColumnKindString})
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

			got := tt.dialect.DDLCreateIndex("task", "idx_task_state", []string{tt.dialect.QuoteField("state")}, false)
			if !strings.HasSuffix(got, ";") {
				t.Fatalf("DDLCreateIndex() = %q, want statement terminator", got)
			}
		})
	}
}
