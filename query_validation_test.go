package tsq

import (
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestRenderSQLForDialectPostgres(t *testing.T) {
	users := newMockTable("users")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	query := mustBuild(Select(userID).From(userID.Table()).Where(userID.EQVar()))
	got := renderSQLForDialect(query.listSQL, PostgresDialect{})
	want := `SELECT "users"."id" FROM "users" WHERE "users"."id" = $1`
	if got != want {
		t.Fatalf("expected postgres SQL %q, got %q", want, got)
	}
}

func TestRenderDeleteByIDsSQLForPostgres(t *testing.T) {
	sqlStr, err := buildDeleteByIDsSQL("users", "id", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := renderSQLForDialect(sqlStr, PostgresDialect{})
	want := `DELETE FROM "users" WHERE "id" IN ($1,$2)`
	if got != want {
		t.Fatalf("expected postgres delete SQL %q, got %q", want, got)
	}
}

func TestValidateExecutorForSQLIgnoresMarkersInsideStringsAndComments(t *testing.T) {
	db := &Engine{}
	rawSQL := "SELECT 1 /* " + identifierMarkerPrefix + "ignored_comment" + identifierMarkerSuffix + " */" + " WHERE note = '" + identifierMarkerPrefix + "ignored_string" + identifierMarkerSuffix + "'" + " -- " + identifierMarkerPrefix + "ignored_tail" + identifierMarkerSuffix + "\n"
	if err := validateExecutorForSQL(db, rawSQL); err != nil {
		t.Fatalf("expected markers inside strings/comments to be ignored, got %v", err)
	}
}

func TestValidateExecutorForSQLIgnoresMarkersInsideDollarQuotedStrings(t *testing.T) {
	db := &Engine{}
	rawSQL := "SELECT $$" + identifierMarkerPrefix + "ignored_marker" + identifierMarkerSuffix + "$$"
	if err := validateExecutorForSQL(db, rawSQL); err != nil {
		t.Fatalf("expected markers inside dollar-quoted strings to be ignored, got %v", err)
	}
}

func TestValidateExecutorForSQLRejectsBindVarsWithoutDialect(t *testing.T) {
	db := &Engine{}
	if err := validateExecutorForSQL(db, "SELECT ?"); err == nil {
		t.Fatal("expected bind vars without a known dialect to return an error")
	}
}

func TestValidateExecutorForSQLIgnoresBindVarsInsideStringsCommentsAndDollarQuotes(t *testing.T) {
	db := &Engine{}
	rawSQL := "SELECT '?'" + " /* ? */" + " WHERE note = $$?$$" + " -- ?\n"
	if err := validateExecutorForSQL(db, rawSQL); err != nil {
		t.Fatalf("expected bind vars inside strings/comments to be ignored, got %v", err)
	}
}

func TestValidateIdentifierLength(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		dialect    Dialect
		wantErr    bool
	}{{name: "valid identifier - mysql", identifier: "users", dialect: MySQLDialect{}, wantErr: false}, {name: "valid identifier - postgres", identifier: "users", dialect: PostgresDialect{}, wantErr: false}, {name: "max length postgres (63)", identifier: "a" + strings.Repeat("b", 62), dialect: PostgresDialect{}, wantErr: false}, {name: "exceeds max postgres (64 > 63)", identifier: strings.Repeat("a", 64), dialect: PostgresDialect{}, wantErr: true}, {name: "max length mysql (64)", identifier: strings.Repeat("a", 64), dialect: MySQLDialect{}, wantErr: false}, {name: "exceeds max mysql (65 > 64)", identifier: strings.Repeat("a", 65), dialect: MySQLDialect{}, wantErr: true}, {name: "sqlite has no limit", identifier: strings.Repeat("a", 200), dialect: SQLiteDialect{}, wantErr: false}, {name: "empty identifier", identifier: "", dialect: MySQLDialect{}, wantErr: true}, {name: "nil dialect skips length validation", identifier: strings.Repeat("a", 100), dialect: nil, wantErr: false}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifierLength(tt.identifier, tt.dialect)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateIdentifierLength(%q, %T) error = %v, wantErr %v", tt.identifier, tt.dialect, err, tt.wantErr)
			}
		})
	}
}

func TestValidateIdentifierForDialect(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		dialect    Dialect
		wantErr    bool
		errContent string
	}{{name: "valid identifier - mysql", identifier: "users", dialect: MySQLDialect{}, wantErr: false}, {name: "valid identifier - postgres", identifier: "users_table", dialect: PostgresDialect{}, wantErr: false}, {name: "starts with underscore", identifier: "_internal", dialect: MySQLDialect{}, wantErr: false}, {name: "invalid - starts with number", identifier: "123users", dialect: MySQLDialect{}, wantErr: true, errContent: "invalid SQL identifier"}, {name: "invalid - contains hyphen", identifier: "user-table", dialect: MySQLDialect{}, wantErr: true, errContent: "invalid SQL identifier"}, {name: "exceeds postgres limit", identifier: strings.Repeat("a", 64), dialect: PostgresDialect{}, wantErr: true, errContent: "exceeds"}, {name: "at mysql limit (64)", identifier: strings.Repeat("x", 64), dialect: MySQLDialect{}, wantErr: false}, {name: "empty identifier", identifier: "", dialect: MySQLDialect{}, wantErr: true, errContent: "cannot be empty"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifierForDialect(tt.identifier, tt.dialect)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateIdentifierForDialect(%q, %T) error = %v, wantErr %v", tt.identifier, tt.dialect, err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContent != "" && !strings.Contains(err.Error(), tt.errContent) {
				t.Errorf("ValidateIdentifierForDialect(%q, %T) error message %q should contain %q", tt.identifier, tt.dialect, err.Error(), tt.errContent)
			}
		})
	}
}
