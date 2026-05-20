package tsq

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
)

func TestBindUsesPlaceholdersForDatabaseCompatibleValues(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name  string
		input any
	}{{name: "string", input: `path\file`}, {name: "bytes", input: []byte("hello")}, {name: "time", input: now}, {name: "valuer", input: customValuer{value: "custom", valid: true}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := Bind(tt.input)
			if err := expressionBuildError(expr); err != nil {
				t.Fatalf("unexpected bind error: %v", err)
			}
			if got := expr.Expr(); got != "?" {
				t.Fatalf("expected bind expression to use placeholder, got %q", got)
			}
			args := expr.Args()
			if len(args) != 1 {
				t.Fatalf("expected one bound arg, got %#v", args)
			}
			if !reflect.DeepEqual(args[0], tt.input) {
				t.Fatalf("expected bound arg %#v, got %#v", tt.input, args[0])
			}
		})
	}
}

func TestBindRejectsNullAndUnsupportedPredicateValues(t *testing.T) {
	var nilPointerValuer *pointerValuer
	tests := []struct {
		name  string
		input any
	}{{name: "nil", input: nil}, {name: "null string", input: sql.NullString{}}, {name: "invalid valuer", input: customValuer{value: "custom", valid: false}}, {name: "typed nil valuer", input: nilPointerValuer}, {name: "slice", input: []int{1, 2, 3}}, {name: "map", input: map[string]int{"id": 1}}, {name: "struct", input: struct{ ID int }{ID: 1}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := expressionBuildError(Bind(tt.input)); err == nil {
				t.Fatal("expected bind to reject value")
			}
		})
	}
}

func TestArgumentToExpressionDefaultsToBind(t *testing.T) {
	expr := argumentToExpression(`it's a test`)
	if err := expressionBuildError(expr); err != nil {
		t.Fatalf("unexpected expression error: %v", err)
	}
	if got := expr.Expr(); got != "?" {
		t.Fatalf("expected default expression conversion to bind values, got %q", got)
	}
	args := expr.Args()
	if len(args) != 1 || args[0] != `it's a test` {
		t.Fatalf("expected bound args [it's a test], got %#v", args)
	}
}
