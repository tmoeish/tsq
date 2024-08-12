package parser

import (
	"go/parser"
	"go/token"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parsePkgAlias(t *testing.T) {
	src := `
package p

import "strings"

import (
	xxpkgv2 "a.b/c/xxpkg.v2"
	"gopkg.in/gorp.v2"
)
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	pkgs := parsePkgAlias(f)
	assert.Equal(t, 3, len(pkgs))
	assert.Equal(t, "strings", pkgs["strings"])
	assert.Equal(t, "a.b/c/xxpkg.v2", pkgs["xxpkgv2"])
	assert.Equal(t, "gopkg.in/gorp.v2", pkgs["gorp"])
}
