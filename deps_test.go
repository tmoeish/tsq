package tsq

import (
	"os/exec"
	"strings"
	"testing"
)

// TestRootPackageImportsNoDriver keeps database drivers and nullbio out of the
// root package and its tests. go mod tidy records what a dependency's tests import
// in the user's go.sum, and what the package itself imports in their go.mod, so
// either would make every TSQ user download a driver they may never use. Tests
// that need a real driver live in internal/integration.
func TestRootPackageImportsNoDriver(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go list")
	}

	out, err := exec.Command("go", "list", "-deps", "-test", "-f", "{{.ImportPath}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}

	for pkg := range strings.FieldsSeq(string(out)) {
		for _, banned := range []string{"github.com/go-sql-driver/", "github.com/jackc/", "github.com/lib/pq", "gopkg.in/nullbio/"} {
			if strings.HasPrefix(pkg, banned) {
				t.Errorf("the root package or its tests import %s", pkg)
			}
		}
	}
}
