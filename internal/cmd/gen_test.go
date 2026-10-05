package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"text/template"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

func TestGenArgsRejectsMissingOrExtraPackagePaths(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
	})

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing", args: nil, want: "tsq gen expects exactly one package path, got 0"},
		{name: "extra", args: []string{"./a", "./b"}, want: "tsq gen expects exactly one package path, got 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := exactOnePackageArg(tc.args)
			if err == nil {
				t.Fatal("expected exact arg validation to fail")
			}
			if err.Error() != tc.want {
				t.Fatalf("unexpected arg error %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

func TestGenCmdHelpDocumentsInputsAndOverwriteBehavior(t *testing.T) {
	buf := new(bytes.Buffer)
	GenCmd.SetOut(buf)
	GenCmd.SetErr(buf)

	if err := GenCmd.Help(); err != nil {
		t.Fatalf("expected gen help to render, got %v", err)
	}

	help := buf.String()
	for _, want := range []string{
		"module import path",
		"relative directory",
		"<struct>.tsq.go for each struct marked //tsq:table",
		"<result>.result.tsq.go for each struct marked //tsq:result",
		"sqlite.sql / mysql.sql / postgres.sql",
		"tsq.json",
		`refuses to overwrite non-generated files`,
		"--dry-run",
		"--check",
		"use -v to print each rendered file path",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("expected gen help to mention %q, got:\n%s", want, help)
		}
	}
}

func TestGenCmdGeneratesDDLArtifactsAndGuidance(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

import "time"

//tsq:table name=users pk=PK
//tsq:managed created_at
type User struct {
	PK        int64     `+"`db:\"id\"`"+`
	CreatedAt time.Time `+"`db:\"created_at\"`"+`
	Name      string    `+"`db:\"name,size:128\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	GenCmd.SetOut(stdout)
	GenCmd.SetErr(stderr)
	GenCmd.SetArgs([]string{"."})

	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() error = %v", err)
	}

	for _, name := range []string{
		"runtime.tsq.go",
		"user.tsq.go",
		"sqlite.sql",
		"mysql.sql",
		"postgres.sql",
		ddlStateFilename,
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected generated artifact %s to exist: %v", name, err)
		}
	}

	for _, name := range []string{
		"sqlite.incremental.sql",
		"mysql.incremental.sql",
		"postgres.incremental.sql",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("expected first run to skip %s, got err=%v", name, err)
		}
	}

	if got := stderr.String(); !strings.Contains(got, "sqlite=sqlite.sql mysql=mysql.sql postgres=postgres.sql") {
		t.Fatalf("expected stderr guidance to mention full ddl files, got:\n%s", got)
	}

	stateBytes, err := os.ReadFile(filepath.Join(dir, ddlStateFilename))
	if err != nil {
		t.Fatalf("failed to read ddl state file: %v", err)
	}
	if !strings.Contains(string(stateBytes), "\n  \"generated_by\": ") {
		t.Fatalf("expected ddl state file to be pretty-printed, got:\n%s", string(stateBytes))
	}

	var state ddlStateFile
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		t.Fatalf("failed to parse ddl state file: %v", err)
	}
	if len(state.Records) != 0 {
		t.Fatalf("expected first run to skip incremental records, got %d", len(state.Records))
	}
}

func TestGenCmdGeneratesRuntimeMetadataFile(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

//tsq:table
//tsq:unique Email name=ux_users_email
type User struct {
	ID    int64  `+"`db:\"id\"`"+`
	Email string `+"`db:\"email\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})

	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() error = %v", err)
	}

	runtimeFile, err := os.ReadFile(filepath.Join(dir, "runtime.tsq.go"))
	if err != nil {
		t.Fatalf("failed to read runtime.tsq.go: %v", err)
	}

	rendered := string(runtimeFile)
	for _, want := range []string{
		"func TSQTables() []tsq.Table",
		"TableUser,",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected runtime.tsq.go to contain %q, got:\n%s", want, rendered)
		}
	}

	tableFile, err := os.ReadFile(filepath.Join(dir, "user.tsq.go"))
	if err != nil {
		t.Fatalf("failed to read user.tsq.go: %v", err)
	}

	for _, want := range []string{
		`Name: "ux_users_email"`,
		`Columns: []string{"email"}`,
	} {
		if !strings.Contains(string(tableFile), want) {
			t.Fatalf("expected user.tsq.go to declare %q, got:\n%s", want, tableFile)
		}
	}
}

func TestGenCmdKeepsDeletedAtInRuntimeAndDDLIndexes(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

//tsq:table name=orders
//tsq:managed deleted_at
//tsq:index Status
type Order struct {
	ID        int64 `+"`db:\"id\"`"+`
	DeletedAt int64 `+"`db:\"deleted_at\"`"+`
	Status    int64 `+"`db:\"status\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() error = %v", err)
	}

	tableFile, err := os.ReadFile(filepath.Join(dir, "order.tsq.go"))
	if err != nil {
		t.Fatalf("failed to read order.tsq.go: %v", err)
	}
	if got := string(tableFile); !strings.Contains(got, `Columns: []string{"deleted_at", "status"}`) {
		t.Fatalf("expected declared index fields to include deleted_at prefix, got:\n%s", got)
	}

	for _, tt := range []struct {
		filename string
		want     string
	}{
		{filename: "mysql.sql", want: "ALTER TABLE `orders` ADD INDEX `idx_orders_status`(`deleted_at`, `status`);"},
		{filename: "postgres.sql", want: `CREATE INDEX "idx_orders_status" ON "orders"("deleted_at", "status");`},
		{filename: "sqlite.sql", want: `CREATE INDEX "idx_orders_status" ON "orders"("deleted_at", "status");`},
	} {
		content, err := os.ReadFile(filepath.Join(dir, tt.filename))
		if err != nil {
			t.Fatalf("failed to read %s: %v", tt.filename, err)
		}
		if got := string(content); !strings.Contains(got, tt.want) {
			t.Fatalf("expected %s to contain %q, got:\n%s", tt.filename, tt.want, got)
		}
	}
}

func TestGenCmdAppendsDDLHistoryOnSubsequentRuns(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users
type User struct {
	ID int64 `+"`db:\"id\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("initial GenCmd.Execute() error = %v", err)
	}

	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users
type User struct {
	ID   int64  `+"`db:\"id\"`"+`
	Name string `+"`db:\"name,size:128\"`"+`
}
`)

	stderr := new(bytes.Buffer)
	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(stderr)
	GenCmd.SetArgs([]string{".", "-v"})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("second GenCmd.Execute() error = %v", err)
	}

	postgresDDL, err := os.ReadFile(filepath.Join(dir, "postgres.sql"))
	if err != nil {
		t.Fatalf("failed to read postgres ddl: %v", err)
	}

	gotDDL := string(postgresDDL)
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "users" (`,
		`"id" BIGSERIAL PRIMARY KEY`,
		`-- Migration: `,
		`ALTER TABLE "users" ADD COLUMN "name" VARCHAR(128) NOT NULL DEFAULT '';`,
	} {
		if !strings.Contains(gotDDL, want) {
			t.Fatalf("expected postgres ddl history to contain %q, got:\n%s", want, gotDDL)
		}
	}

	if got := stderr.String(); !strings.Contains(got, "sqlite=sqlite.sql mysql=mysql.sql postgres=postgres.sql") {
		t.Fatalf("expected stderr guidance to mention schema files, got:\n%s", got)
	}
	if got := stderr.String(); !strings.Contains(got, "ddl:\n  <users>:\n    columns:\n      add column name\n") {
		t.Fatalf("expected stderr summary to mention actual ddl diff, got:\n%s", got)
	}

	stateBytes, err := os.ReadFile(filepath.Join(dir, ddlStateFilename))
	if err != nil {
		t.Fatalf("failed to read ddl state file: %v", err)
	}

	var state ddlStateFile
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		t.Fatalf("failed to parse ddl state file: %v", err)
	}
	if len(state.Records) != 1 {
		t.Fatalf("expected one incremental record after schema change, got %d", len(state.Records))
	}
	if len(state.Records[0].Tables) != 1 || state.Records[0].Tables[0].Table != "users" {
		t.Fatalf("expected record tables to be grouped by table, got %#v", state.Records[0].Tables)
	}
	if len(state.Records[0].Tables[0].Columns) != 1 || state.Records[0].Tables[0].Columns[0] != "add column name" {
		t.Fatalf("expected grouped column diff in record, got %#v", state.Records[0].Tables[0])
	}
	if state.Records[0].Sequence == "" {
		t.Fatal("expected record sequence to use time.DateTime format")
	}
	if _, err := time.Parse(time.DateTime, state.Records[0].Sequence); err != nil {
		t.Fatalf("expected record sequence to match time.DateTime, got %q: %v", state.Records[0].Sequence, err)
	}

	beforeNoChange := append([]byte(nil), postgresDDL...)
	stderr.Reset()
	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(stderr)
	GenCmd.SetArgs([]string{"-v", "."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("third GenCmd.Execute() error = %v", err)
	}

	postgresDDL, err = os.ReadFile(filepath.Join(dir, "postgres.sql"))
	if err != nil {
		t.Fatalf("failed to read postgres ddl after no-change run: %v", err)
	}
	if !bytes.Equal(postgresDDL, beforeNoChange) {
		t.Fatalf("expected no-change run to keep postgres.sql unchanged, got:\n%s", string(postgresDDL))
	}

	if got := stderr.String(); strings.Contains(got, "execute the latest dated schema section") {
		t.Fatalf("expected no-change run to skip ddl guidance, got:\n%s", got)
	}
	if got := stderr.String(); !strings.Contains(got, "ddl: no schema changes") {
		t.Fatalf("expected no-change run to report no ddl changes, got:\n%s", got)
	}

	stateBytes, err = os.ReadFile(filepath.Join(dir, ddlStateFilename))
	if err != nil {
		t.Fatalf("failed to read ddl state file after no-change run: %v", err)
	}
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		t.Fatalf("failed to parse ddl state file after no-change run: %v", err)
	}
	if len(state.Records) != 1 {
		t.Fatalf("expected no-change run to skip empty incremental record, got %d", len(state.Records))
	}
}

func TestPrintDDLChangeSummary(t *testing.T) {
	t.Run("changed", func(t *testing.T) {
		buf := new(bytes.Buffer)
		printSummaryAndWarnings(buf, ddlArtifacts{
			hasChange: true,
			recordTables: []ddlStateRecordTable{
				{
					Table:   "category",
					Columns: []string{"add column name", "drop column abc"},
					Indexes: []string{"add unique index ux_name", "drop index idx_type"},
				},
			},
		})
		if got := buf.String(); got != "ddl:\n  <category>:\n    columns:\n      add column name\n      drop column abc\n    indexes:\n      add unique index ux_name\n      drop index idx_type\n"+
			"warning: category: drop column abc is written commented out (-- DESTRUCTIVE); run it by hand once the drop is meant\n" {
			t.Fatalf("unexpected ddl summary %q", got)
		}
	})

	t.Run("table grouped order", func(t *testing.T) {
		buf := new(bytes.Buffer)
		recordTables := buildDDLRecordTables(ddlChangeSet{
			Tables: []string{"category", "item"},
			ByTable: map[string][]ddlChange{
				"category": {
					{
						kind:      ddlChangeAlterColumn,
						table:     "category",
						oldColumn: &ddlSnapshotColumn{Name: "abc", Kind: ddlColumnBool},
						newColumn: &ddlSnapshotColumn{Name: "abc", Kind: ddlColumnString},
					},
				},
				"item": {
					{
						kind:      ddlChangeAddColumn,
						table:     "item",
						newColumn: &ddlSnapshotColumn{Name: "sku"},
					},
					{
						kind:      ddlChangeDropColumn,
						table:     "item",
						oldColumn: &ddlSnapshotColumn{Name: "spu_name"},
					},
				},
			},
		})
		printSummaryAndWarnings(buf, ddlArtifacts{
			hasChange:    true,
			recordTables: recordTables,
		})
		if got := buf.String(); got != "ddl:\n  <category>:\n    columns:\n      alter column abc (type)\n  <item>:\n    columns:\n      add column sku\n      drop column spu_name\n"+
			"warning: item: drop column spu_name is written commented out (-- DESTRUCTIVE); run it by hand once the drop is meant\n" {
			t.Fatalf("unexpected grouped ddl order %q", got)
		}
		if len(recordTables) != 2 || recordTables[0].Table != "category" || recordTables[1].Table != "item" {
			t.Fatalf("unexpected record table grouping: %#v", recordTables)
		}
	})

	t.Run("new table expands columns and indexes", func(t *testing.T) {
		recordTables := buildDDLRecordTables(ddlChangeSet{
			Tables: []string{"new_table"},
			ByTable: map[string][]ddlChange{
				"new_table": {
					{
						kind:  ddlChangeCreateTable,
						table: "new_table",
						newTable: &ddlSnapshotTable{
							Name: "new_table",
							Columns: []ddlSnapshotColumn{
								{Name: "id"},
								{Name: "name"},
							},
							Indexes: []ddlSnapshotIndex{
								{Name: "ux_name", Unique: true},
								{Name: "idx_name"},
							},
						},
					},
				},
			},
		})
		if len(recordTables) != 1 {
			t.Fatalf("expected one record table, got %#v", recordTables)
		}
		if got := recordTables[0].Columns; strings.Join(got, ",") != "create table" {
			t.Fatalf("unexpected create table columns %#v", got)
		}
		if got := recordTables[0].Indexes; len(got) != 0 {
			t.Fatalf("unexpected create table indexes %#v", got)
		}

		buf := new(bytes.Buffer)
		printSummaryAndWarnings(buf, ddlArtifacts{
			hasChange:    true,
			recordTables: recordTables,
		})
		if got := buf.String(); got != "ddl:\n  <new_table>:\n    create table\n" {
			t.Fatalf("unexpected create table summary %q", got)
		}
	})

	t.Run("drop table is single line", func(t *testing.T) {
		buf := new(bytes.Buffer)
		printSummaryAndWarnings(buf, ddlArtifacts{
			hasChange: true,
			recordTables: []ddlStateRecordTable{
				{Table: "new_table", Columns: []string{"drop table"}},
			},
		})
		if got := buf.String(); got != "ddl:\n  <new_table>:\n    drop table\n"+
			"warning: new_table: drop table is written commented out (-- DESTRUCTIVE); run it by hand once the drop is meant\n" {
			t.Fatalf("unexpected drop table summary %q", got)
		}
	})

	t.Run("unchanged", func(t *testing.T) {
		buf := new(bytes.Buffer)
		printDDLChangeSummary(buf, ddlArtifacts{})
		if got := buf.String(); got != "ddl: no schema changes\n" {
			t.Fatalf("unexpected ddl summary %q", got)
		}
	})
}

func TestGenCmdAppendsSQLiteRebuildDDLForTypeChange(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users
type User struct {
	ID   int64 `+"`db:\"id\"`"+`
	Name int64 `+"`db:\"name\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("initial GenCmd.Execute() error = %v", err)
	}

	initial, err := os.ReadFile(filepath.Join(dir, "sqlite.sql"))
	if err != nil {
		t.Fatal(err)
	}

	// The name changes type, forcing the rebuild, and a NOT NULL column without a
	// default arrives, which the copy fills with the zero value.
	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users
type User struct {
	ID    int64  `+"`db:\"id\"`"+`
	Name  string `+"`db:\"name,size:128\"`"+`
	Email string `+"`db:\"email\"`"+`
}
`)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("second GenCmd.Execute() error = %v", err)
	}

	sqliteDDL, err := os.ReadFile(filepath.Join(dir, "sqlite.sql"))
	if err != nil {
		t.Fatalf("failed to read sqlite ddl: %v", err)
	}

	got := string(sqliteDDL)
	for _, want := range []string{
		`-- Migration: `,
		`-- users: email is NOT NULL without a default; existing rows get ''`,
		`PRAGMA foreign_keys = OFF;`,
		`CREATE TABLE IF NOT EXISTS "__tsq_new_users" (`,
		`INSERT INTO "__tsq_new_users" ("id", "email", "name") SELECT "id", '', "name" FROM "users";`,
		`INSERT INTO sqlite_sequence (name, seq) SELECT '__tsq_new_users', seq FROM sqlite_sequence WHERE name = 'users';`,
		`DROP TABLE "users";`,
		`ALTER TABLE "__tsq_new_users" RENAME TO "users";`,
		`COMMIT;`,
		`PRAGMA foreign_keys = ON;`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected sqlite incremental ddl to contain %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, ";;") {
		t.Fatalf("expected sqlite ddl history to avoid duplicate semicolons, got:\n%s", got)
	}

	// Run the migration the way a user would: the sqlite3 shell, which does not
	// stop at an error. The copy used to fail on the NOT NULL column and the shell
	// then dropped the table with its rows.
	shell, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Skip("sqlite3 shell not installed")
	}

	db := filepath.Join(dir, "t.db")
	migration := got[strings.Index(got, "-- Migration: "):]

	for _, script := range []string{
		string(initial),
		`INSERT INTO users (name) VALUES (1), (2), (3); DELETE FROM users WHERE id = 3;`,
		migration,
	} {
		if out, err := runSQLiteShell(shell, db, script); err != nil || strings.Contains(out, "Error") {
			t.Fatalf("sqlite3: %v\n%s", err, out)
		}
	}

	out, err := runSQLiteShell(shell, db, `INSERT INTO users (name, email) VALUES ('x', 'x@y'); SELECT count(*), max(id) FROM users;`)
	if err != nil || strings.TrimSpace(out) != "3|4" {
		t.Fatalf("after the rebuild: %q, %v; want the 2 rows kept, and key 4 rather than the deleted row's 3", out, err)
	}
}

func TestGenCmdGeneratesMySQLSafeDDLForLargeStringsAndIndexes(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

//tsq:table name=task pk=ID
//tsq:index State
type Task struct {
	ID     int64  `+"`db:\"id\"`"+`
	State  string `+"`db:\"state,size:32\"`"+`
	Params string `+"`db:\"params,size:20000\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() error = %v", err)
	}

	mysqlDDL, err := os.ReadFile(filepath.Join(dir, "mysql.sql"))
	if err != nil {
		t.Fatalf("failed to read mysql ddl: %v", err)
	}

	got := string(mysqlDDL)
	for _, want := range []string{
		"`params` MEDIUMTEXT NOT NULL",
		"ALTER TABLE `task` ADD INDEX `idx_task_state`(`state`);",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected mysql ddl to contain %q, got:\n%s", want, got)
		}
	}
}

func TestGenCmdUsesReasonableDDLTypesForDefaultStringsAndAliases(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

import (
	"database/sql"
	"time"
)

type AliasString = string
type NamedString string
type AliasNullString = sql.NullString
type AliasInt = int
type NamedInt int
type AliasInt32 = int32
type NamedInt32 int32
type AliasUint = uint
type NamedUint uint
type AliasBool = bool
type AliasBytes = []byte
type AliasTime = time.Time

//tsq:table name=artifacts pk=ID
type Artifact struct {
	ID         int64           `+"`db:\"id\"`"+`
	Name       string          `+"`db:\"name\"`"+`
	AliasName  AliasString     `+"`db:\"alias_name\"`"+`
	NamedName  NamedString     `+"`db:\"named_name\"`"+`
	Note       sql.NullString  `+"`db:\"note\"`"+`
	AliasNote  AliasNullString `+"`db:\"alias_note\"`"+`
	Rank       AliasInt        `+"`db:\"rank\"`"+`
	NamedRank  NamedInt        `+"`db:\"named_rank\"`"+`
	Count      AliasInt32      `+"`db:\"count\"`"+`
	NamedCount NamedInt32      `+"`db:\"named_count\"`"+`
	Flags      AliasUint       `+"`db:\"flags\"`"+`
	NamedFlags NamedUint       `+"`db:\"named_flags\"`"+`
	LegacyID   sql.NullInt64   `+"`db:\"legacy_id\"`"+`
	Enabled    AliasBool       `+"`db:\"enabled\"`"+`
	Payload    AliasBytes      `+"`db:\"payload\"`"+`
	CreatedAt  AliasTime       `+"`db:\"created_at\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() error = %v", err)
	}

	tests := []struct {
		name  string
		file  string
		wants []string
	}{
		{
			name: "mysql",
			file: "mysql.sql",
			wants: []string{
				"`name` VARCHAR(255) NOT NULL",
				"`alias_name` VARCHAR(255) NOT NULL",
				"`named_name` VARCHAR(255) NOT NULL",
				"`note` VARCHAR(255)",
				"`alias_note` VARCHAR(255)",
				"`rank` INT NOT NULL",
				"`named_rank` INT NOT NULL",
				"`count` INT NOT NULL",
				"`named_count` INT NOT NULL",
				"`flags` INT UNSIGNED NOT NULL",
				"`named_flags` INT UNSIGNED NOT NULL",
				"`legacy_id` BIGINT",
				"`enabled` BOOLEAN NOT NULL",
				"`payload` BLOB NOT NULL",
				"`created_at` DATETIME(6) NOT NULL",
			},
		},
		{
			name: "postgres",
			file: "postgres.sql",
			wants: []string{
				`"name" VARCHAR(255) NOT NULL`,
				`"alias_name" VARCHAR(255) NOT NULL`,
				`"named_name" VARCHAR(255) NOT NULL`,
				`"note" VARCHAR(255)`,
				`"alias_note" VARCHAR(255)`,
				`"rank" INTEGER NOT NULL`,
				`"named_rank" INTEGER NOT NULL`,
				`"count" INTEGER NOT NULL`,
				`"named_count" INTEGER NOT NULL`,
				// PostgreSQL has no unsigned types: a uint takes the next wider one.
				`"flags" BIGINT NOT NULL`,
				`"named_flags" BIGINT NOT NULL`,
				`"legacy_id" BIGINT`,
				`"enabled" BOOLEAN NOT NULL`,
				`"payload" BYTEA NOT NULL`,
				`"created_at" TIMESTAMP NOT NULL`,
			},
		},
		{
			name: "sqlite",
			file: "sqlite.sql",
			wants: []string{
				`"name" VARCHAR(255) NOT NULL`,
				`"alias_name" VARCHAR(255) NOT NULL`,
				`"named_name" VARCHAR(255) NOT NULL`,
				`"note" VARCHAR(255)`,
				`"alias_note" VARCHAR(255)`,
				`"rank" INTEGER NOT NULL`,
				`"named_rank" INTEGER NOT NULL`,
				`"count" INTEGER NOT NULL`,
				`"named_count" INTEGER NOT NULL`,
				`"flags" INTEGER NOT NULL`,
				`"named_flags" INTEGER NOT NULL`,
				`"legacy_id" INTEGER`,
				`"enabled" BOOLEAN NOT NULL`,
				`"payload" BLOB NOT NULL`,
				`"created_at" TIMESTAMP NOT NULL`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(dir, tt.file))
			if err != nil {
				t.Fatalf("failed to read %s: %v", tt.file, err)
			}

			got := string(content)
			for _, want := range tt.wants {
				if !strings.Contains(got, want) {
					t.Fatalf("expected %s to contain %q, got:\n%s", tt.file, want, got)
				}
			}
		})
	}

	stateBytes, err := os.ReadFile(filepath.Join(dir, ddlStateFilename))
	if err != nil {
		t.Fatalf("failed to read ddl state file: %v", err)
	}

	var state ddlStateFile
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		t.Fatalf("failed to parse ddl state file: %v", err)
	}

	var columns map[string]ddlSnapshotColumn
	for _, table := range state.Snapshot.Tables {
		if table.Name != "artifacts" {
			continue
		}

		columns = make(map[string]ddlSnapshotColumn, len(table.Columns))
		for _, column := range table.Columns {
			columns[column.Name] = column
		}
	}

	if columns == nil {
		t.Fatal("expected artifacts table in ddl snapshot")
	}

	for _, name := range []string{"name", "alias_name", "named_name", "note", "alias_note"} {
		if got := columns[name].Size; got != 255 {
			t.Fatalf("expected ddl snapshot to record default string size 255 for %s, got %d", name, got)
		}
	}
	for _, name := range []string{"rank", "named_rank", "count", "named_count", "flags", "named_flags"} {
		if got := columns[name].Bits; got != 32 {
			t.Fatalf("expected %s to keep 32-bit width, got %d", name, got)
		}
	}
	if got := columns["flags"].Unsigned; !got {
		t.Fatal("expected uint alias to keep unsigned metadata")
	}
	if got := columns["named_flags"].Unsigned; !got {
		t.Fatal("expected named uint to keep unsigned metadata")
	}
	if got := columns["legacy_id"].Bits; got != 64 {
		t.Fatalf("expected explicit 64-bit nullable integer to keep 64-bit width, got %d", got)
	}
	if got := columns["count"].Bits; got != 32 {
		t.Fatalf("expected int32 alias to keep 32-bit width, got %d", got)
	}
	if got := columns["named_count"].Bits; got != 32 {
		t.Fatalf("expected named int32 to keep 32-bit width, got %d", got)
	}
	if got := columns["payload"].Kind; got != ddlColumnBytes {
		t.Fatalf("expected []byte alias to map to bytes kind, got %s", got)
	}
	if got := columns["created_at"].Kind; got != ddlColumnTime {
		t.Fatalf("expected time alias to map to time kind, got %s", got)
	}
}

func TestGenCmdSupportsExplicitDDLTypeOverrideForUnsupportedCustomTypes(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type SkillItem struct {
	Name string `+"`json:\"name\"`"+`
}

type SkillItems []*SkillItem

func (s SkillItems) Value() (driver.Value, error) {
	bs, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return string(bs), nil
}

func (s *SkillItems) Scan(src any) error {
	switch value := src.(type) {
	case []byte:
		return json.Unmarshal(value, s)
	case string:
		return json.Unmarshal([]byte(value), s)
	case nil:
		*s = nil
		return nil
	default:
		return fmt.Errorf("unsupported scan type %T", src)
	}
}

//tsq:table name=profile pk=ID
type Profile struct {
	ID         int64      `+"`db:\"id\"`"+`
	Skills     SkillItems `+"`db:\"skill_items,type:JSON\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() error = %v", err)
	}

	for _, filename := range []string{"mysql.sql", "postgres.sql", "sqlite.sql"} {
		content, err := os.ReadFile(filepath.Join(dir, filename))
		if err != nil {
			t.Fatalf("failed to read %s: %v", filename, err)
		}
		if !strings.Contains(string(content), `skill_items`) || !strings.Contains(string(content), ` JSON NOT NULL`) {
			t.Fatalf("expected %s to keep explicit JSON type, got:\n%s", filename, string(content))
		}
	}

	tableSource, err := os.ReadFile(filepath.Join(dir, "profile.tsq.go"))
	if err != nil {
		t.Fatalf("failed to read profile.tsq.go: %v", err)
	}
	if !strings.Contains(string(tableSource), `RawType: "JSON"`) {
		t.Fatalf("expected profile.tsq.go to keep explicit raw type, got:\n%s", string(tableSource))
	}

	stateBytes, err := os.ReadFile(filepath.Join(dir, ddlStateFilename))
	if err != nil {
		t.Fatalf("failed to read ddl state file: %v", err)
	}

	var state ddlStateFile
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		t.Fatalf("failed to parse ddl state file: %v", err)
	}

	found := false
	for _, table := range state.Snapshot.Tables {
		if table.Name != "profile" {
			continue
		}

		for _, column := range table.Columns {
			if column.Name != "skill_items" {
				continue
			}

			found = true
			if column.RawType != "JSON" {
				t.Fatalf("expected ddl snapshot raw_type JSON, got %q", column.RawType)
			}
		}
	}

	if !found {
		t.Fatal("expected profile.skill_items column in ddl snapshot")
	}
}

func TestGenCmdReportsDSLSourceLocation(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

//tsq:table name=users
//tsq:unique Nickname
type User struct {
	ID int64 `+"`db:\"id\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})

	err := GenCmd.Execute()
	if err == nil {
		t.Fatal("expected invalid DSL field to fail generation")
	}

	got := err.Error()
	if !strings.Contains(got, "model.go:4") {
		t.Fatalf("expected gen error to point at the offending directive line, got %q", got)
	}
	if !strings.Contains(got, "name Go fields, not columns") {
		t.Fatalf("expected gen error to keep field guidance, got %q", got)
	}
}

func TestGenerationPlanStatusFor(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "user.tsq.go")
	src := []byte("// Code generated by tsq-test. DO NOT EDIT.\npackage example\n")

	status, err := generationPlanStatusFor(target, src)
	if err != nil {
		t.Fatalf("expected missing file to plan as create, got %v", err)
	}
	if status != generationPlanCreate {
		t.Fatalf("expected create status, got %s", status)
	}

	if err := os.WriteFile(target, src, 0o644); err != nil {
		t.Fatalf("failed to seed generated file: %v", err)
	}

	status, err = generationPlanStatusFor(target, src)
	if err != nil {
		t.Fatalf("expected unchanged file to plan cleanly, got %v", err)
	}
	if status != generationPlanUnchanged {
		t.Fatalf("expected unchanged status, got %s", status)
	}

	updated := []byte("// Code generated by tsq-test. DO NOT EDIT.\npackage changed\n")
	status, err = generationPlanStatusFor(target, updated)
	if err != nil {
		t.Fatalf("expected generated file to plan as update, got %v", err)
	}
	if status != generationPlanUpdate {
		t.Fatalf("expected update status, got %s", status)
	}
}

func TestGenerationPlanStatusForRejectsNonGeneratedOverwrite(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "user.tsq.go")
	if err := os.WriteFile(target, []byte("package example\n"), 0o644); err != nil {
		t.Fatalf("failed to seed non-generated file: %v", err)
	}

	_, err := generationPlanStatusFor(target, []byte("// Code generated by tsq-test. DO NOT EDIT.\npackage example\n"))
	if err == nil {
		t.Fatal("expected non-generated overwrite planning to fail")
	}
}

func TestGenCheckReportsOutdatedFiles(t *testing.T) {
	err := ensureGenerationPlanUpToDate([]generationPlanEntry{
		{Filename: "user.tsq.go", Status: generationPlanUpdate},
		{Filename: "org.tsq.go", Status: generationPlanCreate},
		{Filename: "item.tsq.go", Status: generationPlanUnchanged},
	})
	if !errors.Is(err, ErrOutOfDate) {
		t.Fatalf("check = %v; want ErrOutOfDate, which the tsq command exits 2 for", err)
	}

	got := err.Error()
	for _, want := range []string{"generated files are out of date", "UPDATE user.tsq.go", "CREATE org.tsq.go"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected check error to mention %q, got %q", want, got)
		}
	}
}

func TestGenDryRunPrintsStatuses(t *testing.T) {
	buf := new(bytes.Buffer)
	printGenerationPlan(buf, []generationPlanEntry{
		{Filename: "user.tsq.go", Status: generationPlanCreate},
		{Filename: "org.tsq.go", Status: generationPlanUpdate},
		{Filename: "item.tsq.go", Status: generationPlanUnchanged},
	})

	got := buf.String()
	for _, want := range []string{"CREATE user.tsq.go", "UPDATE org.tsq.go", "UNCHANGED item.tsq.go"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected dry-run output to mention %q, got %q", want, got)
		}
	}
}

func TestBuildGenerationPlanDetectsStaleGeneratedFiles(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "orphan.tsq.go")
	if err := os.WriteFile(stale, []byte("// Code generated by tsq-test. DO NOT EDIT.\npackage example\n"), 0o644); err != nil {
		t.Fatalf("failed to seed stale generated file: %v", err)
	}

	plan, err := buildGenerationPlan(nil, dir)
	if err != nil {
		t.Fatalf("expected stale file scan to succeed, got %v", err)
	}

	if len(plan) != 1 {
		t.Fatalf("expected one stale plan entry, got %d", len(plan))
	}
	if plan[0].Status != generationPlanStale {
		t.Fatalf("expected stale plan status, got %s", plan[0].Status)
	}
	if plan[0].Filename != stale {
		t.Fatalf("expected stale filename %q, got %q", stale, plan[0].Filename)
	}
}

func TestBuildGenerationPlanIgnoresNonGeneratedTsqFiles(t *testing.T) {
	dir := t.TempDir()
	manual := filepath.Join(dir, "manual.tsq.go")
	if err := os.WriteFile(manual, []byte("package example\n"), 0o644); err != nil {
		t.Fatalf("failed to seed manual tsq-named file: %v", err)
	}

	plan, err := buildGenerationPlan(nil, dir)
	if err != nil {
		t.Fatalf("expected plan build to ignore manual tsq-named files, got %v", err)
	}
	if len(plan) != 0 {
		t.Fatalf("expected no plan entries for manual tsq-named files, got %d", len(plan))
	}
}

func TestGenCheckReportsStaleGeneratedFiles(t *testing.T) {
	err := ensureGenerationPlanUpToDate([]generationPlanEntry{
		{Filename: "orphan.tsq.go", Status: generationPlanStale},
	})
	if err == nil {
		t.Fatal("expected stale generated files to fail check")
	}
	if !strings.Contains(err.Error(), "STALE orphan.tsq.go") {
		t.Fatalf("expected stale generated file in error, got %q", err.Error())
	}
}

func TestGenDryRunPrintsStaleStatuses(t *testing.T) {
	buf := new(bytes.Buffer)
	printGenerationPlan(buf, []generationPlanEntry{
		{Filename: "orphan.tsq.go", Status: generationPlanStale},
	})

	if !strings.Contains(buf.String(), "STALE orphan.tsq.go") {
		t.Fatalf("expected dry-run output to mention stale file, got %q", buf.String())
	}
}

func TestValidateResultFieldsRejectsUnknownTargetField(t *testing.T) {
	dto := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true},
		TypeInfo:  genmodel.TypeInfo{TypeName: "UserResult"},
		Fields: []genmodel.FieldInfo{
			{Name: "UserName", Column: "User.Missing"},
		},
	}

	structsByName := map[string]*genmodel.StructInfo{
		"User": {
			TableMeta: &genmodel.TableMeta{Table: "user"},
			FieldsByName: map[string]genmodel.FieldInfo{
				"PK": {Name: "PK", Column: "id"},
			},
		},
	}

	if err := validateResultFields(dto, structsByName); err == nil {
		t.Fatal("expected invalid Result reference to return an error")
	}
}

func TestValidateResultFieldsRejectsNormalizedReferenceCollisions(t *testing.T) {
	dto := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true},
		TypeInfo:  genmodel.TypeInfo{TypeName: "UserResult"},
		Fields: []genmodel.FieldInfo{
			{Name: "A", Column: "User.Profile_ID"},
			{Name: "B", Column: "User_Profile.ID"},
		},
	}

	structsByName := map[string]*genmodel.StructInfo{
		"User": {
			TableMeta: &genmodel.TableMeta{Table: "user"},
			FieldsByName: map[string]genmodel.FieldInfo{
				"Profile_ID": {Name: "Profile_ID", Column: "profile_id"},
			},
		},
		"User_Profile": {
			TableMeta: &genmodel.TableMeta{Table: "user_profile"},
			FieldsByName: map[string]genmodel.FieldInfo{
				"ID": {Name: "ID", Column: "id"},
			},
		},
	}

	if err := validateResultFields(dto, structsByName); err == nil {
		t.Fatal("expected normalized Result reference collision to return an error")
	}
}

// TestGenResultsTakeNullableForms covers a result field reading the outer side of
// a LEFT JOIN: it has to hold NULL although the column is NOT NULL, and the
// generator refused anything but the column's own type. A type that holds neither
// is still refused.
func TestGenResultsTakeNullableForms(t *testing.T) {
	module := func(field string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\nimport (\n\t\"database/sql\"\n\t\"time\"\n)\n\nvar _ sql.Null[int]\nvar _ time.Time\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n\tName string `db:\"name\"`\n\tAt time.Time `db:\"at\"`\n}\n\n//tsq:result\ntype View struct {\n\t" + field + "\n}\n"}
	}

	for _, field := range []string{
		"Name string `tsq:\"Row.Name\"`",
		"Name sql.Null[string] `tsq:\"Row.Name\"`",
		"Name *string `tsq:\"Row.Name\"`",
		"At sql.NullTime `tsq:\"Row.At\"`",
	} {
		t.Run(field, func(t *testing.T) {
			if err := genModule(t, module(field)); err != nil {
				t.Fatalf("tsq gen = %v", err)
			}

			tidyGenTestModule(t)

			if output, err := exec.Command("go", "build", "./...").CombinedOutput(); err != nil {
				t.Fatalf("generated code does not compile: %v\n%s", err, output)
			}
		})
	}

	for _, field := range []string{"At string `tsq:\"Row.At\"`", "Name sql.Null[int64] `tsq:\"Row.Name\"`"} {
		t.Run(field, func(t *testing.T) {
			if err := genModule(t, module(field)); err == nil || !strings.Contains(err.Error(), "cannot hold Row.") {
				t.Fatalf("tsq gen = %v; want it refused", err)
			}
		})
	}
}

func TestNormalizeResultColumnsUpdatesFieldMap(t *testing.T) {
	dto := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true},
		Fields: []genmodel.FieldInfo{
			{Name: "UserID", Column: "User.ID"},
		},
		FieldsByName: map[string]genmodel.FieldInfo{
			"UserID": {Name: "UserID", Column: "User.ID"},
		},
	}

	normalizeResultColumns(dto)

	if got := dto.Fields[0].Column; got != "TableUser.ID" {
		t.Fatalf("expected Result field column to be normalized, got %q", got)
	}

	if got := dto.FieldsByName["UserID"].Column; got != "TableUser.ID" {
		t.Fatalf("expected Result field map column to be normalized, got %q", got)
	}
}

func TestResultTemplateGeneratesProjectionOnlyResultFile(t *testing.T) {
	dir := t.TempDir()
	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true},
		TypeInfo: genmodel.TypeInfo{
			Package:  genmodel.PackageInfo{Name: "gentest"},
			TypeName: "UserOrder",
		},
		Receiver:   "uo",
		TSQVersion: "test",
		Fields: []genmodel.FieldInfo{
			{Name: "UserID", Type: genmodel.TypeInfo{TypeName: "int64"}, Column: "User_ID", JSONTag: "user_id"},
		},
	}

	tpl, err := template.New("result.go.tmpl").Funcs(funcMap()).Parse(defaultResultTpl)
	if err != nil {
		t.Fatalf("parse Result template: %v", err)
	}

	if err := genResult(data, tpl, dir); err != nil {
		t.Fatalf("render Result template: %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "userorder.result.tsq.go"))
	if err != nil {
		t.Fatalf("read generated Result file: %v", err)
	}

	rendered := string(contents)
	for _, want := range []string{
		"type UserOrderResult struct {",
		"var ResultUserOrder = UserOrderResult{",
		"UserID: tsq.MapInto(",
		"func (r UserOrderResult) Columns() []tsq.BoundColumn[UserOrder] {",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected generated Result file to contain %q, got:\n%s", want, rendered)
		}
	}

	for _, blocked := range []string{
		"UserOrder__Cols",
		"LeftJoinOrder(",
		"SelectUserOrder(",
		"WhereUser(",
		"GroupByUser(",
		"KwSearchUser(",
		"HavingUser(",
		"JoinOn[",
		"JoinCond[",
	} {
		if strings.Contains(rendered, blocked) {
			t.Fatalf("generated Result file contains removed API %q:\n%s", blocked, rendered)
		}
	}
}

func TestValidateGeneratedFilenameCollisionsRejectsCaseConflicts(t *testing.T) {
	list := []*genmodel.StructInfo{
		{
			TableMeta: &genmodel.TableMeta{Table: "user"},
			TypeInfo:  genmodel.TypeInfo{TypeName: "User"},
			Fields:    []genmodel.FieldInfo{{Name: "PK"}},
		},
		{
			TableMeta: &genmodel.TableMeta{Table: "user_lower"},
			TypeInfo:  genmodel.TypeInfo{TypeName: "user"},
			Fields:    []genmodel.FieldInfo{{Name: "PK"}},
		},
	}

	if err := validateGeneratedFilenameCollisions(list); err == nil {
		t.Fatal("expected case-insensitive filename collision to return an error")
	}
}

func TestValidateDatabaseFilledFieldsRefusesManagedColumns(t *testing.T) {
	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table:          "user",
			PrimaryKey:     "ID",
			CreatedAtField: "CreatedAt",
		},
		Schema: []genmodel.SchemaColumn{
			{Name: "id", Fill: "generated"},
		},
		Fields: []genmodel.FieldInfo{
			{Name: "ID", Column: "id"},
			{Name: "CreatedAt", Column: "created_at"},
		},
	}

	if err := validateDatabaseFilledFields(data); err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("generated primary key = %v", err)
	}

	data.Schema = []genmodel.SchemaColumn{{Name: "created_at", Fill: "default"}}
	if err := validateDatabaseFilledFields(data); err == nil || !strings.Contains(err.Error(), "created_at") {
		t.Fatalf("defaulted created_at = %v", err)
	}

	data.Schema = []genmodel.SchemaColumn{{Name: "nickname", Fill: "default"}}
	if err := validateDatabaseFilledFields(data); err != nil {
		t.Fatalf("an ordinary column = %v", err)
	}
}

func TestGeneratedColumnRendersTheSameOnEveryDialect(t *testing.T) {
	column := tsqdialect.ColumnSpec{
		Name:      "slug",
		Type:      tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 160},
		Fill:      tsqdialect.FillGenerated,
		Generated: "LOWER(title)",
	}

	for _, spec := range ddlDialects {
		got, err := renderDDLColumnSpec(spec.dialect, column)
		if err != nil || !strings.HasSuffix(got, "GENERATED ALWAYS AS (LOWER(title)) STORED") || strings.Contains(got, "NOT NULL") {
			t.Errorf("%s: %q, %v", spec.dialect.Name(), got, err)
		}
	}
}

// TestGenSearchesNamedStringTypes covers //tsq:search and //tsq:fulltext over a
// named string type, which tsq.Searchable takes but the generator refused because
// it compared type names; anything that is not text is still refused.
func TestGenSearchesNamedStringTypes(t *testing.T) {
	module := func(field string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\ntype Slug string\n\n//tsq:table\n//tsq:search Label\n//tsq:fulltext Label\ntype Row struct {\n\tID int64 `db:\"id\"`\n\tLabel " + field + " `db:\"label\"`\n}\n"}
	}

	t.Run("named string", func(t *testing.T) {
		if err := genModule(t, module("Slug")); err != nil {
			t.Fatalf("tsq gen = %v", err)
		}

		tidyGenTestModule(t)

		if output, err := exec.Command("go", "build", "./...").CombinedOutput(); err != nil {
			t.Fatalf("generated code does not compile: %v\n%s", err, output)
		}
	})

	t.Run("integer", func(t *testing.T) {
		if err := genModule(t, module("int64")); err == nil || !strings.Contains(err.Error(), "search field Label is not a string") {
			t.Fatalf("tsq gen = %v; want it refused", err)
		}
	})
}

func TestValidateIndexNameCollisionsRejectsCrossTableReuse(t *testing.T) {
	list := []*genmodel.StructInfo{
		{
			TableMeta: &genmodel.TableMeta{
				Table:   "user",
				Uniques: []genmodel.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
			},
		},
		{
			TableMeta: &genmodel.TableMeta{
				Table:   "org",
				Uniques: []genmodel.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
			},
		},
	}

	if err := validateIndexNameCollisions(list); err == nil {
		t.Fatal("expected reused index name across tables to return an error")
	}
}

func TestValidateStructForGenerationRejectsPointerPrimaryKeys(t *testing.T) {
	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table:      "user",
			PrimaryKey: "PK",
		},
		TypeInfo: genmodel.TypeInfo{TypeName: "User"},
		FieldsByName: map[string]genmodel.FieldInfo{
			"PK": {
				Name:      "PK",
				IsPointer: true,
				Type:      genmodel.TypeInfo{TypeName: "string"},
			},
		},
	}

	if err := validateStructForGeneration(data, nil); err == nil {
		t.Fatal("expected pointer primary key to be rejected")
	}
}

func TestValidateStructForGenerationRejectsSlicePrimaryKeys(t *testing.T) {
	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table:      "blob_user",
			PrimaryKey: "PK",
		},
		TypeInfo: genmodel.TypeInfo{TypeName: "BlobUser"},
		FieldsByName: map[string]genmodel.FieldInfo{
			"PK": {
				Name:    "PK",
				IsSlice: true,
				Type:    genmodel.TypeInfo{TypeName: "byte"},
			},
		},
	}

	if err := validateStructForGeneration(data, nil); err == nil {
		t.Fatal("expected slice primary key to be rejected")
	}
}

func TestTableTemplateAvoidsKeywordParameterNames(t *testing.T) {
	dir := t.TempDir()

	tpl, err := template.New("table.go.tmpl").Funcs(funcMap()).Parse(defaultTableTpl)
	if err != nil {
		t.Fatalf("failed to parse table template: %v", err)
	}

	field := genmodel.FieldInfo{Name: "Type", Column: "type", JSONTag: "type", Type: genmodel.TypeInfo{TypeName: "int64"}}
	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table:         "keyworded",
			PrimaryKey:    "Type",
			AutoIncrement: true,
			Uniques:       []genmodel.IndexInfo{{Name: "ux_keyworded_type", Fields: []string{"Type"}}},
		},
		TypeInfo: genmodel.TypeInfo{Package: genmodel.PackageInfo{Name: "example"}, TypeName: "Keyworded"},
		Fields:   []genmodel.FieldInfo{field},
		FieldsByName: map[string]genmodel.FieldInfo{
			"Type": field,
		},
		Receiver:   "k",
		TSQVersion: "test",
	}

	if err := gen(data, tpl, dir); err != nil {
		t.Fatalf("expected template with keyword field to render valid Go, got %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "keyworded.tsq.go"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	rendered := string(contents)
	if !strings.Contains(rendered, "types ...int64") || !strings.Contains(rendered, "type_ int64") {
		t.Fatalf("expected generated parameter to avoid Go keyword, got:\n%s", rendered)
	}

	if strings.Contains(rendered, "\ttype ...int64") {
		t.Fatalf("generated code still contains keyword parameter:\n%s", rendered)
	}
}

func TestTableTemplateGeneratesNoQueriesForPlainIndexes(t *testing.T) {
	dir := t.TempDir()

	tpl, err := template.New("table.go.tmpl").Funcs(funcMap()).Parse(defaultTableTpl)
	if err != nil {
		t.Fatalf("failed to parse table template: %v", err)
	}

	idField := genmodel.FieldInfo{Name: "PK", Column: "id", JSONTag: "id", Type: genmodel.TypeInfo{TypeName: "int64"}}
	orgField := genmodel.FieldInfo{Name: "OrgID", Column: "org_id", JSONTag: "org_id", Type: genmodel.TypeInfo{TypeName: "int64"}}
	itemField := genmodel.FieldInfo{Name: "ItemID", Column: "item_id", JSONTag: "item_id", Type: genmodel.TypeInfo{TypeName: "int64"}}

	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table:      "order",
			PrimaryKey: "PK",
			Indexes: []genmodel.IndexInfo{
				{Name: "idx_order_org_item", Fields: []string{"OrgID", "ItemID"}},
			},
		},
		TypeInfo: genmodel.TypeInfo{Package: genmodel.PackageInfo{Name: "example"}, TypeName: "Order"},
		Fields:   []genmodel.FieldInfo{idField, orgField, itemField},
		FieldsByName: map[string]genmodel.FieldInfo{
			"PK":     idField,
			"OrgID":  orgField,
			"ItemID": itemField,
		},
		Receiver:   "o",
		TSQVersion: "test",
	}

	if err := gen(data, tpl, dir); err != nil {
		t.Fatalf("expected query list template to render valid Go, got %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "order.tsq.go"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	// A plain index is a schema object only: lookups on it are written by hand.
	rendered := string(contents)
	if strings.Contains(rendered, "QueryOrderByOrgID") {
		t.Fatalf("did not expect a query for a plain index, got:\n%s", rendered)
	}

	if !strings.Contains(rendered, `{Name: "idx_order_org_item", Columns: []string{"org_id", "item_id"}}`) {
		t.Fatalf("expected the index in the table spec, got:\n%s", rendered)
	}
}

func TestTableTemplateGeneratesFullUniqueIndexInHelpers(t *testing.T) {
	dir := t.TempDir()

	tpl, err := template.New("table.go.tmpl").Funcs(funcMap()).Parse(defaultTableTpl)
	if err != nil {
		t.Fatalf("failed to parse table template: %v", err)
	}

	idField := genmodel.FieldInfo{Name: "ID", Column: "id", JSONTag: "id", Type: genmodel.TypeInfo{TypeName: "int64"}}
	emailField := genmodel.FieldInfo{Name: "Email", Column: "email", JSONTag: "email", Type: genmodel.TypeInfo{TypeName: "string"}}
	orgField := genmodel.FieldInfo{Name: "OrgID", Column: "org_id", JSONTag: "org_id", Type: genmodel.TypeInfo{TypeName: "int64"}}
	slugField := genmodel.FieldInfo{Name: "Slug", Column: "slug", JSONTag: "slug", Type: genmodel.TypeInfo{TypeName: "string"}}

	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table:      "user",
			PrimaryKey: "ID",
			Uniques: []genmodel.IndexInfo{
				{Name: "ux_user_email", Fields: []string{"Email"}},
				{Name: "ux_user_org_slug", Fields: []string{"OrgID", "Slug"}},
			},
		},
		TypeInfo: genmodel.TypeInfo{Package: genmodel.PackageInfo{Name: "example"}, TypeName: "User"},
		Fields:   []genmodel.FieldInfo{idField, emailField, orgField, slugField},
		FieldsByName: map[string]genmodel.FieldInfo{
			"ID":    idField,
			"Email": emailField,
			"OrgID": orgField,
			"Slug":  slugField,
		},
		Receiver:   "u",
		TSQVersion: "test",
	}

	if err := gen(data, tpl, dir); err != nil {
		t.Fatalf("expected unique index IN helpers to render valid Go, got %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "user.tsq.go"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	rendered := string(contents)
	for _, want := range []string{
		"func (t UserTable) GetByEmail(",
		"func (t UserTable) FetchByEmail(",
		"return t.FetchBy(ctx, db, t.Email, emails)",
		"func (t UserTable) GetByOrgIDAndSlug(",
		"orgID int64,",
		"func (t UserTable) FetchByOrgIDAndSlug(",
		"return t.FetchBy(ctx, db, t.Slug, slugs, t.OrgID.EQ(tsq.Val(orgID)))",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected generated unique index IN code to mention %q, got:\n%s", want, rendered)
		}
	}

	// Index prefixes do not identify a row, so they get no generated query.
	if strings.Contains(rendered, "GetByOrgID(") || strings.Contains(rendered, "FetchByOrgID(") {
		t.Fatalf("did not expect a query on a unique index prefix, got:\n%s", rendered)
	}
}

func TestGenDoesNotWriteBrokenGoOnFormatError(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user.tsq.go")
	if err := os.WriteFile(target, []byte("// existing\n"), 0o644); err != nil {
		t.Fatalf("failed to seed generated file: %v", err)
	}

	tpl, err := template.New("broken").Parse("package {{.TypeInfo.Package.Name}}\nfunc {")
	if err != nil {
		t.Fatalf("failed to parse broken template: %v", err)
	}

	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{Table: "user"},
		TypeInfo:  genmodel.TypeInfo{Package: genmodel.PackageInfo{Name: "example"}, TypeName: "User"},
		Fields:    []genmodel.FieldInfo{{Name: "PK"}},
	}

	if err := gen(data, tpl, dir); err == nil {
		t.Fatal("expected generation to fail for invalid Go output")
	}

	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	if string(contents) != "// existing\n" {
		t.Fatalf("expected format failure to leave existing file untouched, got %q", string(contents))
	}
}

func TestGenResultDoesNotWriteBrokenGoOnFormatError(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "userresult.result.tsq.go")
	if err := os.WriteFile(target, []byte("// existing result\n"), 0o644); err != nil {
		t.Fatalf("failed to seed Result generated file: %v", err)
	}

	tpl, err := template.New("broken").Parse("package {{.TypeInfo.Package.Name}}\nfunc {")
	if err != nil {
		t.Fatalf("failed to parse broken template: %v", err)
	}

	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true},
		TypeInfo:  genmodel.TypeInfo{Package: genmodel.PackageInfo{Name: "example"}, TypeName: "UserResult"},
		Fields:    []genmodel.FieldInfo{{Name: "PK"}},
	}

	if err := genResult(data, tpl, dir); err == nil {
		t.Fatal("expected Result generation to fail for invalid Go output")
	}

	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read Result generated file: %v", err)
	}

	if string(contents) != "// existing result\n" {
		t.Fatalf("expected Result format failure to leave existing file untouched, got %q", string(contents))
	}
}

func TestWriteGeneratedFileReplacesContentsAtomically(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user.tsq.go")
	if err := os.WriteFile(target, []byte("// Code generated by tsq-test. DO NOT EDIT.\nold"), 0o644); err != nil {
		t.Fatalf("failed to seed target file: %v", err)
	}

	if err := writeGeneratedFile(target, []byte("new")); err != nil {
		t.Fatalf("expected atomic write helper to succeed, got %v", err)
	}

	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read target file: %v", err)
	}

	if string(contents) != "new" {
		t.Fatalf("expected target file to be replaced, got %q", string(contents))
	}
}

func TestWriteGeneratedFileRejectsNonGeneratedFile(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user.tsq.go")
	if err := os.WriteFile(target, []byte("package example\n"), 0o644); err != nil {
		t.Fatalf("failed to seed target file: %v", err)
	}

	err := writeGeneratedFile(target, []byte("new"))
	if err == nil {
		t.Fatal("expected non-generated file overwrite to fail")
	}
}

func TestWriteGeneratedFilePreservesPermissions(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user.tsq.go")
	if err := os.WriteFile(target, []byte("// Code generated by tsq-test. DO NOT EDIT.\n"), 0o600); err != nil {
		t.Fatalf("failed to seed target file: %v", err)
	}

	if err := writeGeneratedFile(target, []byte("// Code generated by tsq-test. DO NOT EDIT.\npackage example\n")); err != nil {
		t.Fatalf("expected generated file rewrite to succeed, got %v", err)
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("failed to stat target file: %v", err)
	}

	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected permissions to be preserved, got %o", got)
	}
}

// gen and genResult are thin helpers used by tests to render a template for a
// single struct and write the output to dir.  They are test-only wrappers
// around renderGenerationModel and intentionally not part of the production
// code path.

func gen(data *genmodel.StructInfo, t *template.Template, dir string) error {
	return renderGenerationModel(io.Discard, generationModel{
		Data:       data,
		Template:   t,
		Filename:   filepath.Join(dir, generatedFilename(data)),
		ErrorLabel: "template rendering failed",
	})
}

func genResult(data *genmodel.StructInfo, t *template.Template, dir string) error {
	return renderGenerationModel(io.Discard, generationModel{
		Data:       data,
		Template:   t,
		Filename:   filepath.Join(dir, generatedFilename(data)),
		ErrorLabel: "Result template rendering failed",
	})
}

func TestStableVersion(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"v1.2.0-10-ga3683ff-dirty", "v1.2.0"},
		{"v1.2.0-dirty", "v1.2.0"},
		{"v1.2.0-10-ga3683ff", "v1.2.0"},
		{"v1.2.0", "v1.2.0"},
		{"v1.2.0-beta.1", "v1.2.0-beta.1"},
		{"dev", "dev"},
		{"unknown", "unknown"},
	}

	for _, tc := range cases {
		if got := stableVersion(tc.input); got != tc.want {
			t.Errorf("stableVersion(%q) = %q; want %q", tc.input, got, tc.want)
		}
	}
}

func chdirForGenTest(t *testing.T, dir string) {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	if err := os.Chdir(dir); err != nil {
		t.Fatalf("failed to chdir to temp module: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatalf("failed to restore working directory: %v", err)
		}
	})
}

func genTestModuleFile(t *testing.T) string {
	t.Helper()

	// Tests run in internal/cmd; the module root is two levels up.
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to get repo root for test module: %v", err)
	}

	return "module example.com/gentest\n\n" +
		"go 1.24.2\n\n" +
		"require github.com/tmoeish/tsq/v5 v5.0.0\n\n" +
		"replace github.com/tmoeish/tsq/v5 => " + root + "\n"
}

func tidyGenTestModule(t *testing.T) {
	t.Helper()

	cmd := exec.Command("go", "mod", "tidy")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go mod tidy failed: %v\n%s", err, string(output))
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
}

// TestGeneratedCodeWithDatabaseSQLFieldsCompiles builds generated code for fields
// typed from database/sql. The templates render those types through the tsqsql alias,
// and for months nothing imported it: the field type check passed, and every user with
// a sql.NullString column got generated code that did not compile.
func TestGeneratedCodeWithDatabaseSQLFieldsCompiles(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

import (
	"database/sql"
	"time"
)

//tsq:table name=users
//tsq:unique Email
//tsq:managed updated_at
type User struct {
	ID        int64                `+"`db:\"id\"`"+`
	Email     string               `+"`db:\"email,size:128\"`"+`
	Nickname  sql.NullString       `+"`db:\"nickname,size:64\"`"+`
	Bio       *string              `+"`db:\"bio,size:256\"`"+`
	SeenAt    sql.NullTime         `+"`db:\"seen_at\"`"+`
	Score     sql.Null[int64]      `+"`db:\"score\"`"+`
	UpdatedAt sql.Null[time.Time]  `+"`db:\"updated_at\"`"+`
}

//tsq:result
type UserNickname struct {
	UserID   int64           `+"`json:\"user_id\" tsq:\"User.ID\"`"+`
	Nickname sql.NullString  `+"`json:\"nickname\" tsq:\"User.Nickname\"`"+`
	Score    sql.Null[int64] `+"`json:\"score\" tsq:\"User.Score\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})

	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() error = %v", err)
	}

	// The model imports nothing from tsq, so only now does the module need it.
	tidyGenTestModule(t)

	output, err := exec.Command("go", "build", "./...").CombinedOutput()
	if err != nil {
		t.Fatalf("generated code does not compile: %v\n%s", err, output)
	}

	// Nullable fields become NullColumns of their value type, and a nullable
	// result field is mapped with MapIntoNull; time is imported for the value type
	// even though no field is a time.Time.
	table, _ := os.ReadFile("user.tsq.go")

	results, _ := filepath.Glob("*.result.tsq.go")
	if len(results) != 1 {
		t.Fatalf("result files = %v", results)
	}

	result, _ := os.ReadFile(results[0])

	for file, want := range map[string]string{
		"table nickname": "Nickname: tsq.NewNullColumn[string](t, \"nickname\"",
		"table bio":      "Bio:      tsq.NewNullColumn[string](t, \"bio\"",
		"table seen_at":  "tsq.NewNullColumn[tsqtime.Time](t, \"seen_at\"",
		"table id":       "tsq.NewColumn(t, \"id\"",
		"table generic":  "Score:     tsq.NewNullColumn[int64](t, \"score\", \"Score\", func(r *User) *tsqsql.Null[int64]",
		"table time":     "tsq.NewNullColumn[tsqtime.Time](t, \"updated_at\", \"UpdatedAt\", func(r *User) *tsqsql.Null[tsqtime.Time]",
		"table field":    "Nickname tsq.NullColumn[User, string]",
		"result":         "tsq.MapIntoNull(TableUser.Nickname",
	} {
		source := table
		if file == "result" {
			source = result
		}

		// gofmt aligns fields by the longest name, so compare with spaces collapsed.
		if !strings.Contains(strings.Join(strings.Fields(string(source)), " "), strings.Join(strings.Fields(want), " ")) {
			t.Errorf("%s: generated code lacks %q", file, want)
		}
	}
}

// TestGenCmdRejectsIdentifiersADialectWouldTruncate catches a derived index name
// longer than PostgreSQL's 63 characters at generation time, where the fix is one
// name= on the directive, instead of when a runtime starts.
func TestGenCmdRejectsIdentifiersADialectWouldTruncate(t *testing.T) {
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

//tsq:table name=subscription_renewal_attempts
//tsq:index CustomerAccountID,BillingPeriodStart
type Attempt struct {
	ID                 int64 `+"`db:\"id\"`"+`
	CustomerAccountID  int64 `+"`db:\"customer_account_id\"`"+`
	BillingPeriodStart int64 `+"`db:\"billing_period_start\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{"."})

	err := GenCmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "name it explicitly: //tsq:index CustomerAccountID,BillingPeriodStart name=") {
		t.Fatalf("GenCmd.Execute() error = %v, want the long derived index name reported with its fix", err)
	}

	// Naming the index is the fix.
	writeTestFile(t, filepath.Join(dir, "model.go"), `package gentest

//tsq:table name=subscription_renewal_attempts
//tsq:index CustomerAccountID,BillingPeriodStart name=idx_renewal_customer_period
type Attempt struct {
	ID                 int64 `+"`db:\"id\"`"+`
	CustomerAccountID  int64 `+"`db:\"customer_account_id\"`"+`
	BillingPeriodStart int64 `+"`db:\"billing_period_start\"`"+`
}
`)

	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("GenCmd.Execute() with an explicit index name error = %v", err)
	}
}

// TestGenCmdMigrationDropsIndexesAndFlagsManualChanges covers the migration
// outputs no other test reached: dropping an index that left the struct, SQLite's
// table rebuild, and the "manual change required" comment for a primary key
// that stops being auto-increment, which no dialect can ALTER safely.
func TestGenCmdMigrationDropsIndexesAndFlagsManualChanges(t *testing.T) {
	t.Cleanup(func() { GenCmd.SetArgs(nil) })

	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))
	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users
//tsq:index Name
type User struct {
	ID   int64  `+"`db:\"id\"`"+`
	Name string `+"`db:\"name,size:64\"`"+`
}
`)
	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	run := func(stage string) {
		t.Helper()
		GenCmd.SetOut(new(bytes.Buffer))
		GenCmd.SetErr(new(bytes.Buffer))
		GenCmd.SetArgs([]string{"."})

		if err := GenCmd.Execute(); err != nil {
			t.Fatalf("%s GenCmd.Execute() error = %v", stage, err)
		}
	}

	run("initial")

	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users
type User struct {
	ID   int64  `+"`db:\"id\"`"+`
	Name string `+"`db:\"name,size:128\"`"+`
}
`)
	run("second")

	for file, wants := range map[string][]string{
		"postgres.sql": {`DROP INDEX "idx_users_name";`, `ALTER TABLE "users" ALTER COLUMN "name" TYPE VARCHAR(128);`},
		"mysql.sql":    {"DROP INDEX `idx_users_name` ON `users`;", "MODIFY COLUMN `name`"},
		// SQLite does not enforce a VARCHAR size, so it drops the index and does not
		// rebuild the table (which would lose its triggers) for nothing.
		"sqlite.sql": {`DROP INDEX "idx_users_name";`, "SQLite does not enforce; nothing to run"},
	} {
		content, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}

		for _, want := range wants {
			if !strings.Contains(string(content), want) {
				t.Errorf("%s lacks %q:\n%s", file, want, content)
			}
		}
	}
	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users pk=ID assigned
type User struct {
	ID   int64  `+"`db:\"id\"`"+`
	Name string `+"`db:\"name,size:128\"`"+`
}
`)
	run("third")

	for _, file := range []string{"postgres.sql", "mysql.sql"} {
		content, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}

		if want := "-- users: manual change required for primary key column id"; !strings.Contains(string(content), want) {
			t.Errorf("%s lacks %q:\n%s", file, want, content)
		}
	}
}

// genModule writes a module of files (relative path to content), runs tsq gen on
// its root package and returns the error.
func genModule(t *testing.T, files map[string]string, args ...string) error {
	t.Helper()
	t.Cleanup(func() {
		dryRunFlag = false
		checkFlag = false
		v = false
		GenCmd.SetArgs(nil)
	})

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "go.mod"), genTestModuleFile(t))

	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}

		writeTestFile(t, path, content)
	}

	chdirForGenTest(t, dir)
	tidyGenTestModule(t)

	return runGen(t, args...)
}

// runGen runs tsq gen on the working directory's package.
func runGen(t *testing.T, args ...string) error {
	t.Helper()

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs(append([]string{"."}, args...))

	return GenCmd.Execute()
}

// shapeModule holds a field of every shape the generator once got wrong. Each
// shape is named by what used to fail.
var shapeModule = map[string]string{
	"ext/ext.go": `package ext

// Money is a type from another package, projected into a result.
type Money int64
`,
	"x1/pkg/pkg.go": "package pkg\n\ntype V string\n",
	"x2/pkg/pkg.go": "package pkg\n\ntype V string\n",
	"model.go": `package gentest

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"example.com/gentest/ext"
	p1 "example.com/gentest/x1/pkg"
	p2 "example.com/gentest/x2/pkg"
)

// Legacy and Stream are not TSQ's: a map field, and an embedded interface.
type Legacy struct {
	Meta map[string]string ` + "`db:\"meta\"`" + `
}

type Stream struct {
	io.Reader
}

//tsq:table name=device_bindings
//tsq:unique Name
type DeviceBinding struct {
	ID   int64    ` + "`db:\"id\"`" + `
	Name string   ` + "`db:\"name,size:64\"`" + `
	Hash [32]byte ` + "`db:\"hash,type:BINARY(32)\"`" + `
	Tags *string  ` + "`db:\"tags,size:32,default:'a, b'\"`" + `
	Slug string   ` + "`db:\"slug,generated\"`" + `
}

// Box is a local generic codec type, instantiated with a type from another package.
type Box[T any] struct{ V T }

func (b Box[T]) Value() (driver.Value, error) { return nil, nil }
func (b *Box[T]) Scan(any) error             { return errors.New("unused") }

// NullMoney is a nullable codec type: a NullColumn on the Go side, so NULL in DDL.
type NullMoney struct {
	Money int64
	Valid bool
}

func (n NullMoney) Value() (driver.Value, error) { return nil, nil }
func (n *NullMoney) Scan(any) error             { return errors.New("unused") }

//tsq:table name=wallets
//tsq:unique Ctx
//tsq:unique Db
//tsq:unique T
//tsq:unique Tsq
//tsq:unique Digest
//tsq:fulltext Note
//tsq:managed deleted_at
type Wallet struct {
	ID        int64          ` + "`db:\"id\"`" + `
	Ctx       string         ` + "`db:\"ctx,size:32\"`" + `
	Db        string         ` + "`db:\"db_name,size:32\"`" + `
	T         string         ` + "`db:\"t,size:32\"`" + `
	Tsq       string         ` + "`db:\"tsq,size:32\"`" + `
	Note      string         ` + "`db:\"note,size:200\"`" + `
	Balance   ext.Money      ` + "`db:\"balance\"`" + `
	A         p1.V           ` + "`db:\"a,size:16\"`" + `
	B         p2.V           ` + "`db:\"b,size:16\"`" + `
	NA        sql.Null[p1.V] ` + "`db:\"na,size:16\"`" + `
	NB        sql.Null[p2.V] ` + "`db:\"nb,size:16\"`" + `
	Data      Box[ext.Money] ` + "`db:\"data,type:TEXT\"`" + `
	Amount    NullMoney      ` + "`db:\"amount,type:BIGINT\"`" + `
	DeletedAt int64          ` + "`db:\"deleted_at\"`" + `
	Quoted    string         ` + "`db:\"quoted,size:8\" json:\"say \\\"hi\\\"\"`" + `
	Digest    Hash           ` + "`db:\"digest,type:VARCHAR(64)\"`" + `
}

// Hash is a named byte slice: not comparable, so a lookup by it cannot go through
// the generic GetBy.
type Hash []byte

//tsq:result
type WalletBrief struct {
	ID      int64     ` + "`json:\"id\" tsq:\"Wallet.ID\"`" + `
	Balance ext.Money ` + "`json:\"balance\" tsq:\"Wallet.Balance\"`" + `
}
`,
}

// TestGeneratedCodeCompilesForEveryFieldShape generates and builds the shape
// module. Each shape used to produce code or DDL that failed:
//   - a result field from another package: the result template wrote no imports
//   - two packages named pkg: types were spelled by package name, not alias
//   - Box[ext.Money]: a local generic type dropped its type argument's import
//   - fields named Ctx, Db, T and Tsq: GetByX parameters collided with ctx, db,
//     the receiver and the tsq package
//   - NullMoney with type:: a NullColumn in Go but NOT NULL in DDL
//   - a full-text index on a soft-delete table: deleted_at was put into it
//   - a JSON tag holding a quote: tag values were pasted into string literals
//   - DeviceBinding: its receiver was db, the parameter of every row method
//   - [32]byte: the column accessor returned *[]byte
//   - Legacy and Stream: structs no directive names stopped gen with an error
//     that named neither the struct nor the field
//   - default:'a, b': the tag was cut at the comma inside the literal
//   - generated with no expression: written as a NOT NULL column no INSERT fills
func TestGeneratedCodeCompilesForEveryFieldShape(t *testing.T) {
	if err := genModule(t, shapeModule); err != nil {
		t.Fatalf("tsq gen: %v", err)
	}

	tidyGenTestModule(t)

	if output, err := exec.Command("go", "build", "./...").CombinedOutput(); err != nil {
		t.Fatalf("generated code does not compile: %v\n%s", err, output)
	}

	// The table without its live-row filter keeps the columns, alias and full-text
	// index, and has no lookup by unique index: those values repeat among deleted
	// rows, so the lookup could only fail when it runs.
	writeTestFile(t, "probe.go", "package gentest\n\nvar _ = TableWallet.WithDeleted().As(\"w\").FullTextNote()\nvar _ = TableWallet.WithDeleted().Ctx\n")

	if output, err := exec.Command("go", "build", "./...").CombinedOutput(); err != nil {
		t.Fatalf("the WithDeleted table lost a method: %v\n%s", err, output)
	}

	writeTestFile(t, "probe.go", "package gentest\n\nvar _ = TableWallet.WithDeleted().GetByCtx\n")

	if output, err := exec.Command("go", "build", "./...").CombinedOutput(); err == nil || !strings.Contains(string(output), "GetByCtx") {
		t.Fatalf("GetByCtx on the WithDeleted table = %v\n%s; want it undefined", err, output)
	}

	if err := os.Remove("probe.go"); err != nil {
		t.Fatal(err)
	}

	table, err := os.ReadFile("wallet.tsq.go")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"tsq.NullColumn[Wallet, pkg.V]", "tsq.NullColumn[Wallet, pkg1.V]", "tsq.NullColumn[Wallet, int64]", "ctx_ string", "db_ string", "t_ string", "tsq_ string"} {
		if !strings.Contains(string(table), want) {
			t.Errorf("wallet.tsq.go lacks %q", want)
		}
	}

	mysql, err := os.ReadFile("mysql.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"`amount` BIGINT,", "FULLTEXT INDEX `ft_wallets_note`(`note`)"} {
		if !strings.Contains(string(mysql), want) {
			t.Errorf("mysql.sql lacks %q:\n%s", want, mysql)
		}
	}

	sqlite, err := os.ReadFile("sqlite.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{`"tags" VARCHAR(32) DEFAULT 'a, b'`, "slug is computed by the database"} {
		if !strings.Contains(string(sqlite), want) {
			t.Errorf("sqlite.sql lacks %q:\n%s", want, sqlite)
		}
	}

	if strings.Contains(string(sqlite), `"slug"`) {
		t.Errorf("sqlite.sql creates the column a migration owns:\n%s", sqlite)
	}

	postgres, err := os.ReadFile("postgres.sql")
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(postgres), `coalesce("deleted_at"`) {
		t.Errorf("postgres.sql puts deleted_at into the full-text index:\n%s", postgres)
	}
}

// TestGenRefusesToGuessACodecColumnType covers a type that implements
// driver.Valuer: what it stores is up to Value, which the underlying string does
// not tell, so the column type must be declared. It used to be guessed from the
// underlying type, a VARCHAR here, and the first write of an integer failed.
func TestGenRefusesToGuessACodecColumnType(t *testing.T) {
	err := genModule(t, map[string]string{"model.go": `package gentest

import "database/sql/driver"

type Status string

func (s Status) Value() (driver.Value, error) { return int64(len(s)), nil }

//tsq:table
type Row struct {
	ID    int64  ` + "`db:\"id\"`" + `
	State Status ` + "`db:\"state\"`" + `
}
`})
	if err == nil || !strings.Contains(err.Error(), "implements driver.Valuer") || !strings.Contains(err.Error(), "type:") {
		t.Fatalf("tsq gen = %v; want the codec type refused with the type: fix", err)
	}
}

// TestGenRefusesTwoFieldsWithOneColumn covers a repeated db tag, and one tag on a
// field list, which both produced a CREATE TABLE naming the column twice.
func TestGenRefusesTwoFieldsWithOneColumn(t *testing.T) {
	for name, fields := range map[string]string{
		"repeated tag": "Name string `db:\"label\"`\n\tTitle string `db:\"LABEL\"`",
		"field list":   "Name, Title string `db:\"label\"`",
	} {
		t.Run(name, func(t *testing.T) {
			err := genModule(t, map[string]string{"model.go": "package gentest\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n\t" + fields + "\n}\n"})
			if err == nil || !strings.Contains(err.Error(), "both map to column") {
				t.Fatalf("tsq gen = %v; want the duplicate column refused", err)
			}
		})
	}
}

// TestGenRemovesTheGoFilesItNoLongerGenerates covers a struct deleted from the
// source: its generated file used to stay behind, naming a type that no longer
// exists, so the package did not compile and gen --check failed after every gen.
func TestGenRemovesTheGoFilesItNoLongerGenerates(t *testing.T) {
	if err := genModule(t, shapeModule); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat("walletbrief.result.tsq.go"); err != nil {
		t.Fatalf("result file after the first gen: %v", err)
	}

	source, err := os.ReadFile("model.go")
	if err != nil {
		t.Fatal(err)
	}

	cut := strings.Index(string(source), "//tsq:result")
	writeTestFile(t, "model.go", string(source[:cut]))

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat("walletbrief.result.tsq.go"); !os.IsNotExist(err) {
		t.Fatalf("stale result file: %v; want it removed", err)
	}

	if err := runGen(t, "--check"); err != nil {
		t.Fatalf("gen --check after gen = %v", err)
	}
}

// TestMigrationAddsANotNullColumnToATableWithRows covers the ADD COLUMN a
// migration record holds for a new field. A NOT NULL column with no default, and
// on SQLite one whose default is not a constant (CURRENT_TIMESTAMP, which adding
// created_at has), fail on a table with rows; the statement was written anyway.
// The rows now get the type's zero value on every dialect, SQLite by a rebuild.
func TestMigrationAddsANotNullColumnToATableWithRows(t *testing.T) {
	model := func(fields string) string {
		return "package gentest\n\nimport \"time\"\n\nvar _ time.Time\n\n//tsq:table\n//tsq:managed created_at\ntype Row struct {\n\tID int64 `db:\"id\"`\n\tCreatedAt time.Time `db:\"created_at\"`\n" + fields + "}\n"
	}

	if err := genModule(t, map[string]string{"model.go": strings.Replace(strings.Replace(model(""), "//tsq:managed created_at\n", "", 1), "\tCreatedAt time.Time `db:\"created_at\"`\n", "", 1)}); err != nil {
		t.Fatal(err)
	}

	writeTestFile(t, "model.go", model("\tAge int64 `db:\"age\"`\n\tNote *string `db:\"note,size:20\"`\n"))

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	for file, want := range map[string][]string{
		"mysql.sql":    {"ADD COLUMN `age` BIGINT NOT NULL DEFAULT 0;", "ALTER COLUMN `age` DROP DEFAULT;"},
		"postgres.sql": {`ADD COLUMN "age" BIGINT NOT NULL DEFAULT 0;`, `ALTER COLUMN "age" DROP DEFAULT;`},
		"sqlite.sql":   {`rebuilt by copying its rows`, `"created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`},
	} {
		ddl, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}

		want = append(want, "-- row: age is NOT NULL without a default; existing rows get 0")
		for _, w := range want {
			if !strings.Contains(string(ddl), w) {
				t.Errorf("%s lacks %q:\n%s", file, w, ddl)
			}
		}

		if strings.Contains(string(ddl), "note is NOT NULL") {
			t.Errorf("%s warns about a nullable column:\n%s", file, ddl)
		}

		if file == "sqlite.sql" && strings.Contains(string(ddl), "ADD COLUMN") {
			t.Errorf("sqlite.sql adds a column SQLite refuses on a table with rows:\n%s", ddl)
		}
	}
}

// TestGenRefusesWhatItCannotGenerate covers declarations tsq gen used to turn
// into code that panicked, did not compile or broke the DDL, and now refuses
// with the reason and the way out.
func TestGenRefusesWhatItCannotGenerate(t *testing.T) {
	table := func(directives, fields string) string {
		return "package gentest\n\n" + directives + "\ntype Row struct {\n\tID int64 `db:\"id\"`\n\t" + fields + "\n}\n"
	}

	for name, tt := range map[string]struct {
		source string
		want   string
	}{
		// Every generated accessor of a promoted field dereferenced the pointer.
		"embedded pointer": {"package gentest\n\ntype Base struct {\n\tID int64 `db:\"id\"`\n}\n\n//tsq:table\ntype Row struct {\n\t*Base\n\tName string `db:\"name\"`\n}\n", "embed the struct by value"},
		// A zero value read as unset: false and 0 could never be written.
		"default on a field that cannot hold NULL": {table("//tsq:table", "Active bool `db:\"active,default:true\"`"), "has default: but cannot hold NULL"},
		"default and generated":                    {table("//tsq:table", "Up *string `db:\"up,default:'x',generated:UPPER(up)\"`"), "both default: and generated:"},
		// The DDL renderer panicked.
		"generated string key": {"package gentest\n\n//tsq:table pk=Code\ntype Row struct {\n\tCode string `db:\"code\"`\n}\n", "pk=Code assigned"},
		// runtime.tsq.go overwrote the table's file.
		"table named Runtime": {"package gentest\n\n//tsq:table\ntype Runtime struct {\n\tID int64 `db:\"id\"`\n}\n", "runtime.tsq.go collides"},
		// A field beside the generated method did not compile.
		"field named like a row method": {table("//tsq:table\n//tsq:managed deleted_at", "IsDeleted bool `db:\"is_deleted\"`\n\tDeletedAt int64 `db:\"deleted_at\"`"), "generated row method IsDeleted"},
		"directive on a non-struct":     {"package gentest\n\n//tsq:table\ntype Status string\n", "is not a struct"},
		"generic table":                 {"package gentest\n\n//tsq:table\ntype Row[T any] struct {\n\tID int64 `db:\"id\"`\n}\n", "is generic"},
		// It was accepted and dropped.
		"search on a result": {table("//tsq:table", "") + "\n//tsq:result\n//tsq:search Name\ntype View struct {\n\tName int64 `tsq:\"Row.ID\"`\n}\n", "search belongs to a table"},
		// The generated result referenced a TableView that does not exist.
		"result of a result": {table("//tsq:table", "") + "\n//tsq:result\ntype A struct {\n\tID int64 `tsq:\"Row.ID\"`\n}\n\n//tsq:result\ntype B struct {\n\tID int64 `tsq:\"A.ID\"`\n}\n", "which is a result"},
	} {
		t.Run(name, func(t *testing.T) {
			err := genModule(t, map[string]string{"model.go": tt.source})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("tsq gen = %v; want an error containing %q", err, tt.want)
			}
		})
	}
}

// TestGenReadsAPackageThatUsesCgo covers import "C", which has no package to
// load: resolving it stopped gen for every package that used cgo.
func TestGenReadsAPackageThatUsesCgo(t *testing.T) {
	err := genModule(t, map[string]string{
		"cgo.go":   "package gentest\n\n// #include <stdlib.h>\nimport \"C\"\n\nfunc free() { C.free(nil) }\n",
		"model.go": "package gentest\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n}\n",
	})
	if err != nil {
		t.Fatalf("tsq gen: %v", err)
	}
}

// TestGenTakesAnAbsoluteDirectoryFromOutsideTheModule runs tsq gen on a module
// by its absolute path from a directory outside it. Packages were reloaded by
// import path, resolved from the working directory, and not found.
func TestGenTakesAnAbsoluteDirectoryFromOutsideTheModule(t *testing.T) {
	if err := genModule(t, map[string]string{"model.go": "package gentest\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n}\n"}); err != nil {
		t.Fatal(err)
	}

	module, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	chdirForGenTest(t, t.TempDir())

	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(new(bytes.Buffer))
	GenCmd.SetArgs([]string{module})

	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("tsq gen %s: %v", module, err)
	}
}

// TestGenWarnsAboutIndexesMySQLRejects covers indexes MySQL refuses when the DDL
// runs (errors 1071 and 1170) and the other dialects accept. They were refused by
// tsq gen, which blocked a schema that never runs on MySQL; now mysql.sql says why
// the statement will fail there, and the other files are as declared.
func TestGenWarnsAboutIndexesMySQLRejects(t *testing.T) {
	err := genModule(t, map[string]string{"model.go": "package gentest\n\n//tsq:table\n//tsq:unique Body\n//tsq:index Notes\ntype Row struct {\n\tID    int64  `db:\"id\"`\n\tBody  string `db:\"body,size:2000\"`\n\tNotes string `db:\"notes,size:20000\"`\n}\n"})
	if err != nil {
		t.Fatalf("tsq gen: %v", err)
	}

	mysql, err := os.ReadFile("mysql.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"-- row: index ux_row_body can take 8000 bytes on MySQL", "-- row: index idx_row_notes covers notes, a MEDIUMTEXT on MySQL"} {
		if !strings.Contains(string(mysql), want) {
			t.Errorf("mysql.sql lacks %q:\n%s", want, mysql)
		}
	}

	if postgres, err := os.ReadFile("postgres.sql"); err != nil || strings.Contains(string(postgres), "on MySQL") {
		t.Errorf("postgres.sql carries the MySQL note: %v\n%s", err, postgres)
	}
}

// runSQLiteShell runs script with the sqlite3 shell on db, as a user runs a
// migration section: from stdin, without stopping at an error.
func runSQLiteShell(shell, db, script string) (string, error) {
	cmd := exec.Command(shell, db)
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.CombinedOutput()

	return string(out), err
}

// TestGenMigrationsNeverDropDataUnasked covers three migrations the generator got
// wrong. Removing an indexed field dropped the column before its index, which
// fails on every dialect, and wrote the DROP COLUMN as a plain statement, although
// a renamed db tag or a mistyped directive looks the same to the generator.
// Adding a generated column wrote ADD COLUMN ... STORED, which SQLite refuses.
func TestGenMigrationsNeverDropDataUnasked(t *testing.T) {
	model := func(fields string) string {
		return "package gentest\n\n//tsq:table name=scores\ntype Score struct {\n\tID int64 `db:\"id\"`\n\tName string `db:\"name,size:32\"`\n" + fields + "}\n"
	}

	if err := genModule(t, map[string]string{"model.go": strings.Replace(model("\tPoints int64 `db:\"points\"`\n"), "//tsq:table name=scores", "//tsq:table name=scores\n//tsq:index Points", 1)}); err != nil {
		t.Fatal(err)
	}

	writeTestFile(t, "model.go", model(""))

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	postgres, err := os.ReadFile("postgres.sql")
	if err != nil {
		t.Fatal(err)
	}

	migration := string(postgres[strings.Index(string(postgres), "-- Migration: "):])
	dropIndex := strings.Index(migration, `DROP INDEX "idx_scores_points";`)
	dropColumn := strings.Index(migration, `-- ALTER TABLE "scores" DROP COLUMN "points";`)

	if dropIndex < 0 || dropColumn < 0 || dropIndex > dropColumn || !strings.Contains(migration, "-- DESTRUCTIVE (scores drops column points") {
		t.Fatalf("postgres migration drops the index after the column, or runs the drop:\n%s", migration)
	}

	writeTestFile(t, "model.go", model("\tSlug string `db:\"slug,size:32,generated:lower(name)\"`\n"))

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	sqlite, err := os.ReadFile("sqlite.sql")
	if err != nil {
		t.Fatal(err)
	}

	last := string(sqlite[strings.LastIndex(string(sqlite), "-- Migration: "):])
	if strings.Contains(last, "ADD COLUMN") || !strings.Contains(last, `ALTER TABLE "__tsq_new_scores" RENAME TO "scores";`) {
		t.Fatalf("sqlite adds a generated column in place, which it refuses:\n%s", last)
	}
}

// TestGenRefusesNamesThePackageAlreadyUses covers generated names that clash with
// the package: a declaration of TableRow in a file of the package, and a second
// table named after the first one's generated type. Both used to leave a package
// that no longer compiled.
func TestGenRefusesNamesThePackageAlreadyUses(t *testing.T) {
	row := "//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n}\n"

	for name, tc := range map[string]struct{ source, want string }{
		"declared":            {"package gentest\n\nvar TableRow = 1\n\n" + row, "TableRow is declared here"},
		"table named so":      {"package gentest\n\n" + row + "\n//tsq:table\ntype RowTable struct {\n\tID int64 `db:\"id\"`\n}\n", "generated symbol RowTable collides"},
		"runtime symbol":      {"package gentest\n\nfunc TSQTables() {}\n\n" + row, "TSQTables is declared here"},
		"a method of the row": {"package gentest\n\n" + row + "\nfunc (r *Row) Update(name string) {}\n", "Row.Update is declared here"},
		"a table named Table": {"package gentest\n\n//tsq:table name=things\ntype Table struct {\n\tID int64 `db:\"id\"`\n}\n", "would generate TableTable twice"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := genModule(t, map[string]string{"model.go": tc.source}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("tsq gen = %v; want %q", err, tc.want)
			}
		})
	}
}

// TestGenRefusesAFieldWithTwoRoles covers a primary key TSQ also writes (version
// or a timestamp), two roles on one field, and two structs on one table: each
// generated code or DDL that failed only once it ran.
func TestGenRefusesAFieldWithTwoRoles(t *testing.T) {
	for name, tc := range map[string]struct{ source, want string }{
		"key is the version":     {"//tsq:table pk=Version\n//tsq:managed version\ntype Row struct {\n\tVersion int64 `db:\"version\"`\n}\n", "both the id and the version field"},
		"one time, two roles":    {"//tsq:table\n//tsq:managed created_at=At updated_at=At\ntype Row struct {\n\tID int64 `db:\"id\"`\n\tAt time.Time `db:\"at\"`\n}\n", "both the created_at and the updated_at field"},
		"one table, two structs": {"//tsq:table name=rows\ntype Row struct {\n\tID int64 `db:\"id\"`\n}\n\n//tsq:table name=ROWS\ntype Other struct {\n\tID int64 `db:\"id\"`\n}\n", "Other and Row both map to table"},
	} {
		t.Run(name, func(t *testing.T) {
			source := "package gentest\n\nimport \"time\"\n\nvar _ time.Time\n\n" + tc.source

			err := genModule(t, map[string]string{"model.go": source})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("tsq gen = %v; want %q", err, tc.want)
			}

			// An error about one struct points at its declaration.
			if !strings.Contains(tc.want, "both map to table") && !strings.Contains(err.Error(), "model.go:9:6: Row: ") {
				t.Errorf("tsq gen = %v; want the struct's position", err)
			}
		})
	}
}

// TestGeneratedTablesFollowTheStruct covers the shape of a generated table: its
// columns in the order the struct declares them (an embedded struct's in place),
// FindByX beside GetByX for a unique index, a FullTextX method for each full-text
// index, and IsDeleted on a soft-delete row.
func TestGeneratedTablesFollowTheStruct(t *testing.T) {
	err := genModule(t, map[string]string{"model.go": `package gentest

type Base struct {
	ID        int64 ` + "`db:\"id\"`" + `
	DeletedAt int64 ` + "`db:\"deleted_at\"`" + `
}

//tsq:table
//tsq:managed deleted_at
//tsq:unique Slug
//tsq:fulltext Title,Body
type Post struct {
	Title string ` + "`db:\"title\"`" + `
	Base
	Slug string ` + "`db:\"slug\"`" + `
	Body string ` + "`db:\"body\"`" + `
}
`})
	if err != nil {
		t.Fatalf("tsq gen = %v", err)
	}

	tidyGenTestModule(t)

	if output, err := exec.Command("go", "build", "./...").CombinedOutput(); err != nil {
		t.Fatalf("generated code does not compile: %v\n%s", err, output)
	}

	source, err := os.ReadFile("post.tsq.go")
	if err != nil {
		t.Fatal(err)
	}

	generated := string(source)

	order := regexp.MustCompile(`(?m)^\t(\w+) +tsq\.Column\[Post`).FindAllStringSubmatch(generated, -1)
	var names []string
	for _, m := range order {
		names = append(names, m[1])
	}

	if got := strings.Join(names, ","); got != "Title,ID,DeletedAt,Slug,Body" {
		t.Errorf("column order = %s; want the declaration order", got)
	}

	for _, want := range []string{"func (t PostTable) FindBySlug(", "func (t PostTable) FullTextTitleAndBody() tsq.FullTextIndex", "func (p *Post) IsDeleted() bool"} {
		if !strings.Contains(generated, want) {
			t.Errorf("post.tsq.go lacks %q", want)
		}
	}
}

// TestGenMigrationFillsAColumnThatBecomesNotNull covers a nullable field made NOT
// NULL: the SQLite rebuild copied the column as it was, the copy failed on the
// rows holding NULL, and the sqlite3 shell went on to drop the table with its rows.
func TestGenMigrationFillsAColumnThatBecomesNotNull(t *testing.T) {
	shell, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Skip("sqlite3 shell not installed")
	}

	model := func(nick string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\n//tsq:table name=people\ntype Person struct {\n\tID int64 `db:\"id\"`\n\t" + nick + "\n}\n"}
	}

	if err := genModule(t, model("Nick *string `db:\"nick\"`")); err != nil {
		t.Fatal(err)
	}

	initial, err := os.ReadFile("sqlite.sql")
	if err != nil {
		t.Fatal(err)
	}

	for field, want := range map[string]string{
		"Nick string `db:\"nick\"`": "2|",
	} {
		writeTestFile(t, "model.go", model(field)["model.go"])

		if err := runGen(t); err != nil {
			t.Fatal(err)
		}

		sqlite, err := os.ReadFile("sqlite.sql")
		if err != nil {
			t.Fatal(err)
		}

		db := filepath.Join(t.TempDir(), "t.db")
		migration := string(sqlite[strings.LastIndex(string(sqlite), "-- Migration: "):])

		for _, script := range []string{string(initial), `INSERT INTO people (nick) VALUES ('kept'), (NULL);`, migration} {
			if out, err := runSQLiteShell(shell, db, script); err != nil || strings.Contains(out, "Error") {
				t.Fatalf("%s: sqlite3: %v\n%s\n%s", field, err, out, migration)
			}
		}

		if out, err := runSQLiteShell(shell, db, `SELECT count(*), max(CASE WHEN nick <> 'kept' THEN nick END) FROM people;`); err != nil || strings.TrimSpace(out) != want {
			t.Fatalf("%s: after the rebuild %q, %v; want %q", field, out, err, want)
		}

		// Start the next case from the nullable column again.
		writeTestFile(t, "model.go", model("Nick *string `db:\"nick\"`")["model.go"])

		if err := runGen(t); err != nil {
			t.Fatal(err)
		}

		if initial, err = os.ReadFile("sqlite.sql"); err != nil {
			t.Fatal(err)
		}
	}
}

// TestGenRefusesToStartHistoryOverGeneratedSQL covers a lost tsq.json, deleted to
// settle a merge conflict: the generated .sql was adopted as the start of history,
// and a field added since reached no migration at all.
func TestGenRefusesToStartHistoryOverGeneratedSQL(t *testing.T) {
	model := func(fields string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n" + fields + "}\n"}
	}

	if err := genModule(t, model("")); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove("tsq.json"); err != nil {
		t.Fatal(err)
	}

	writeTestFile(t, "model.go", model("\tExtra string `db:\"extra\"`\n")["model.go"])

	if err := runGen(t); err == nil || !strings.Contains(err.Error(), "tsq.json does not") {
		t.Fatalf("tsq gen without tsq.json = %v; want it refused", err)
	}
}

// TestGenMigrationFreesIndexNamesFirst covers index names, which PostgreSQL and
// SQLite keep per schema: renaming a table, whose DROP stays commented, created
// its unique index under the new table while the old one still held the name,
// and an index moved from table b to table a was created before b dropped it.
func TestGenMigrationFreesIndexNamesFirst(t *testing.T) {
	shell, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Skip("sqlite3 shell not installed")
	}

	model := func(name, index string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\n//tsq:table name=" + name + "\n//tsq:unique Email name=ux_email\ntype Person struct {\n\tID int64 `db:\"id\"`\n\tEmail string `db:\"email\"`\n}\n\n//tsq:table name=b\n" + index + "type B struct {\n\tID int64 `db:\"id\"`\n\tCode string `db:\"code\"`\n}\n\n//tsq:table name=a\n" + strings.ReplaceAll(index, "//tsq:index Code name=idx_code\n", "") + "type A struct {\n\tID int64 `db:\"id\"`\n\tCode string `db:\"code\"`\n}\n"}
	}

	if err := genModule(t, model("people", "//tsq:index Code name=idx_code\n")); err != nil {
		t.Fatal(err)
	}

	initial, err := os.ReadFile("sqlite.sql")
	if err != nil {
		t.Fatal(err)
	}

	// The table is renamed, and idx_code moves from b to a.
	next := model("persons", "")["model.go"]
	next = strings.Replace(next, "//tsq:table name=a\n", "//tsq:table name=a\n//tsq:index Code name=idx_code\n", 1)
	writeTestFile(t, "model.go", next)

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	sqlite, err := os.ReadFile("sqlite.sql")
	if err != nil {
		t.Fatal(err)
	}

	migration := string(sqlite[strings.LastIndex(string(sqlite), "-- Migration: "):])
	db := filepath.Join(t.TempDir(), "t.db")

	for _, script := range []string{string(initial), migration} {
		if out, err := runSQLiteShell(shell, db, script); err != nil || strings.Contains(out, "Error") {
			t.Fatalf("sqlite3: %v\n%s\n%s", err, out, migration)
		}
	}

	out, err := runSQLiteShell(shell, db, `SELECT tbl_name FROM sqlite_master WHERE name IN ('ux_email', 'idx_code') ORDER BY name;`)
	if err != nil || strings.Fields(out)[0] != "a" || strings.Fields(out)[1] != "persons" {
		t.Fatalf("indexes after the migration: %q, %v; want idx_code on a and ux_email on persons", out, err)
	}
}

// TestGenMigrationLeavesGeneratedColumnsToAMigration covers a changed generated
// expression: MySQL's MODIFY COLUMN left out GENERATED ALWAYS AS, which turned the
// column into a plain NOT NULL column TSQ never writes.
func TestGenMigrationLeavesGeneratedColumnsToAMigration(t *testing.T) {
	model := func(expr string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n\tName string `db:\"name,size:20\"`\n\tUp string `db:\"up,size:20,generated:" + expr + "\"`\n}\n"}
	}

	if err := genModule(t, model("UPPER(name)")); err != nil {
		t.Fatal(err)
	}

	writeTestFile(t, "model.go", model("LOWER(name)")["model.go"])

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	for _, file := range []string{"mysql.sql", "postgres.sql"} {
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}

		migration := string(content[strings.LastIndex(string(content), "-- Migration: "):])
		if strings.Contains(migration, "MODIFY COLUMN") || strings.Contains(migration, "ALTER COLUMN") || !strings.Contains(migration, "up changes how the database computes it") {
			t.Fatalf("%s alters a generated column in place:\n%s", file, migration)
		}
	}
}

// printSummaryAndWarnings is what gen -v prints about the schema: the summary,
// then the destructive warnings every run prints.
func printSummaryAndWarnings(w io.Writer, artifacts ddlArtifacts) {
	printDDLChangeSummary(w, artifacts)
	printDestructiveWarnings(w, artifacts)
}

// TestGenWarnsWithoutVerboseAndKeepsUnchangedFiles covers two things a plain gen
// did: the warning about a drop written commented out printed only with -v, so a
// renamed table went unnoticed, and every generated file was written again on a
// run that changed nothing, which woke every build cache and file watcher.
func TestGenWarnsWithoutVerboseAndKeepsUnchangedFiles(t *testing.T) {
	model := func(name string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\n//tsq:table name=" + name + "\ntype Row struct {\n\tID int64 `db:\"id\"`\n}\n"}
	}

	if err := genModule(t, model("rows")); err != nil {
		t.Fatal(err)
	}

	stat := func() time.Time {
		info, err := os.Stat("row.tsq.go")
		if err != nil {
			t.Fatal(err)
		}

		return info.ModTime()
	}

	before := stat()
	time.Sleep(20 * time.Millisecond)

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	if !stat().Equal(before) {
		t.Error("a run that changed nothing wrote row.tsq.go again")
	}

	writeTestFile(t, "model.go", model("renamed_rows")["model.go"])

	stderr := new(bytes.Buffer)
	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(stderr)
	GenCmd.SetArgs([]string{"."})

	if err := GenCmd.Execute(); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(stderr.String(), "rows: drop table is written commented out") {
		t.Fatalf("gen without -v printed:\n%s\nwant the drop warning", stderr.String())
	}
}

// TestGenReadsDirectivesOfAGroupedTypeFromTheType covers //tsq:table written above
// a type ( ... ) group: it was applied to every struct of the group. A directive
// belongs to the type it is written on.
func TestGenReadsDirectivesOfAGroupedTypeFromTheType(t *testing.T) {
	source := "package gentest\n\n//tsq:table\ntype (\n\tUser struct {\n\t\tID int64 `db:\"id\"`\n\t}\n\n\t//tsq:table\n\tOther struct {\n\t\tID int64 `db:\"id\"`\n\t}\n)\n"

	if err := genModule(t, map[string]string{"model.go": source}); err == nil || !strings.Contains(err.Error(), "model.go:3:1") || !strings.Contains(err.Error(), "belongs to no one type") {
		t.Fatalf("tsq gen = %v; want the group's directive refused where it is", err)
	}

	writeTestFile(t, "model.go", strings.Replace(source, "//tsq:table\ntype (", "type (", 1))

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat("user.tsq.go"); !os.IsNotExist(err) {
		t.Errorf("user.tsq.go: %v; want only the type with the directive generated", err)
	}

	if _, err := os.Stat("other.tsq.go"); err != nil {
		t.Errorf("other.tsq.go: %v; want Other, which carries the directive, generated", err)
	}
}

// TestGenTakesAFullTextIndexBesideAUniqueOne covers a full-text index over the
// fields a unique index has, which was refused as the same index twice although
// the two answer different queries; a search field named twice was kept twice.
func TestGenTakesAFullTextIndexBesideAUniqueOne(t *testing.T) {
	model := func(directives string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\n//tsq:table\n" + directives + "type Post struct {\n\tID int64 `db:\"id\"`\n\tTitle string `db:\"title,size:64\"`\n}\n"}
	}

	t.Run("unique and full-text", func(t *testing.T) {
		if err := genModule(t, model("//tsq:unique Title\n//tsq:fulltext Title\n")); err != nil {
			t.Fatalf("tsq gen = %v", err)
		}
	})

	t.Run("search twice", func(t *testing.T) {
		if err := genModule(t, model("//tsq:search Title\n//tsq:search Title\n")); err == nil || !strings.Contains(err.Error(), "search already covers Title") {
			t.Fatalf("tsq gen = %v; want the repeated field refused", err)
		}
	})
}

// TestGenRenamesAColumnOnlyInCase covers a db tag changed only in case: it was
// written as ADD COLUMN "Name" and a commented drop of name, and the ADD fails on
// MySQL and SQLite, which take both spellings for one column.
func TestGenRenamesAColumnOnlyInCase(t *testing.T) {
	model := func(tag string) map[string]string {
		return map[string]string{"model.go": "package gentest\n\n//tsq:table name=people\ntype Person struct {\n\tID int64 `db:\"id\"`\n\tName string `db:\"" + tag + "\"`\n}\n"}
	}

	if err := genModule(t, model("name,size:20")); err != nil {
		t.Fatal(err)
	}

	initial, err := os.ReadFile("sqlite.sql")
	if err != nil {
		t.Fatal(err)
	}

	// Renamed in case and widened: SQLite rebuilds the table.
	writeTestFile(t, "model.go", model("Name,size:40")["model.go"])

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	last := func(file string) string {
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}

		return string(content[strings.LastIndex(string(content), "-- Migration: "):])
	}

	if pg := last("postgres.sql"); !strings.Contains(pg, `ALTER TABLE "people" RENAME COLUMN "name" TO "Name";`) || strings.Contains(pg, "ADD COLUMN") {
		t.Fatalf("postgres migration:\n%s", pg)
	}

	if my := last("mysql.sql"); strings.Contains(my, "ADD COLUMN") || !strings.Contains(my, "nothing to run") {
		t.Fatalf("mysql migration:\n%s", my)
	}

	shell, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Skip("sqlite3 shell not installed")
	}

	db := filepath.Join(t.TempDir(), "t.db")
	for _, script := range []string{string(initial), `INSERT INTO people (name) VALUES ('kept');`, last("sqlite.sql")} {
		if out, err := runSQLiteShell(shell, db, script); err != nil || strings.Contains(out, "Error") {
			t.Fatalf("sqlite3: %v\n%s", err, out)
		}
	}

	if out, err := runSQLiteShell(shell, db, `SELECT Name FROM people;`); err != nil || strings.TrimSpace(out) != "kept" {
		t.Fatalf("after the migration: %q, %v; want the value kept", out, err)
	}
}

// TestGenRefusesToGenerateNothing covers runs that produced nothing without a
// word: a package with no //tsq: struct (which got four empty schema files and
// the advice to run them) and a result none of whose fields has a tsq tag. A
// unique index over Tags and Tag generated two parameters named tags.
func TestGenRefusesToGenerateNothing(t *testing.T) {
	t.Run("no directive", func(t *testing.T) {
		if err := genModule(t, map[string]string{"model.go": "package gentest\n\ntype Row struct{ ID int64 }\n"}); err == nil || !strings.Contains(err.Error(), "has no struct with a //tsq:table") {
			t.Fatalf("tsq gen = %v", err)
		}
	})

	t.Run("result without columns", func(t *testing.T) {
		source := "package gentest\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n}\n\n//tsq:result\ntype View struct {\n\tID int64\n}\n"
		if err := genModule(t, map[string]string{"model.go": source}); err == nil || !strings.Contains(err.Error(), "no field has a tsq tag") {
			t.Fatalf("tsq gen = %v", err)
		}
	})

	t.Run("parameters of Tags and Tag", func(t *testing.T) {
		source := "package gentest\n\n//tsq:table\n//tsq:unique Tags,Tag\ntype Row struct {\n\tID int64 `db:\"id\"`\n\tTags string `db:\"tags,size:32\"`\n\tTag string `db:\"tag,size:32\"`\n}\n"
		if err := genModule(t, map[string]string{"model.go": source}); err != nil {
			t.Fatal(err)
		}

		tidyGenTestModule(t)

		if output, err := exec.Command("go", "build", "./...").CombinedOutput(); err != nil {
			t.Fatalf("generated code does not compile: %v\n%s", err, output)
		}
	})
}

// TestMigrationFillsNullsOfAColumnThatBecomesNotNull covers a nullable column made
// NOT NULL: SQLite filled its NULLs with the zero value, PostgreSQL wrote a bare
// SET NOT NULL that fails on them, and MySQL a MODIFY that fails in strict mode
// and turns them into zero values silently otherwise.
func TestMigrationFillsNullsOfAColumnThatBecomesNotNull(t *testing.T) {
	model := func(field string) string {
		return "package gentest\n\n//tsq:table\ntype Row struct {\n\tID int64 `db:\"id\"`\n\t" + field + "\n}\n"
	}

	if err := genModule(t, map[string]string{"model.go": model("Note *string `db:\"note,size:20\"`")}); err != nil {
		t.Fatal(err)
	}

	writeTestFile(t, "model.go", model("Note string `db:\"note,size:20\"`"))

	if err := runGen(t); err != nil {
		t.Fatal(err)
	}

	for file, want := range map[string][]string{
		"postgres.sql": {`-- row: note becomes NOT NULL; rows holding NULL get ''`, `UPDATE "row" SET "note" = '' WHERE "note" IS NULL;`, `ALTER TABLE "row" ALTER COLUMN "note" SET NOT NULL;`},
		"mysql.sql":    {"-- row: note becomes NOT NULL; rows holding NULL get ''", "UPDATE `row` SET `note` = '' WHERE `note` IS NULL;", "MODIFY COLUMN `note` VARCHAR(20) NOT NULL;"},
	} {
		ddl, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}

		for _, w := range want {
			if !strings.Contains(string(ddl), w) {
				t.Errorf("%s lacks %q:\n%s", file, w, ddl)
			}
		}
	}
}
