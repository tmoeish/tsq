package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
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
			err := exactOnePackageArg(nil, tc.args)
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
	v = true
	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(stderr)
	GenCmd.SetArgs([]string{"."})
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
		`ALTER TABLE "users" ADD COLUMN "name" VARCHAR(128) NOT NULL;`,
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
	GenCmd.SetArgs([]string{"."})
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
		printDDLChangeSummary(buf, ddlArtifacts{
			hasChange: true,
			recordTables: []ddlStateRecordTable{
				{
					Table:   "category",
					Columns: []string{"add column name", "drop column abc"},
					Indexes: []string{"add unique index ux_name", "drop index idx_type"},
				},
			},
		})
		if got := buf.String(); got != "ddl:\n  <category>:\n    columns:\n      add column name\n      drop column abc\n    indexes:\n      add unique index ux_name\n      drop index idx_type\n" {
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
		printDDLChangeSummary(buf, ddlArtifacts{
			hasChange:    true,
			recordTables: recordTables,
		})
		if got := buf.String(); got != "ddl:\n  <category>:\n    columns:\n      alter column abc (type)\n  <item>:\n    columns:\n      add column sku\n      drop column spu_name\n" {
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
		printDDLChangeSummary(buf, ddlArtifacts{
			hasChange:    true,
			recordTables: recordTables,
		})
		if got := buf.String(); got != "ddl:\n  <new_table>:\n    create table\n" {
			t.Fatalf("unexpected create table summary %q", got)
		}
	})

	t.Run("drop table is single line", func(t *testing.T) {
		buf := new(bytes.Buffer)
		printDDLChangeSummary(buf, ddlArtifacts{
			hasChange: true,
			recordTables: []ddlStateRecordTable{
				{Table: "new_table", Columns: []string{"drop table"}},
			},
		})
		if got := buf.String(); got != "ddl:\n  <new_table>:\n    drop table\n" {
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

	writeTestFile(t, modelPath, `package gentest

//tsq:table name=users
type User struct {
	ID   int64  `+"`db:\"id\"`"+`
	Name string `+"`db:\"name,size:128\"`"+`
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
		`ALTER TABLE "users" RENAME TO "__tsq_rebuild_users";`,
		`CREATE TABLE IF NOT EXISTS "users" (`,
		`INSERT INTO "users" ("id", "name") SELECT "id", "name" FROM "__tsq_rebuild_users";`,
		`DROP TABLE "__tsq_rebuild_users";`,
		`COMMIT;`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected sqlite incremental ddl to contain %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, ";;") {
		t.Fatalf("expected sqlite ddl history to avoid duplicate semicolons, got:\n%s", got)
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
				"`created_at` DATETIME NOT NULL",
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
				`"flags" INTEGER NOT NULL`,
				`"named_flags" INTEGER NOT NULL`,
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
	if !strings.Contains(got, "use Go field names, not column names") {
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
	if err == nil {
		t.Fatal("expected outdated plan to fail check")
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

func TestValidateResultFieldsRejectsIncompatibleTypes(t *testing.T) {
	dto := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true},
		TypeInfo:  genmodel.TypeInfo{TypeName: "UserResult"},
		Fields: []genmodel.FieldInfo{
			{Name: "OrderTime", Column: "Order.CreatedAt", Type: genmodel.TypeInfo{TypeName: "string"}},
		},
	}

	structsByName := map[string]*genmodel.StructInfo{
		"Order": {
			TableMeta: &genmodel.TableMeta{Table: "order"},
			FieldsByName: map[string]genmodel.FieldInfo{
				"CreatedAt": {
					Name:   "CreatedAt",
					Column: "created_at",
					Type: genmodel.TypeInfo{
						Package:  genmodel.PackageInfo{Path: "time", Name: "time"},
						TypeName: "Time",
					},
				},
			},
		},
	}

	if err := validateResultFields(dto, structsByName); err == nil {
		t.Fatal("expected incompatible Result field type to return an error")
	}
}

func TestValidateResultFieldsAcceptsMatchingTypes(t *testing.T) {
	timeType := genmodel.TypeInfo{
		Package:  genmodel.PackageInfo{Path: "time", Name: "time"},
		TypeName: "Time",
	}
	dto := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true},
		TypeInfo:  genmodel.TypeInfo{TypeName: "UserResult"},
		Fields: []genmodel.FieldInfo{
			{Name: "OrderTime", Column: "Order.CreatedAt", Type: timeType},
		},
	}

	structsByName := map[string]*genmodel.StructInfo{
		"Order": {
			TableMeta: &genmodel.TableMeta{Table: "order"},
			FieldsByName: map[string]genmodel.FieldInfo{
				"CreatedAt": {Name: "CreatedAt", Column: "created_at", Type: timeType},
			},
		},
	}

	if err := validateResultFields(dto, structsByName); err != nil {
		t.Fatalf("expected matching Result field type to pass, got %v", err)
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

func TestValidateFieldDatabaseTypeRequiresStringSearchFields(t *testing.T) {
	search := map[string]struct{}{"F": {}}
	tests := []struct {
		name  string
		field genmodel.FieldInfo
		ok    bool
	}{
		{"string", genmodel.FieldInfo{Name: "F", Type: genmodel.TypeInfo{TypeName: "string"}}, true},
		{"int", genmodel.FieldInfo{Name: "F", Type: genmodel.TypeInfo{TypeName: "int64"}}, false},
		{"null string", genmodel.FieldInfo{Name: "F", Type: genmodel.TypeInfo{TypeName: "NullString", Package: genmodel.PackageInfo{Path: "database/sql"}}}, false},
		{"pointer", genmodel.FieldInfo{Name: "F", IsPointer: true, Type: genmodel.TypeInfo{TypeName: "string"}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateFieldDatabaseType(tt.field, search); (err == nil) != tt.ok {
				t.Fatalf("validateFieldDatabaseType() = %v, want ok=%v", err, tt.ok)
			}
		})
	}
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
	if !strings.Contains(rendered, "type_s ...int64") || !strings.Contains(rendered, "type_ int64") {
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
	return renderGenerationModel(generationModel{
		Data:       data,
		Template:   t,
		Filename:   filepath.Join(dir, generatedFilename(data)),
		ErrorLabel: "template rendering failed",
	})
}

func genResult(data *genmodel.StructInfo, t *template.Template, dir string) error {
	return renderGenerationModel(generationModel{
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
