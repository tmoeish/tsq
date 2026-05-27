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

	"github.com/tmoeish/tsq/v4/internal/genmodel"
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
		"<struct>_tsq.go",
		"<result>_result_tsq.go",
		"sqlite.sql / mysql.sql / postgres.sql",
		"ddl.json",
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

// @TABLE(name="users", pk="PK,true", created_at)
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
		"runtime_tsq.go",
		"user_tsq.go",
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

// @TABLE(
//   ux=[{name="ux_users_email",fields=["Email"]}]
// )
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

	runtimeFile, err := os.ReadFile(filepath.Join(dir, "runtime_tsq.go"))
	if err != nil {
		t.Fatalf("failed to read runtime_tsq.go: %v", err)
	}

	rendered := string(runtimeFile)
	for _, want := range []string{
		"func TSQTables() []tsq.TableRegistration",
		"Table: TableUser",
		`Name: "ux_users_email"`,
		`Fields: []string{"email"}`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected runtime_tsq.go to contain %q, got:\n%s", want, rendered)
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

// @TABLE(
//   name="orders",
//   deleted_at,
//   idx=[{fields=["Status"]}],
// )
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

	runtimeFile, err := os.ReadFile(filepath.Join(dir, "runtime_tsq.go"))
	if err != nil {
		t.Fatalf("failed to read runtime_tsq.go: %v", err)
	}
	if got := string(runtimeFile); !strings.Contains(got, `Fields: []string{"deleted_at", "status"}`) {
		t.Fatalf("expected runtime index fields to include deleted_at prefix, got:\n%s", got)
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

func TestGenCmdGeneratesIncrementalDDLOnSubsequentRuns(t *testing.T) {
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

// @TABLE(name="users")
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

// @TABLE(name="users")
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

	incremental, err := os.ReadFile(filepath.Join(dir, "postgres.incremental.sql"))
	if err != nil {
		t.Fatalf("failed to read postgres incremental ddl: %v", err)
	}
	if !strings.Contains(string(incremental), `ALTER TABLE "users" ADD COLUMN "name" VARCHAR(128) NOT NULL;`) {
		t.Fatalf("expected postgres incremental ddl to add name column, got:\n%s", string(incremental))
	}

	if got := stderr.String(); !strings.Contains(got, "sqlite=sqlite.incremental.sql mysql=mysql.incremental.sql postgres=postgres.incremental.sql") {
		t.Fatalf("expected stderr guidance to mention incremental ddl files, got:\n%s", got)
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

	stderr.Reset()
	GenCmd.SetOut(new(bytes.Buffer))
	GenCmd.SetErr(stderr)
	GenCmd.SetArgs([]string{"."})
	if err := GenCmd.Execute(); err != nil {
		t.Fatalf("third GenCmd.Execute() error = %v", err)
	}

	for _, name := range []string{
		"sqlite.incremental.sql",
		"mysql.incremental.sql",
		"postgres.incremental.sql",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("expected no-change run to remove %s, got err=%v", name, err)
		}
	}

	if got := stderr.String(); strings.Contains(got, "incremental.sql") {
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

func TestGenCmdGeneratesSQLiteRebuildDDLForTypeChange(t *testing.T) {
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

// @TABLE(name="users")
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

// @TABLE(name="users")
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

	incremental, err := os.ReadFile(filepath.Join(dir, "sqlite.incremental.sql"))
	if err != nil {
		t.Fatalf("failed to read sqlite incremental ddl: %v", err)
	}

	got := string(incremental)
	for _, want := range []string{
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
		t.Fatalf("expected sqlite incremental ddl to avoid duplicate semicolons, got:\n%s", got)
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

// @TABLE(
//   name="task",
//   pk="ID,true",
//   idx=[{fields=["State"]}],
// )
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

// @TABLE(name="artifacts", pk="ID,true")
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

// @TABLE(
//   name="users",
//   ux=[
//     {fields=["name"]},
//   ],
// )
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
	if !strings.Contains(got, "model.go:6") {
		t.Fatalf("expected gen error to include file and line, got %q", got)
	}
	if !strings.Contains(got, "use struct field names, not db column names") {
		t.Fatalf("expected gen error to keep field guidance, got %q", got)
	}
}

func TestGenerationPlanStatusFor(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "user_tsq.go")
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
	target := filepath.Join(dir, "user_tsq.go")
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
		{Filename: "user_tsq.go", Status: generationPlanUpdate},
		{Filename: "org_tsq.go", Status: generationPlanCreate},
		{Filename: "item_tsq.go", Status: generationPlanUnchanged},
	})
	if err == nil {
		t.Fatal("expected outdated plan to fail check")
	}

	got := err.Error()
	for _, want := range []string{"generated files are out of date", "UPDATE user_tsq.go", "CREATE org_tsq.go"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected check error to mention %q, got %q", want, got)
		}
	}
}

func TestGenDryRunPrintsStatuses(t *testing.T) {
	buf := new(bytes.Buffer)
	printGenerationPlan(buf, []generationPlanEntry{
		{Filename: "user_tsq.go", Status: generationPlanCreate},
		{Filename: "org_tsq.go", Status: generationPlanUpdate},
		{Filename: "item_tsq.go", Status: generationPlanUnchanged},
	})

	got := buf.String()
	for _, want := range []string{"CREATE user_tsq.go", "UPDATE org_tsq.go", "UNCHANGED item_tsq.go"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected dry-run output to mention %q, got %q", want, got)
		}
	}
}

func TestBuildGenerationPlanDetectsStaleGeneratedFiles(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "orphan_tsq.go")
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
	manual := filepath.Join(dir, "manual_tsq.go")
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
		{Filename: "orphan_tsq.go", Status: generationPlanStale},
	})
	if err == nil {
		t.Fatal("expected stale generated files to fail check")
	}
	if !strings.Contains(err.Error(), "STALE orphan_tsq.go") {
		t.Fatalf("expected stale generated file in error, got %q", err.Error())
	}
}

func TestGenDryRunPrintsStaleStatuses(t *testing.T) {
	buf := new(bytes.Buffer)
	printGenerationPlan(buf, []generationPlanEntry{
		{Filename: "orphan_tsq.go", Status: generationPlanStale},
	})

	if !strings.Contains(buf.String(), "STALE orphan_tsq.go") {
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
			FieldMap: map[string]genmodel.FieldInfo{
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
			FieldMap: map[string]genmodel.FieldInfo{
				"Profile_ID": {Name: "Profile_ID", Column: "profile_id"},
			},
		},
		"User_Profile": {
			TableMeta: &genmodel.TableMeta{Table: "user_profile"},
			FieldMap: map[string]genmodel.FieldInfo{
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
			FieldMap: map[string]genmodel.FieldInfo{
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
			FieldMap: map[string]genmodel.FieldInfo{
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
		FieldMap: map[string]genmodel.FieldInfo{
			"UserID": {Name: "UserID", Column: "User.ID"},
		},
	}

	normalizeResultColumns(dto)

	if got := dto.Fields[0].Column; got != "User_ID" {
		t.Fatalf("expected Result field column to be normalized, got %q", got)
	}

	if got := dto.FieldMap["UserID"].Column; got != "User_ID" {
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
		Recv:       "uo",
		TSQVersion: "test",
		Fields: []genmodel.FieldInfo{
			{Name: "UserID", Type: genmodel.TypeInfo{TypeName: "int64"}, Column: "User_ID", JsonTag: "user_id"},
		},
	}

	tpl, err := template.New("tsq_result.go.tmpl").Funcs(funcMap()).Parse(defaultResultTpl)
	if err != nil {
		t.Fatalf("parse Result template: %v", err)
	}

	if err := genResult(data, tpl, dir); err != nil {
		t.Fatalf("render Result template: %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "userorder_result_tsq.go"))
	if err != nil {
		t.Fatalf("read generated Result file: %v", err)
	}

	rendered := string(contents)
	for _, want := range []string{
		"var ResultUserOrder = UserOrder{}",
		"func (uo UserOrder) TSQResult() {}",
		"func (uo UserOrder) Cols() []tsq.BoundColumn[UserOrder] {",
		"UserOrder_UserID = tsq.MapInto",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected generated Result file to contain %q, got:\n%s", want, rendered)
		}
	}

	for _, blocked := range []string{
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

func TestValidateIndexNameCollisionsRejectsCrossTableReuse(t *testing.T) {
	list := []*genmodel.StructInfo{
		{
			TableMeta: &genmodel.TableMeta{
				Table:  "user",
				UxList: []genmodel.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
			},
		},
		{
			TableMeta: &genmodel.TableMeta{
				Table:  "org",
				UxList: []genmodel.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
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
			Table: "user",
			PK:    "PK",
		},
		TypeInfo: genmodel.TypeInfo{TypeName: "User"},
		FieldMap: map[string]genmodel.FieldInfo{
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
			Table: "blob_user",
			PK:    "PK",
		},
		TypeInfo: genmodel.TypeInfo{TypeName: "BlobUser"},
		FieldMap: map[string]genmodel.FieldInfo{
			"PK": {
				Name:    "PK",
				IsArray: true,
				Type:    genmodel.TypeInfo{TypeName: "byte"},
			},
		},
	}

	if err := validateStructForGeneration(data, nil); err == nil {
		t.Fatal("expected slice primary key to be rejected")
	}
}

func TestTableTemplateOrErrPreservesErrNoRows(t *testing.T) {
	want := `if errors.Is(err, {{ GeneratedSQLRef "ErrNoRows" }}) {`
	if count := strings.Count(defaultTableTpl, want); count < 4 {
		t.Fatalf("expected table template to preserve sql.ErrNoRows in every OrErr helper, count=%d", count)
	}
}

func TestTableTemplateAvoidsKeywordParameterNames(t *testing.T) {
	dir := t.TempDir()

	tpl, err := template.New("tsq.go.tmpl").Funcs(funcMap()).Parse(defaultTableTpl)
	if err != nil {
		t.Fatalf("failed to parse table template: %v", err)
	}

	field := genmodel.FieldInfo{Name: "Type", Column: "type", JsonTag: "type", Type: genmodel.TypeInfo{TypeName: "int64"}}
	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table: "keyworded",
			PK:    "Type",
			AI:    true,
		},
		TypeInfo: genmodel.TypeInfo{Package: genmodel.PackageInfo{Name: "example"}, TypeName: "Keyworded"},
		Fields:   []genmodel.FieldInfo{field},
		FieldMap: map[string]genmodel.FieldInfo{
			"Type": field,
		},
		Recv:       "k",
		TSQVersion: "test",
	}

	if err := gen(data, tpl, dir); err != nil {
		t.Fatalf("expected template with keyword field to render valid Go, got %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "keyworded_tsq.go"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	rendered := string(contents)
	if !strings.Contains(rendered, "type_ int64") {
		t.Fatalf("expected generated parameter to avoid Go keyword, got:\n%s", rendered)
	}

	if strings.Contains(rendered, "\ttype int64") {
		t.Fatalf("generated code still contains keyword parameter:\n%s", rendered)
	}
}

func TestTableTemplateAnnotatesQueryListErrorsWithSourceIndexName(t *testing.T) {
	dir := t.TempDir()

	tpl, err := template.New("tsq.go.tmpl").Funcs(funcMap()).Parse(defaultTableTpl)
	if err != nil {
		t.Fatalf("failed to parse table template: %v", err)
	}

	idField := genmodel.FieldInfo{Name: "PK", Column: "id", JsonTag: "id", Type: genmodel.TypeInfo{TypeName: "int64"}}
	orgField := genmodel.FieldInfo{Name: "OrgID", Column: "org_id", JsonTag: "org_id", Type: genmodel.TypeInfo{TypeName: "int64"}}
	itemField := genmodel.FieldInfo{Name: "ItemID", Column: "item_id", JsonTag: "item_id", Type: genmodel.TypeInfo{TypeName: "int64"}}

	data := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{
			Table: "order",
			PK:    "PK",
			QueryList: []genmodel.IndexInfo{
				{
					Name:       "OrgIDAndItemID",
					SourceName: "idx_order_org_item",
					Fields:     []string{"OrgID", "ItemID"},
				},
				{
					Name:       "OrgIDAndItemIDIn",
					SourceName: "idx_order_org_item",
					Fields:     []string{"OrgID", "ItemID"},
					IsSet:      true,
				},
			},
		},
		TypeInfo: genmodel.TypeInfo{Package: genmodel.PackageInfo{Name: "example"}, TypeName: "Order"},
		Fields:   []genmodel.FieldInfo{idField, orgField, itemField},
		FieldMap: map[string]genmodel.FieldInfo{
			"PK":     idField,
			"OrgID":  orgField,
			"ItemID": itemField,
		},
		Recv:       "o",
		TSQVersion: "test",
	}

	if err := gen(data, tpl, dir); err != nil {
		t.Fatalf("expected query list template to render valid Go, got %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "order_tsq.go"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	rendered := string(contents)
	for _, want := range []string{
		"query by index idx_order_org_item",
		"var ListOrderByOrgIDAndItemIDInQuery OrderGeneratedQuery",
		"Order_ItemID.InVar()",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected generated query list code to mention %q, got:\n%s", want, rendered)
		}
	}
}

func TestResolveTemplateTextUsesFallbackWithoutLeakingPreviousOverride(t *testing.T) {
	dir := t.TempDir()

	overridePath := filepath.Join(dir, "custom.tmpl")
	if err := os.WriteFile(overridePath, []byte("custom-template"), 0o644); err != nil {
		t.Fatalf("failed to write override template: %v", err)
	}

	override, err := resolveTemplateText(overridePath, defaultTableTpl, "template")
	if err != nil {
		t.Fatalf("expected override template to load, got %v", err)
	}

	if override != "custom-template" {
		t.Fatalf("unexpected override template: %q", override)
	}

	fallback, err := resolveTemplateText("", defaultTableTpl, "template")
	if err != nil {
		t.Fatalf("expected fallback template to load, got %v", err)
	}

	if fallback != defaultTableTpl {
		t.Fatal("expected empty override path to return embedded template")
	}

	if fallback == override {
		t.Fatal("expected fallback template to ignore previous override content")
	}
}

func TestGenDoesNotWriteBrokenGoOnFormatError(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user_tsq.go")
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

	target := filepath.Join(dir, "userresult_result_tsq.go")
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

	target := filepath.Join(dir, "user_tsq.go")
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

	target := filepath.Join(dir, "user_tsq.go")
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

	target := filepath.Join(dir, "user_tsq.go")
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

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get repo root for test module: %v", err)
	}

	return "module example.com/gentest\n\n" +
		"go 1.24.2\n\n" +
		"require github.com/tmoeish/tsq/v4 v4.0.2\n\n" +
		"replace github.com/tmoeish/tsq/v4 => " + wd + "\n"
}

func tidyGenTestModule(t *testing.T) {
	t.Helper()

	cmd := exec.Command("go", "mod", "tidy")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go mod tidy failed: %v\n%s", err, string(output))
	}
}
