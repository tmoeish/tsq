package tsq

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

var (
	mutationUserID    = NewCol[batchMutationUser, int64]("id", "id", func(t *batchMutationUser) *int64 { return &t.ID })
	mutationUserName  = NewCol[batchMutationUser, string]("name", "name", func(t *batchMutationUser) *string { return &t.Name })
	mutationUserEmail = NewCol[batchMutationUser, string]("email", "email", func(t *batchMutationUser) *string { return &t.Email })

	lockedUserID      = NewCol[optimisticMutationUser, int64]("id", "id", func(t *optimisticMutationUser) *int64 { return &t.ID })
	lockedUserName    = NewCol[optimisticMutationUser, string]("name", "name", func(t *optimisticMutationUser) *string { return &t.Name })
	lockedUserVersion = NewCol[optimisticMutationUser, int64]("version", "version", func(t *optimisticMutationUser) *int64 { return &t.Version })
)

func seedMutationUsers(t *testing.T, rt *Runtime, withVersion bool) {
	t.Helper()

	stmt := `INSERT INTO users (id, name, email) VALUES (1, 'alice', 'alice@example.com'), (2, 'bob', 'bob@example.com'), (3, 'carol', 'carol@example.com')`
	if withVersion {
		stmt = `INSERT INTO users (id, name, email, version) VALUES (1, 'alice', 'alice@example.com', 1), (2, 'bob', 'bob@example.com', 1), (3, 'carol', 'carol@example.com', 1)`
	}

	if _, err := rt.DB().ExecContext(context.Background(), stmt); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
}

func queryMutationUsers(t *testing.T, rt *Runtime) []batchMutationUser {
	t.Helper()

	rows, err := rt.DB().QueryContext(context.Background(), `SELECT id, name, email FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}

	defer func() { _ = rows.Close() }()

	var users []batchMutationUser

	for rows.Next() {
		var user batchMutationUser
		if err := rows.Scan(&user.ID, &user.Name, &user.Email); err != nil {
			t.Fatalf("scan row: %v", err)
		}

		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rows: %v", err)
	}

	return users
}

func TestUpdateTableRendersCanonicalSQL(t *testing.T) {
	mutation, err := UpdateTable[batchMutationUser]().
		SetVal(mutationUserName, "renamed").
		SetVar(mutationUserEmail).
		Where(mutationUserID.GTVar(), mutationUserName.NEVal("root")).
		Build()
	if err != nil {
		t.Fatalf("build update: %v", err)
	}

	want := `UPDATE "users" SET "name" = ?, "email" = ? WHERE ("users"."id" > ? AND "users"."name" <> ?)`
	if got := mutation.SQL(); got != want {
		t.Fatalf("unexpected SQL\n got: %s\nwant: %s", got, want)
	}
}

func TestDeleteFromRendersCanonicalSQL(t *testing.T) {
	mutation, err := DeleteFrom[batchMutationUser]().
		Where(mutationUserID.InVar()).
		Build()
	if err != nil {
		t.Fatalf("build delete: %v", err)
	}

	want := `DELETE FROM "users" WHERE "users"."id" IN (?)`
	if got := mutation.SQL(); got != want {
		t.Fatalf("unexpected SQL\n got: %s\nwant: %s", got, want)
	}
}

func TestUpdateTableBindsSetPlaceholdersBeforeWhere(t *testing.T) {
	rt := requireInitializedRuntime(t, newBatchMutationEngine(t))
	seedMutationUsers(t, rt, false)

	affected, err := UpdateTable[batchMutationUser]().
		SetVar(mutationUserName).
		Set(mutationUserEmail, mutationUserEmail.Exprf("%s || %s", "-bulk")).
		Where(mutationUserID.InVar()).
		Exec(context.Background(), rt, "bulk", []int64{1, 3})
	if err != nil {
		t.Fatalf("exec update: %v", err)
	}

	if affected != 2 {
		t.Fatalf("expected 2 affected rows, got %d", affected)
	}

	users := queryMutationUsers(t, rt)
	if len(users) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(users))
	}

	if users[0].Name != "bulk" || users[0].Email != "alice@example.com-bulk" {
		t.Fatalf("row 1 not updated: %+v", users[0])
	}

	if users[1].Name != "bob" || users[1].Email != "bob@example.com" {
		t.Fatalf("row 2 must be untouched: %+v", users[1])
	}

	if users[2].Name != "bulk" || users[2].Email != "carol@example.com-bulk" {
		t.Fatalf("row 3 not updated: %+v", users[2])
	}
}

func TestUpdateTableAssignsColumnExpressions(t *testing.T) {
	rt := requireInitializedRuntime(t, newBatchMutationEngine(t))
	seedMutationUsers(t, rt, false)

	affected, err := UpdateTable[batchMutationUser]().
		Set(mutationUserName, mutationUserEmail).
		Where(mutationUserID.EQVal(2)).
		Exec(context.Background(), rt)
	if err != nil {
		t.Fatalf("exec update: %v", err)
	}

	if affected != 1 {
		t.Fatalf("expected 1 affected row, got %d", affected)
	}

	if users := queryMutationUsers(t, rt); users[1].Name != "bob@example.com" {
		t.Fatalf("expected name copied from email, got %+v", users[1])
	}
}

func TestUpdateTableIncrementsVersionWithoutCheckingIt(t *testing.T) {
	rt := requireInitializedRuntime(t, newOptimisticMutationEngine(t))
	seedMutationUsers(t, rt, true)

	stale := &optimisticMutationUser{ID: 1, Name: "alice", Email: "alice@example.com", Version: 1}

	mutation := UpdateTable[optimisticMutationUser]().
		SetVal(lockedUserName, "renamed").
		Where(lockedUserID.EQVal(1)).
		MustBuild()

	if !strings.Contains(mutation.SQL(), `"version" = "version" + 1`) {
		t.Fatalf("expected version increment in SQL, got %s", mutation.SQL())
	}

	if strings.Contains(mutation.SQL(), `WHERE "users"."version"`) {
		t.Fatalf("bulk update must not check the version column, got %s", mutation.SQL())
	}

	affected, err := mutation.Exec(context.Background(), rt)
	if err != nil {
		t.Fatalf("exec update: %v", err)
	}

	if affected != 1 {
		t.Fatalf("expected 1 affected row, got %d", affected)
	}

	var version int64
	if err := rt.DB().QueryRowContext(context.Background(), `SELECT version FROM users WHERE id = 1`).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}

	if version != 2 {
		t.Fatalf("expected version 2 after bulk update, got %d", version)
	}

	stale.Name = "stale write"

	err = Update(context.Background(), rt, stale)
	if !IsOptimisticLockError(err) {
		t.Fatalf("expected stale row to hit an optimistic lock conflict, got %v", err)
	}
}

func TestDeleteFromIgnoresVersionAndReportsAffectedRows(t *testing.T) {
	rt := requireInitializedRuntime(t, newOptimisticMutationEngine(t))
	seedMutationUsers(t, rt, true)

	mutation := DeleteFrom[optimisticMutationUser]().
		Where(lockedUserID.GTEVar()).
		MustBuild()

	if strings.Contains(mutation.SQL(), "version") {
		t.Fatalf("bulk delete must not reference the version column, got %s", mutation.SQL())
	}

	affected, err := mutation.Exec(context.Background(), rt, int64(2))
	if err != nil {
		t.Fatalf("exec delete: %v", err)
	}

	if affected != 2 {
		t.Fatalf("expected 2 affected rows, got %d", affected)
	}

	var count int
	if err := rt.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 remaining row, got %d", count)
	}
}

func TestDeleteFromEmptyInVarAffectsNoRows(t *testing.T) {
	rt := requireInitializedRuntime(t, newBatchMutationEngine(t))
	seedMutationUsers(t, rt, false)

	affected, err := DeleteFrom[batchMutationUser]().
		Where(mutationUserID.InVar()).
		Exec(context.Background(), rt, []int64(nil))
	if err != nil {
		t.Fatalf("exec delete: %v", err)
	}

	if affected != 0 {
		t.Fatalf("expected an empty InVar to delete nothing, got %d rows", affected)
	}
}

func TestDeleteFromAcceptsSubqueryConditions(t *testing.T) {
	rt := requireInitializedRuntime(t, newBatchMutationEngine(t))
	seedMutationUsers(t, rt, false)

	ids, err := BuildSubquery(
		Select(mutationUserID).From(batchMutationUser{}).Where(mutationUserName.StartsWithVal("b")),
		mutationUserID,
	)
	if err != nil {
		t.Fatalf("build subquery: %v", err)
	}

	affected, err := DeleteFrom[batchMutationUser]().
		Where(mutationUserID.In(ids)).
		Exec(context.Background(), rt)
	if err != nil {
		t.Fatalf("exec delete: %v", err)
	}

	if affected != 1 {
		t.Fatalf("expected 1 affected row, got %d", affected)
	}
}

func TestUpdateBuilderBranchesDoNotShareState(t *testing.T) {
	base := UpdateTable[batchMutationUser]().SetVal(mutationUserName, "base")

	left := base.SetVal(mutationUserEmail, "left@example.com").Where(mutationUserID.EQVal(1)).MustBuild()
	right := base.Where(mutationUserID.EQVal(2)).MustBuild()

	if !strings.Contains(left.SQL(), `"email"`) {
		t.Fatalf("left branch lost its assignment: %s", left.SQL())
	}

	if strings.Contains(right.SQL(), `"email"`) {
		t.Fatalf("right branch inherited the left branch's assignment: %s", right.SQL())
	}
}

func TestMutationBuildRejectsInvalidShapes(t *testing.T) {
	otherTable := newMockTable("orders")
	otherCol := newColForTable[Table, int64](otherTable, "user_id", "user_id", nil)
	aliasedUser := newColForTable[batchMutationUser, int64](AliasTable(batchMutationUser{}, "u"), "id", "id", nil)

	cases := []struct {
		name  string
		build func() (any, error)
		want  string
	}{
		{
			name: "update_requires_assignment",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().Where(mutationUserID.EQVal(1)).Build()
			},
			want: "at least one assignment",
		},
		{
			name: "update_rejects_duplicate_assignment",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().
					SetVal(mutationUserName, "a").
					SetVal(mutationUserName, "b").
					Where(mutationUserID.EQVal(1)).
					Build()
			},
			want: "assigned more than once",
		},
		{
			name: "update_rejects_version_assignment",
			build: func() (any, error) {
				return UpdateTable[optimisticMutationUser]().
					SetVal(lockedUserVersion, 7).
					Where(lockedUserID.EQVal(1)).
					Build()
			},
			want: "incremented automatically",
		},
		{
			name: "update_rejects_expression_target",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().
					SetVal(mutationUserName.Expr("LOWER(%s)"), "a").
					Where(mutationUserID.EQVal(1)).
					Build()
			},
			want: "physical table column",
		},
		{
			name: "update_rejects_malformed_value_expression",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().
					Set(mutationUserEmail, mutationUserEmail.Exprf("%s || ?", "-x")).
					Where(mutationUserID.EQVal(1)).
					Build()
			},
			want: "placeholder count mismatch",
		},
		{
			name: "update_rejects_foreign_value_column",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().
					Set(mutationUserID, otherCol).
					Where(mutationUserID.EQVal(1)).
					Build()
			},
			want: "references table orders",
		},
		{
			name: "update_rejects_foreign_condition",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().
					SetVal(mutationUserName, "a").
					Where(otherCol.EQVal(1)).
					Build()
			},
			want: "references table orders",
		},
		{
			name: "update_rejects_aliased_condition",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().
					SetVal(mutationUserName, "a").
					Where(aliasedUser.EQVal(1)).
					Build()
			},
			want: "references alias u",
		},
		{
			name: "update_rejects_empty_where",
			build: func() (any, error) {
				return UpdateTable[batchMutationUser]().SetVal(mutationUserName, "a").Where().Build()
			},
			want: "at least one condition",
		},
		{
			name: "delete_rejects_foreign_condition",
			build: func() (any, error) {
				return DeleteFrom[batchMutationUser]().Where(otherCol.EQVal(1)).Build()
			},
			want: "references table orders",
		},
		{
			name: "delete_rejects_pointer_owner",
			build: func() (any, error) {
				return DeleteFrom[*batchMutationUser]().Where(mutationUserID.EQVal(1)).Build()
			},
			want: "not a pointer",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.build()
			if err == nil {
				t.Fatal("expected build error")
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestMutationExecRejectsMissingArguments(t *testing.T) {
	rt := requireInitializedRuntime(t, newBatchMutationEngine(t))

	_, err := UpdateTable[batchMutationUser]().
		SetVar(mutationUserName).
		Where(mutationUserID.EQVar()).
		Exec(context.Background(), rt, "only-one")
	if err == nil || !strings.Contains(err.Error(), "missing external query argument") {
		t.Fatalf("expected missing argument error, got %v", err)
	}
}

func TestMutationExecRequiresDialect(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	_, err = DeleteFrom[batchMutationUser]().
		Where(mutationUserID.EQVal(1)).
		Exec(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "dialect cannot be determined") {
		t.Fatalf("expected dialect error for a bare *sql.DB, got %v", err)
	}
}

func TestMutationNilSafety(t *testing.T) {
	var mutation *Mutation[batchMutationUser]

	if got := mutation.SQL(); got != "" {
		t.Fatalf("expected empty SQL for nil mutation, got %q", got)
	}

	rt := requireInitializedRuntime(t, newBatchMutationEngine(t))

	_, err := mutation.Exec(context.Background(), rt)
	if err == nil || !strings.Contains(err.Error(), "cannot be nil") {
		t.Fatalf("expected nil mutation error, got %v", err)
	}
}
