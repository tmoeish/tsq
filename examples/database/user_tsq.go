// Code generated by tsq-dev. DO NOT EDIT.
package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/juju/errors"
	"github.com/tmoeish/tsq"
	"gopkg.in/gorp.v2"

	null "gopkg.in/nullbio/null.v6"
)

// =============================================================================
// Table Interface Implementation
// =============================================================================

func init() {
	tsq.RegisterTable(TableUser)
}

// TableUser implements the tsq.Table interface for User.
var TableUser tsq.Table = User{}

// TableUserCols is the list of columns for User table.
var TableUserCols = []tsq.Column{
	User_CT,
	User_Email,
	User_ID,
	User_Name,
	User_OrgID,
}

// Column definitions for User table.
var (
	User_CT = tsq.NewCol[null.Time](TableUser, "ct", "ct", func(t any) any {
		return &t.(*User).CT
	})
	User_Email = tsq.NewCol[string](TableUser, "email", "email", func(t any) any {
		return &t.(*User).Email
	})
	User_ID = tsq.NewCol[int64](TableUser, "id", "id", func(t any) any {
		return &t.(*User).ID
	})
	User_Name = tsq.NewCol[string](TableUser, "name", "name", func(t any) any {
		return &t.(*User).Name
	})
	User_OrgID = tsq.NewCol[int64](TableUser, "org_id", "org_id", func(t any) any {
		return &t.(*User).OrgID
	})
)

// Init initializes the User table in the database.
func (u User) Init(db *gorp.DbMap, upsertIndexies bool) error {
	db.AddTableWithName(u, "user").SetKeys(true, "ID")

	if !upsertIndexies {
		return nil
	}

	// Upsert Ux list
	if err := tsq.UpsertIndex(db, "user", true, "ux_name", []string{`name`}); err != nil {
		return errors.Annotatef(err, "upsert ux %s for %s", "ux_name", u.Table())
	}

	return nil
}

// Table returns the database table name for User.
func (u User) Table() string { return "user" }

// KwList returns columns that support keyword search for User.
func (u User) KwList() []tsq.Column {
	return []tsq.Column{
		User_Name,
		User_Email,
	}
}

// =============================================================================
// Query by Primary Key
// =============================================================================
var getUserByIDQuery = tsq.
	Select(TableUserCols...).
	Where(User_ID.EQVar()).
	MustBuild()

// GetUserByID retrieves a User record by its ID.
// Returns (nil, nil) if the record is not found.
func GetUserByID(
	ctx context.Context,
	db gorp.SqlExecutor,
	id int64,
) (*User, error) {
	row := &User{}
	err := getUserByIDQuery.Load(ctx, db, row, id)
	switch errors.Cause(err) {
	case nil:
		return row, nil
	case sql.ErrNoRows:
		return nil, nil
	default:
		return nil, errors.Trace(err)
	}
}

// GetUserByIDOrErr retrieves a User record by its ID.
// Returns (nil, sql.ErrNoRows) if the record is not found.
func GetUserByIDOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	id int64,
) (*User, error) {
	row := &User{}
	return row, getUserByIDQuery.Load(ctx, db, row, id)
}

// ListUserByIDIn retrieves multiple User records by a set of ID values.
// Records not found are silently ignored.
func ListUserByIDIn(
	ctx context.Context,
	db gorp.SqlExecutor,
	ids ...int64,
) ([]*User, error) {
	query := tsq.
		Select(TableUserCols...).
		Where(User_ID.In(ids...)).
		MustBuild()

	return tsq.List[User](ctx, db, query)
}

// ListUserByIDInOrErr retrieves multiple User records by a set of ID values.
// Returns an error if any of the specified records are not found.
func ListUserByIDInOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	ids ...int64,
) ([]*User, error) {
	idSet := map[int64]bool{}
	for _, i := range ids {
		idSet[i] = true
	}
	query := tsq.
		Select(TableUserCols...).
		Where(User_ID.In(ids...)).
		MustBuild()

	list, err := tsq.List[User](ctx, db, query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, i := range list {
		delete(idSet, i.ID)
	}
	if len(idSet) > 0 {
		var missings []int64
		for i := range idSet {
			missings = append(missings, i)
		}
		return nil, errors.Errorf("User(s) not found: %v", missings)
	}
	return list, nil
}

// =============================================================================
// CRUD Operations
// =============================================================================

// Insert inserts a new User record into the database.
// Automatically sets creation and modification timestamps if configured.
func (u *User) Insert(
	ctx context.Context,
	db gorp.SqlExecutor,
) error {
	u.CT = null.TimeFrom(time.Now())
	err := tsq.Insert(ctx, db, u)
	if err != nil {
		return errors.Annotate(err, tsq.PrettyJSON(u))
	}

	return nil
}

// Update updates an existing User record in the database.
// Automatically updates the modification timestamp if configured.
func (u *User) Update(
	ctx context.Context,
	db gorp.SqlExecutor,
) error {
	err := tsq.Update(ctx, db, u)
	if err != nil {
		return errors.Annotate(err, tsq.PrettyJSON(u))
	}

	return nil
}

// Delete permanently removes a User record from the database.
func (u *User) Delete(
	ctx context.Context,
	db gorp.SqlExecutor,
) error {
	err := tsq.Delete(ctx, db, u)
	if err != nil {
		return errors.Annotate(err, tsq.PrettyJSON(u))
	}

	return nil
}

// ListUserByQuery executes a custom query to retrieve User records.
func ListUserByQuery(
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *tsq.Query,
	args ...any,
) ([]*User, error) {
	return tsq.List[User](ctx, tx, qb, args...)
}

// PageUserByQuery executes a custom query with pagination to retrieve User records.
func PageUserByQuery(
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *tsq.PageReq,
	qb *tsq.Query,
	args ...any,
) (*tsq.PageResp[User], error) {
	return tsq.Page[User](ctx, tx, page, qb, args...)
}

// =============================================================================
// List All Records
// =============================================================================
// listUserQuery is the base query for retrieving all User records.
var listUserQuery = tsq.
	Select(TableUserCols...).
	KwSearch(TableUser.KwList()...).
	MustBuild()

// CountUser returns the total count of User records.
func CountUser(
	ctx context.Context,
	tx gorp.SqlExecutor,
) (int, error) {
	return listUserQuery.Count(ctx, tx)
}

// ListUser retrieves all User records from the database.
func ListUser(
	ctx context.Context,
	tx gorp.SqlExecutor,
) ([]*User, error) {
	return tsq.List[User](ctx, tx, listUserQuery)
}

// PageUser retrieves User records with pagination support.
func PageUser(
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *tsq.PageReq,
) (*tsq.PageResp[User], error) {
	return tsq.Page[User](ctx, tx, page, listUserQuery)
}

// =============================================================================
// Query by Unique Indexes
// =============================================================================
var getUserByNameQuery = tsq.
	Select(TableUserCols...).
	Where(
		User_Name.EQVar(),
	).
	KwSearch(TableUser.KwList()...).
	MustBuild()

// GetUserByName retrieves a User record by unique index ux_name.
// Returns (nil, nil) if the record is not found.
func GetUserByName(
	ctx context.Context,
	db gorp.SqlExecutor,
	name string,
) (*User, error) {
	query := getUserByNameQuery

	row := &User{}
	err := query.Load(ctx, db, row,
		name,
	)
	switch errors.Cause(err) {
	case nil:
		return row, nil
	case sql.ErrNoRows:
		return nil, nil
	default:
		return nil, errors.Trace(err)
	}
}

// GetUserByNameOrErr retrieves a User record by unique index ux_name.
// Returns (nil, sql.ErrNoRows) if the record is not found.
func GetUserByNameOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	name string,
) (*User, error) {
	query := getUserByNameQuery

	row := &User{}
	err := query.Load(ctx, db, row,
		name,
	)
	return row, errors.Trace(err)
}

// ExistsUserByName checks whether a User record exists by unique index ux_name.
func ExistsUserByName(
	ctx context.Context,
	db gorp.SqlExecutor,
	name string,
) (bool, error) {
	query := getUserByNameQuery

	rs, err := query.Exists(ctx, db,
		name,
	)
	return rs, errors.Trace(err)
}
