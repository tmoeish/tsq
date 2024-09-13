// Package database *_tsq.go files are generated by tsq. DO NOT EDIT.
package database

import (
	"context"
	"database/sql"
	"github.com/juju/errors"
	"github.com/tmoeish/tsq/pkg/tsq"
	"gopkg.in/gorp.v2"
	time "time"
)

func init() {
	tsq.RegisterTable(TableEnv)
}

////////////////////////////// Table interface /////////////////////////////////

// TableEnv table Env
var TableEnv tsq.Table = Env{}

// Table returns table name
func (r Env) Table() string {
	return "env"
}

// CustomID indicates if Env has custom id
func (r Env) CustomID() bool {
	return false
}

// IDFiled returns id field name
func (r Env) IDField() string {
	return "ID"
}

// VersionField returns version field name
func (r Env) VersionField() string {
	return "V"
}

// table Env column list
var (
	Env_AppCode = tsq.NewColumn[string](
		TableEnv,
		"app_code",
		func(holder any) any {
			return &holder.(*Env).AppCode
		},
	)
	Env_AppID = tsq.NewColumn[int64](
		TableEnv,
		"app_id",
		func(holder any) any {
			return &holder.(*Env).AppID
		},
	)
	Env_AppName = tsq.NewColumn[string](
		TableEnv,
		"app_name",
		func(holder any) any {
			return &holder.(*Env).AppName
		},
	)
	Env_CT = tsq.NewColumn[time.Time](
		TableEnv,
		"ct",
		func(holder any) any {
			return &holder.(*Env).CT
		},
	)
	Env_DT = tsq.NewColumn[int64](
		TableEnv,
		"dt",
		func(holder any) any {
			return &holder.(*Env).DT
		},
	)
	Env_EnvCode = tsq.NewColumn[string](
		TableEnv,
		"env_code",
		func(holder any) any {
			return &holder.(*Env).EnvCode
		},
	)
	Env_EnvLevel = tsq.NewColumn[EnvLevel](
		TableEnv,
		"env_level",
		func(holder any) any {
			return &holder.(*Env).EnvLevel
		},
	)
	Env_EnvName = tsq.NewColumn[string](
		TableEnv,
		"env_name",
		func(holder any) any {
			return &holder.(*Env).EnvName
		},
	)
	Env_ID = tsq.NewColumn[int64](
		TableEnv,
		"id",
		func(holder any) any {
			return &holder.(*Env).ID
		},
	)
	Env_ModifiedTime = tsq.NewColumn[time.Time](
		TableEnv,
		"modified_time",
		func(holder any) any {
			return &holder.(*Env).ModifiedTime
		},
	)
	Env_V = tsq.NewColumn[int64](
		TableEnv,
		"v",
		func(holder any) any {
			return &holder.(*Env).V
		},
	)
)

// Columns returns fileds of table Env
func (r Env) Columns() []tsq.IColumn {
	return []tsq.IColumn{
		Env_AppCode,
		Env_AppID,
		Env_AppName,
		Env_CT,
		Env_DT,
		Env_EnvCode,
		Env_EnvLevel,
		Env_EnvName,
		Env_ID,
		Env_ModifiedTime,
		Env_V,
	}
}

// KwList implements Table interface
func (r Env) KwList() []tsq.IColumn {
	return []tsq.IColumn{
		Env_EnvName,
	}
}

// UxMap implements Table interface
func (r Env) UxMap() map[string][]string {
	return map[string][]string{
		"app_id_and_env_code": {
			"dt",
			"app_id",
			"env_code",
		},
	}
}

// IdxMap implements Table interface
func (r Env) IdxMap() map[string][]string {
	return map[string][]string{
		"app_id_and_env_level": {
			"dt",
			"app_id",
			"env_level",
		},
		"env_level": {
			"dt",
			"env_level",
		},
	}
}

// Active checks if Env is active
func (r *Env) Active() bool {
	return r.DT == 0
}

// //////////////////////////// Query by ID /////////////////////////////////////
var getEnvByIDQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_ID.EQVar(),
	).
	MustBuild()

// GetEnvByID get Env by ID.
// If not found, (nil, nil) returned
func GetEnvByID(
	ctx context.Context,
	db gorp.SqlExecutor,
	id int64,
) (*Env, error) {
	row := &Env{}
	return row, tsq.TraceDB(ctx, func(ctx context.Context) error {
		err := getEnvByIDQuery.Load(
			ctx, db, row, id,
		)
		switch errors.Cause(err) {
		case nil:
			return nil
		case sql.ErrNoRows:
			row = nil
			return nil
		default:
			return errors.Trace(err)
		}
	})
}

// GetEnvByIDOrErr get Env by ID.
// If not found, (nil, sql.ErrNoRows) returned
func GetEnvByIDOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	id int64,
) (*Env, error) {
	row := &Env{}
	err := tsq.TraceDB(ctx, func(ctx context.Context) error {
		return getEnvByIDQuery.Load(
			ctx, db, row, id,
		)
	})
	return row, errors.Trace(err)
}

// ListEnvByIDSet lists Envs by ID set.
// Not found item will be ignored
func ListEnvByIDSet(
	ctx context.Context,
	db gorp.SqlExecutor,
	ids ...int64,
) ([]*Env, error) {
	query := tsq.
		Select(TableEnv.Columns()...).
		Where(Env_ID.In(ids...)).
		MustBuild()

	var list []*Env
	return list, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		list, err = tsq.List[Env](
			ctx, db, query,
		)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
}

// ListEnvByIDSetOrErr lists Envs by ID set.
// Error returned if any item not found
func ListEnvByIDSetOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	ids ...int64,
) ([]*Env, error) {
	idSet := map[int64]bool{}
	for _, i := range ids {
		idSet[i] = true
	}
	query := tsq.
		Select(TableEnv.Columns()...).
		Where(Env_ID.In(ids...)).
		MustBuild()
	var list []*Env
	return list, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		list, err = tsq.List[Env](
			ctx, db, query,
		)
		if err != nil {
			return errors.Trace(err)
		}

		for _, i := range list {
			delete(idSet, i.ID)
		}
		if len(idSet) > 0 {
			var missings []int64
			for i := range idSet {
				missings = append(missings, i)
			}
			return errors.Errorf("Env(s) not found: %v", missings)
		}
		return nil
	})
}

////////////////////////////// Query acitve by ID //////////////////////////////

var getActiveEnvByIDQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_DT.EQ(0),
		Env_ID.EQVar(),
	).
	MustBuild()

// GetActiveEnvByID get **Active** Env by ID.
// If not found, (nil, nil) returned.
func GetActiveEnvByID(
	ctx context.Context,
	db gorp.SqlExecutor,
	id int64,
) (*Env, error) {
	row := &Env{}
	return row, tsq.TraceDB(ctx, func(ctx context.Context) error {
		err := getActiveEnvByIDQuery.Load(
			ctx, db, row, id,
		)
		switch errors.Cause(err) {
		case nil:
			return nil
		case sql.ErrNoRows:
			row = nil
			return nil
		default:
			return errors.Trace(err)
		}
	})
}

// GetActiveEnvByIDOrErr get **Active** Env by ID.
// If not found, (nil, nil) returned.
func GetActiveEnvByIDOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	id int64,
) (*Env, error) {
	row := &Env{}
	err := tsq.TraceDB(ctx, func(ctx context.Context) error {
		return getActiveEnvByIDQuery.Load(
			ctx, db, row, id,
		)
	})
	return row, errors.Trace(err)
}

// ListActiveEnvByIDSet lists **Active** Envs by ID set.
// Not found item will be ignored.
func ListActiveEnvByIDSet(
	ctx context.Context,
	db gorp.SqlExecutor,
	ids ...int64,
) ([]*Env, error) {
	query := tsq.
		Select(TableEnv.Columns()...).
		Where(
			Env_DT.EQ(0),
			Env_ID.In(ids...),
		).
		MustBuild()
	var list []*Env
	err := tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		list, err = tsq.List[Env](
			ctx, db, query,
		)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	return list, errors.Trace(err)
}

// ListActiveEnvByIDSetOrErr lists **Active** Envs by ID set.
// Error returned if any item not found
func ListActiveEnvByIDSetOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	ids ...int64,
) ([]*Env, error) {
	idSet := map[int64]bool{}
	for _, i := range ids {
		idSet[i] = true
	}
	query := tsq.
		Select(TableEnv.Columns()...).
		Where(
			Env_DT.EQ(0),
			Env_ID.In(ids...),
		).
		MustBuild()

	var list []*Env
	return list, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		list, err = tsq.List[Env](
			ctx, db, query,
		)
		if err != nil {
			return errors.Trace(err)
		}
		for _, i := range list {
			delete(idSet, i.ID)
		}
		if len(idSet) > 0 {
			var missings []int64
			for i := range idSet {
				missings = append(missings, i)
			}
			return errors.Errorf("Env(s) not found: %v", missings)
		}
		return nil
	})
}

////////////////////////////// CRUD ////////////////////////////////////////////

// Insert Env to db.
func (r *Env) Insert(
	ctx context.Context,
	db gorp.SqlExecutor,
	preAndPostHook ...func(r *Env) error,
) error {
	return tsq.TraceDB(ctx, func(ctx context.Context) error {
		if len(preAndPostHook) > 0 {
			if err := preAndPostHook[0](r); err != nil {
				return errors.Annotatef(
					err,
					"pre hook error",
				)
			}
		}
		r.CT = time.Now()
		r.ModifiedTime = time.Now()
		err := db.Insert(r)
		if err != nil {
			return errors.Annotatef(err, "%+v", r)
		}

		if len(preAndPostHook) > 1 {
			if err := preAndPostHook[1](r); err != nil {
				return errors.Annotatef(
					err,
					"post hook error",
				)
			}
		}

		return nil
	})
}

// Update updates row.
func (r *Env) Update(
	ctx context.Context,
	db gorp.SqlExecutor,
	preAndPostHook ...func(r *Env) error,
) error {
	return tsq.TraceDB(ctx, func(ctx context.Context) error {
		if len(preAndPostHook) > 0 {
			if err := preAndPostHook[0](r); err != nil {
				return errors.Annotatef(
					err,
					"pre hook error",
				)
			}
		}
		r.ModifiedTime = time.Now()
		_, err := db.Update(r)
		if err != nil {
			return errors.Annotatef(err, "%+v", r)
		}

		if len(preAndPostHook) > 1 {
			if err := preAndPostHook[1](r); err != nil {
				return errors.Annotatef(
					err,
					"post hook error",
				)
			}
		}

		return nil
	})
}

// Delete Env from db.
func (r *Env) Delete(
	ctx context.Context,
	db gorp.SqlExecutor,
	preAndPostHook ...func(r *Env) error,
) error {
	return tsq.TraceDB(ctx, func(ctx context.Context) error {
		if len(preAndPostHook) > 0 {
			if err := preAndPostHook[0](r); err != nil {
				return errors.Annotatef(
					err,
					"pre hook error",
				)
			}
		}

		_, err := db.Delete(r)
		if err != nil {
			return errors.Annotatef(err, "%+v", r)
		}

		if len(preAndPostHook) > 1 {
			if err := preAndPostHook[1](r); err != nil {
				return errors.Annotatef(
					err,
					"post hook error",
				)
			}
		}

		return nil
	})
}

// SoftDelete soft deletes Env.
// dt(unix nano timestamp) set delete timestamp manually if it was greater than 0.
func (r *Env) SoftDelete(
	ctx context.Context,
	db gorp.SqlExecutor,
	dt int64,
	preAndPostHook ...func(r *Env) error,
) error {
	return tsq.TraceDB(ctx, func(ctx context.Context) error {
		if len(preAndPostHook) > 0 {
			if err := preAndPostHook[0](r); err != nil {
				return errors.Annotatef(
					err,
					"pre hook error",
				)
			}
		}

		if dt > 0 {
			r.DT = dt
		} else {
			r.DT = time.Now().UnixNano()
		}
		r.ModifiedTime = time.Now()
		_, err := db.Update(r)
		if err != nil {
			return errors.Annotatef(err, "%+v", r)
		}

		if len(preAndPostHook) > 1 {
			if err := preAndPostHook[1](r); err != nil {
				return errors.Annotatef(
					err,
					"post hook error",
				)
			}
		}

		return nil
	})
}

// ListEnvByQuery list Env by query.
func ListEnvByQuery(
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *tsq.Query,
	args ...any,
) ([]*Env, error) {
	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](ctx, tx, qb, args...)
		return errors.Trace(err)
	})
}

// PageEnvByQuery page lists Env by query.
func PageEnvByQuery(
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *tsq.PageReq,
	qb *tsq.Query,
	args ...any,
) (*tsq.PageResp[Env], error) {
	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = tsq.Page[Env](
			ctx, tx, page, qb, args...,
		)
		return errors.Trace(err)
	})
}

////////////////////////////// List all ////////////////////////////////////////

// listEnvQuery queries Env.
var listEnvQuery = tsq.
	Select(TableEnv.Columns()...).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountEnv counts Env.
func CountEnv(
	ctx context.Context,
	tx gorp.SqlExecutor,
) (int, error) {
	query := listEnvQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(ctx, tx)
		return errors.Trace(err)
	})
}

// ListEnv lists []*Env.
func ListEnv(
	ctx context.Context,
	tx gorp.SqlExecutor,
) ([]*Env, error) {
	query := listEnvQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](ctx, tx, query)
		return errors.Trace(err)
	})
}

// PageEnv page lists Env.
func PageEnv(
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *tsq.PageReq,
) (*tsq.PageResp[Env], error) {
	query := listEnvQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = tsq.Page[Env](
			ctx, tx, page, query,
		)
		return errors.Trace(err)
	})
}

////////////////////////////// List actives ////////////////////////////////////

// listActiveEnvQuery queries *Active* Env.
var listActiveEnvQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(Env_DT.EQ(0)).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountActiveEnv counts *Active* Env.
// removed records not included.
func CountActiveEnv(
	ctx context.Context,
	tx gorp.SqlExecutor,
) (int, error) {
	query := listActiveEnvQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(ctx, tx)
		return errors.Trace(err)
	})
}

// ListActiveEnv lists *Active* Env.
// removed records not included.
func ListActiveEnv(
	ctx context.Context,
	tx gorp.SqlExecutor,
) ([]*Env, error) {
	query := listActiveEnvQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](ctx, tx, query)
		return errors.Trace(err)
	})
}

// PageActiveEnv page lists *Active* Env.
// removed records not included.
func PageActiveEnv(
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *tsq.PageReq,
) (*tsq.PageResp[Env], error) {
	query := listActiveEnvQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = tsq.Page[Env](
			ctx, tx, page, query,
		)
		return errors.Trace(err)
	})
}

////////////////////////////// Query by UK /////////////////////////////////////

var getEnvByAppIDAndEnvCodeQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_AppID.EQVar(),
		Env_EnvCode.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// GetEnvByAppIDAndEnvCode gets Env by unique index AppIDAndEnvCode.
// If not found, (nil, nil) returned.
func GetEnvByAppIDAndEnvCode(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envCode string,
) (*Env, error) {
	query := getEnvByAppIDAndEnvCodeQuery

	row := &Env{}
	return row, tsq.TraceDB(ctx, func(ctx context.Context) error {
		err := query.Load(
			ctx,
			db,
			row,
			appID,
			envCode,
		)
		switch errors.Cause(err) {
		case nil:
			return nil
		case sql.ErrNoRows:
			row = nil
			return nil
		default:
			return errors.Trace(err)
		}
	})
}

// GetEnvByAppIDAndEnvCodeOrErr gets Env by unique index AppIDAndEnvCode.
// If not found, (nil, sql.ErrNoRows) returned
func GetEnvByAppIDAndEnvCodeOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envCode string,
) (*Env, error) {
	query := getEnvByAppIDAndEnvCodeQuery

	row := &Env{}
	return row, tsq.TraceDB(ctx, func(ctx context.Context) error {
		return query.Load(
			ctx,
			db,
			row,
			appID,
			envCode,
		)
	})
}

// ExistsEnvByAppIDAndEnvCode checks if Env exists by unique index AppIDAndEnvCode.
func ExistsEnvByAppIDAndEnvCode(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envCode string,
) (bool, error) {
	query := getEnvByAppIDAndEnvCodeQuery

	var rs bool
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Exists(
			ctx,
			db,
			appID,
			envCode,
		)
		return errors.Trace(err)
	})
}

////////////////////////////// Query active by UK //////////////////////////////

// getActiveEnvByAppIDAndEnvCodeQuery queries *Active* Env by unique index AppIDAndEnvCode.
var getActiveEnvByAppIDAndEnvCodeQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_DT.EQ(0),
		Env_AppID.EQVar(),
		Env_EnvCode.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// GetActiveEnvByAppIDAndEnvCode gets *Active* Env by unique index AppIDAndEnvCode.
// *removed record not included*.
// If not found, (nil, nil) returned.
func GetActiveEnvByAppIDAndEnvCode(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envCode string,
) (*Env, error) {
	query := getActiveEnvByAppIDAndEnvCodeQuery

	row := &Env{}
	return row, tsq.TraceDB(ctx, func(ctx context.Context) error {
		err := query.Load(
			ctx,
			db,
			row,
			appID,
			envCode,
		)
		switch errors.Cause(err) {
		case nil:
			return nil
		case sql.ErrNoRows:
			row = nil
			return nil
		default:
			return errors.Trace(err)
		}
	})
}

// GetActiveEnvByAppIDAndEnvCodeOrErr gets *Active* Env by unique index AppIDAndEnvCode.
// *removed record not included*.
// If not found, (nil, sql.ErrNoRows) returned
func GetActiveEnvByAppIDAndEnvCodeOrErr(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envCode string,
) (*Env, error) {
	query := getActiveEnvByAppIDAndEnvCodeQuery

	row := &Env{}
	return row, tsq.TraceDB(ctx, func(ctx context.Context) error {
		return query.Load(
			ctx,
			db,
			row,
			appID,
			envCode,
		)
	})
}

// ExistsActiveEnvByAppIDAndEnvCode checks if *Active* Env exists by unique index AppIDAndEnvCode.
// *removed record not included*.
func ExistsActiveEnvByAppIDAndEnvCode(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envCode string,
) (bool, error) {
	query := getActiveEnvByAppIDAndEnvCodeQuery

	var rs bool
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Exists(
			ctx,
			db,
			appID,
			envCode,
		)
		return errors.Trace(err)
	})
}

////////////////////////////// Query all by IDX ////////////////////////////////

// ListEnvByAppIDQuery queries Env by index AppID.
var ListEnvByAppIDQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_AppID.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountEnvByAppID counts Env by index AppID.
func CountEnvByAppID(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
) (int, error) {
	query := ListEnvByAppIDQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(
			ctx,
			db,
			appID,
		)
		return errors.Trace(err)
	})
}

// ListEnvByAppID lists Env by index AppID.
func ListEnvByAppID(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
) ([]*Env, error) {
	query := ListEnvByAppIDQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](
			ctx,
			db,
			query,
			appID,
		)
		return errors.Trace(err)
	})
}

// PageEnvByAppID page lists Env by index AppID.
func PageEnvByAppID(
	ctx context.Context,
	db gorp.SqlExecutor,
	page *tsq.PageReq,
	appID int64,
) (*tsq.PageResp[Env], error) {
	query := ListEnvByAppIDQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = tsq.Page[Env](
			ctx,
			db,
			page,
			query,
			appID,
		)
		return errors.Trace(err)
	})
}

// ListEnvByAppIDAndEnvLevelQuery queries Env by index AppIDAndEnvLevel.
var ListEnvByAppIDAndEnvLevelQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_AppID.EQVar(),
		Env_EnvLevel.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountEnvByAppIDAndEnvLevel counts Env by index AppIDAndEnvLevel.
func CountEnvByAppIDAndEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envLevel EnvLevel,
) (int, error) {
	query := ListEnvByAppIDAndEnvLevelQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(
			ctx,
			db,
			appID,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// ListEnvByAppIDAndEnvLevel lists Env by index AppIDAndEnvLevel.
func ListEnvByAppIDAndEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envLevel EnvLevel,
) ([]*Env, error) {
	query := ListEnvByAppIDAndEnvLevelQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](
			ctx,
			db,
			query,
			appID,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// PageEnvByAppIDAndEnvLevel page lists Env by index AppIDAndEnvLevel.
func PageEnvByAppIDAndEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	page *tsq.PageReq,
	appID int64,
	envLevel EnvLevel,
) (*tsq.PageResp[Env], error) {
	query := ListEnvByAppIDAndEnvLevelQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = tsq.Page[Env](
			ctx,
			db,
			page,
			query,
			appID,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// ListEnvByEnvLevelQuery queries Env by index EnvLevel.
var ListEnvByEnvLevelQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_EnvLevel.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountEnvByEnvLevel counts Env by index EnvLevel.
func CountEnvByEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	envLevel EnvLevel,
) (int, error) {
	query := ListEnvByEnvLevelQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(
			ctx,
			db,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// ListEnvByEnvLevel lists Env by index EnvLevel.
func ListEnvByEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	envLevel EnvLevel,
) ([]*Env, error) {
	query := ListEnvByEnvLevelQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](
			ctx,
			db,
			query,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// PageEnvByEnvLevel page lists Env by index EnvLevel.
func PageEnvByEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	page *tsq.PageReq,
	envLevel EnvLevel,
) (*tsq.PageResp[Env], error) {
	query := ListEnvByEnvLevelQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = tsq.Page[Env](
			ctx,
			db,
			page,
			query,
			envLevel,
		)
		return errors.Trace(err)
	})
}

////////////////////////////// Query actives by IDX ////////////////////////////

// listActiveEnvByAppIDQuery queries *Active* Env by index AppID.
// *removed record are not included*.
var listActiveEnvByAppIDQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_DT.EQ(0),
		Env_AppID.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountActiveEnvByAppID counts *Active* Env by index AppID.
// *removed record are not included*.
func CountActiveEnvByAppID(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
) (int, error) {
	query := listActiveEnvByAppIDQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(
			ctx,
			db,
			appID,
		)
		return errors.Trace(err)
	})
}

// ListActiveEnvByAppID lists *Active* Env by index AppID.
// *removed record are not included*.
func ListActiveEnvByAppID(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
) ([]*Env, error) {
	query := listActiveEnvByAppIDQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](
			ctx,
			db,
			query,
			appID,
		)
		return errors.Trace(err)
	})
}

// PageActiveEnvByAppID page lists *Active* Env by index AppID.
// *removed records are not included*.
func PageActiveEnvByAppID(
	ctx context.Context,
	db gorp.SqlExecutor,
	page *tsq.PageReq,
	appID int64,
) (*tsq.PageResp[Env], error) {
	query := listActiveEnvByAppIDQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		//rs, err = PageEnvByQuery(
		rs, err = tsq.Page[Env](
			ctx,
			db,
			page,
			query,
			appID,
		)
		return errors.Trace(err)
	})
}

// listActiveEnvByAppIDAndEnvLevelQuery queries *Active* Env by index AppIDAndEnvLevel.
// *removed record are not included*.
var listActiveEnvByAppIDAndEnvLevelQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_DT.EQ(0),
		Env_AppID.EQVar(),
		Env_EnvLevel.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountActiveEnvByAppIDAndEnvLevel counts *Active* Env by index AppIDAndEnvLevel.
// *removed record are not included*.
func CountActiveEnvByAppIDAndEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envLevel EnvLevel,
) (int, error) {
	query := listActiveEnvByAppIDAndEnvLevelQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(
			ctx,
			db,
			appID,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// ListActiveEnvByAppIDAndEnvLevel lists *Active* Env by index AppIDAndEnvLevel.
// *removed record are not included*.
func ListActiveEnvByAppIDAndEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	appID int64,
	envLevel EnvLevel,
) ([]*Env, error) {
	query := listActiveEnvByAppIDAndEnvLevelQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](
			ctx,
			db,
			query,
			appID,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// PageActiveEnvByAppIDAndEnvLevel page lists *Active* Env by index AppIDAndEnvLevel.
// *removed records are not included*.
func PageActiveEnvByAppIDAndEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	page *tsq.PageReq,
	appID int64,
	envLevel EnvLevel,
) (*tsq.PageResp[Env], error) {
	query := listActiveEnvByAppIDAndEnvLevelQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		//rs, err = PageEnvByQuery(
		rs, err = tsq.Page[Env](
			ctx,
			db,
			page,
			query,
			appID,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// listActiveEnvByEnvLevelQuery queries *Active* Env by index EnvLevel.
// *removed record are not included*.
var listActiveEnvByEnvLevelQuery = tsq.
	Select(TableEnv.Columns()...).
	Where(
		Env_DT.EQ(0),
		Env_EnvLevel.EQVar(),
	).
	KwSearch(TableEnv.KwList()...).
	MustBuild()

// CountActiveEnvByEnvLevel counts *Active* Env by index EnvLevel.
// *removed record are not included*.
func CountActiveEnvByEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	envLevel EnvLevel,
) (int, error) {
	query := listActiveEnvByEnvLevelQuery

	var rs int
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		rs, err = query.Count(
			ctx,
			db,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// ListActiveEnvByEnvLevel lists *Active* Env by index EnvLevel.
// *removed record are not included*.
func ListActiveEnvByEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	envLevel EnvLevel,
) ([]*Env, error) {
	query := listActiveEnvByEnvLevelQuery

	var data []*Env
	return data, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		data, err = tsq.List[Env](
			ctx,
			db,
			query,
			envLevel,
		)
		return errors.Trace(err)
	})
}

// PageActiveEnvByEnvLevel page lists *Active* Env by index EnvLevel.
// *removed records are not included*.
func PageActiveEnvByEnvLevel(
	ctx context.Context,
	db gorp.SqlExecutor,
	page *tsq.PageReq,
	envLevel EnvLevel,
) (*tsq.PageResp[Env], error) {
	query := listActiveEnvByEnvLevelQuery

	var rs *tsq.PageResp[Env]
	return rs, tsq.TraceDB(ctx, func(ctx context.Context) error {
		var err error
		//rs, err = PageEnvByQuery(
		rs, err = tsq.Page[Env](
			ctx,
			db,
			page,
			query,
			envLevel,
		)
		return errors.Trace(err)
	})
}
