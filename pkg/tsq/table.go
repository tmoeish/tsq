package tsq

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"gopkg.in/gorp.v2"
)

var (
	tables = make(map[string]Table)
)

func RegisterTable(table Table) {
	tables[table.Table()] = table
}

func Init(db *gorp.DbMap, autoCreateTable bool) error {
	for name, table := range tables {
		if len(table.VersionField()) > 0 {
			db.AddTableWithName(table, name).
				SetKeys(!table.CustomID(), table.IDField()).
				SetVersionCol(table.VersionField())
		} else {
			db.AddTableWithName(table, name).
				SetKeys(!table.CustomID(), table.IDField())
		}
	}

	if autoCreateTable {
		if err := db.CreateTablesIfNotExists(); err != nil {
			return errors.Trace(err)
		}

		for name, table := range tables {
			for ux, fields := range table.UxMap() {
				err := ensureIdx(db, name, true, ux, fields)
				if err != nil {
					return errors.Trace(err)
				}
			}
			for idx, fields := range table.IdxMap() {
				err := ensureIdx(db, name, false, idx, fields)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	return nil
}

func ensureIdx(
	db *gorp.DbMap,
	table string,
	unique bool,
	idx string,
	fields []string,
) error {
	ok, err := isIdxExist(db, table, idx)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return nil
	}
	if err := createIdx(db, table, unique, idx, fields); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func isIdxExist(dbMap *gorp.DbMap, table string, idx string) (bool, error) {
	n, err := dbMap.SelectInt(`
SELECT COUNT(1) 
FROM INFORMATION_SCHEMA.STATISTICS
WHERE 
	table_schema=DATABASE() 
	AND table_name=? 
	AND index_name=?
	`,
		table, idx,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return n > 0, nil
}

func createIdx(
	dbMap *gorp.DbMap,
	table string,
	unique bool,
	idx string,
	fields []string,
) error {
	qarr := make([]string, len(fields))
	for k, v := range fields {
		qarr[k] = fmt.Sprintf("`%s`", v)
	}
	var uk string
	if unique {
		uk = " UNIQUE "
	}
	query := fmt.Sprintf(
		"ALTER TABLE `%s` ADD %s INDEX %s(%s)",
		table, uk, idx, strings.Join(qarr, ","),
	)
	if _, err := dbMap.Exec(query); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type Table interface {
	Table() string

	CustomID() bool
	IDField() string
	VersionField() string

	Columns() []IColumn
	KwList() []IColumn

	UxMap() map[string][]string
	IdxMap() map[string][]string
}
