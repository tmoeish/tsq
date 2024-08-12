package tsq

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/gorp.v2"
)

var (
	dbMap  *gorp.DbMap
	tables = make(map[string]Table)
)

func Init(db *gorp.DbMap, autoCreateTable bool) error {
	dbMap = db
	for name, table := range tables {
		db.AddTableWithName(table, name).
			SetKeys(!table.CustomID(), table.IDField()).
			SetVersionCol(table.VersionField())
	}

	if autoCreateTable {
		if err := db.CreateTablesIfNotExists(); err != nil {
			return errors.Trace(err)
		}
		for name, table := range tables {
			for ux, fields := range table.UxMap() {
				if err := EnsureIndexExists(name, true, ux, fields); err != nil {
					logrus.Fatalln(name, ux, errors.ErrorStack(err))
					return errors.Trace(err)
				}
			}
			for idx, fields := range table.IdxMap() {
				if err := EnsureIndexExists(name, false, idx, fields); err != nil {
					logrus.Fatalln(name, idx, errors.ErrorStack(err))
					return errors.Trace(err)
				}
			}
		}
	}

	return nil
}

func EnsureIndexExists(table string, unique bool, idx string, fields []string) error {
	ok, err := ExistsIndex(table, idx)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return nil
	}
	if err := CreateIndex(table, unique, idx, fields); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func ExistsIndex(table string, idx string) (bool, error) {
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

func CreateIndex(table string, unique bool, idx string, fields []string) error {
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

func AddTable(name string, table Table) {
	tables[name] = table
}

func GetDB() *gorp.DbMap {
	return dbMap
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
