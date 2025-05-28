package database

import (
	"database/sql/driver"

	"github.com/tmoeish/tsq/examples/common"
)

// Category 分类表
// @TABLE(
//   name="category",
//   pk="ID",
//   ux=[{fields=["Name"]}],
//   kw=["Name","Description"]
// )

type Category struct {
	common.ImmutableTable
	CategoryContent
}

type CategoryContent struct {
	Type CategoryType `db:"type" json:"type"`
	// 分类名
	Name string `db:"name,size:200" json:"name"`
	// 分类描述
	Description string `db:"description,size:4096" json:"description"`
}

type CategoryType int

const (
	CategoryTypeArticle CategoryType = iota
	CategoryTypeVideo
)

var _ driver.Valuer = CategoryType(0)

func (c CategoryType) Value() (driver.Value, error) {
	return int64(c), nil
}
