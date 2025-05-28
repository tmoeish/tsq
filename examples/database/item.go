package database

import (
	"github.com/tmoeish/tsq/examples/common"
)

// Item 商品表
// @TABLE(
//
//	ux=[{fields=["Name"]}],
//	idx=[{name="IdxCategory", fields=["CategoryID"]}],
//
//	kw=["Name"]
//
// )
type Item struct {
	common.ImmutableTable

	CategoryID int64 `db:"category_id"` // 商品分类ID

	Name string `db:"name,size:200"` // 商品名称

	Price int64 `db:"price"` // 商品价格
}
