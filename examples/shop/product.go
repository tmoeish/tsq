package shop

import "time"

// Product 是在售商品，也是功能最全的一张表。
//
//   - managed：TSQ 替你维护的四个字段。created_at / updated_at 自动打时间戳；
//     deleted_at 让 Delete 变成软删除；version 让 Update 带乐观锁。
//   - unique SKU：生成 GetBySKU / FindBySKU / FetchBySKU。
//   - index CategoryID：普通索引，只影响建表，不生成方法。
//   - search：TableProduct.Query() 用 tsq.Keyword(词) 搜这两列（LIKE 子串匹配）。
//   - fulltext：全文索引，用 tsq.Matches(TableProduct.FullTextNameAndDescription(), 词) 搜。
//
//tsq:table name=products
//tsq:managed created_at updated_at deleted_at version
//tsq:unique SKU
//tsq:index CategoryID
//tsq:search Name,Description
//tsq:fulltext Name,Description
type Product struct {
	ID          int64         `db:"id"                   json:"id"`
	CategoryID  int64         `db:"category_id"          json:"category_id"`
	SKU         string        `db:"sku,size:32"          json:"sku"`
	Name        string        `db:"name,size:128"        json:"name"`
	Description string        `db:"description,size:512" json:"description"`
	PriceCents  int64         `db:"price_cents"          json:"price_cents"`
	Stock       int64         `db:"stock"                json:"stock"`
	Status      ProductStatus `db:"status,size:16"       json:"status"`

	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
	// DeletedAt 为 0 表示未删除；软删除时写入删除时间的 Unix 纳秒数。
	DeletedAt int64 `db:"deleted_at" json:"deleted_at"`
	Version   int64 `db:"version"    json:"version"`
}

// ProductStatus 是基于 string 的命名类型；TSQ 按底层类型推导列类型，
// 比较时要求右值也是 ProductStatus，传错类型编译不过。
type ProductStatus string

const (
	ProductOnSale  ProductStatus = "on_sale"
	ProductOffSale ProductStatus = "off_sale"
)
