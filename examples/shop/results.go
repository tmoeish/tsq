package shop

import "database/sql"

// ProductListing 是一个结果投影（result）：不是表，而是一次查询的返回形状。
// 每个字段用 tsq:"表.字段" 指明它来自哪张表的哪一列；tsq gen 生成 ResultProductListing，
// 查询时 tsq.Select(ResultProductListing.Columns()...) 就把各列读进这个结构体。
//
//tsq:result
type ProductListing struct {
	ProductID    int64  `json:"product_id"    tsq:"Product.ID"`
	SKU          string `json:"sku"           tsq:"Product.SKU"`
	ProductName  string `json:"product_name"  tsq:"Product.Name"`
	PriceCents   int64  `json:"price_cents"   tsq:"Product.PriceCents"`
	CategoryName string `json:"category_name" tsq:"Category.Name"`
}

// CustomerOrder 是"顾客左连接订单"的一行。没下过单的顾客也会出现，
// 这时订单那一侧全是 NULL，所以这两个字段必须是可空类型；
// 用 int64 的话，查询在执行前就会报错，而不是扫描到 NULL 时才失败。
//
//tsq:result
type CustomerOrder struct {
	CustomerName string          `json:"customer_name" tsq:"Customer.Name"`
	OrderID      sql.Null[int64] `json:"order_id"      tsq:"Order.ID"`
	TotalCents   sql.Null[int64] `json:"total_cents"   tsq:"Order.TotalCents"`
}
