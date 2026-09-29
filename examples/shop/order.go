package shop

import "time"

// Order 是订单。表名用复数 orders，避开 SQL 关键字 ORDER。
//
//tsq:table name=orders
//tsq:managed version created_at updated_at
//tsq:index CustomerID,Status
type Order struct {
	ID         int64       `db:"id"             json:"id"`
	CustomerID int64       `db:"customer_id"    json:"customer_id"`
	Status     OrderStatus `db:"status,size:16" json:"status"`
	TotalCents int64       `db:"total_cents"    json:"total_cents"`
	Note       *string     `db:"note,size:255"  json:"note"`

	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
	Version   int64     `db:"version"    json:"version"`
}

// OrderStatus 是订单状态。
type OrderStatus string

const (
	OrderPending   OrderStatus = "pending"
	OrderPaid      OrderStatus = "paid"
	OrderShipped   OrderStatus = "shipped"
	OrderCancelled OrderStatus = "cancelled"
)

// OrderItem 是订单里的一行商品。
//
//   - unique OrderID,ProductID：同一订单里同一商品只占一行；
//     生成 GetByOrderIDAndProductID 等复合键查找方法。
//   - LineTotalCents 是生成列：数据库按表达式计算，TSQ 从不写它，单行 Insert 后读回。
//
//tsq:table name=order_items
//tsq:unique OrderID,ProductID
//tsq:index ProductID
type OrderItem struct {
	ID             int64 `db:"id"                                                     json:"id"`
	OrderID        int64 `db:"order_id"                                               json:"order_id"`
	ProductID      int64 `db:"product_id"                                             json:"product_id"`
	Quantity       int64 `db:"quantity"                                               json:"quantity"`
	UnitPriceCents int64 `db:"unit_price_cents"                                       json:"unit_price_cents"`
	LineTotalCents int64 `db:"line_total_cents,generated:quantity * unit_price_cents" json:"line_total_cents"`
}
