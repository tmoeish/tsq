package database

import (
	"database/sql/driver"

	"github.com/tmoeish/tsq/examples/common"
)

// 订单表
// @TABLE(
//   idx=[{name="IdxUserItem", fields=["UserID","ItemID"]}, {name="IdxItem", fields=["ItemID"]}],
//   ct
// )

type Order struct {
	common.MutableTable

	UserID int64 `db:"user_id"`
	ItemID int64 `db:"item_id"`

	Amount int64 `db:"amount"`
	Price  int64 `db:"price"`

	Status OrderStatus `db:"status"`
}

// OrderStatus 订单状态
type OrderStatus int

const (
	OrderStatusPending OrderStatus = iota
	OrderStatusPaid
	OrderStatusShipped
	OrderStatusCompleted
	OrderStatusCancelled
)

var _ driver.Valuer = OrderStatus(0)

func (s OrderStatus) Value() (driver.Value, error) {
	return int64(s), nil
}
