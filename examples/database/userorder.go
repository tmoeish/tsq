package database

import (
	"context"
	"time"

	"github.com/tmoeish/tsq"
	"gopkg.in/gorp.v2"
)

// UserOrder 用于用户和订单联合查询(aaa)
// @DTO
type UserOrder struct {
	UserID    int64  `json:"user_id"    tsq:"User.ID"`    // 用户ID
	UserName  string `json:"user_name"  tsq:"User.Name"`  // 用户名
	UserEmail string `json:"user_email" tsq:"User.Email"` // 用户邮箱

	OrgName string `json:"org_name" tsq:"Org.Name"` // 组织名称

	OrderID     int         `json:"order_id"     tsq:"Order.ID"`     // 订单ID
	OrderAmount float64     `json:"order_amount" tsq:"Order.Amount"` // 订单数量
	OrderPrice  float64     `json:"order_price"  tsq:"Order.Price"`  // 订单金额
	OrderStatus OrderStatus `json:"order_status" tsq:"Order.Status"` // 订单状态
	OrderTime   string      `json:"order_time"   tsq:"Order.CT"`     // 订单时间

	ItemID    int64  `json:"item_id"    tsq:"Item.ID"`    // 商品ID
	ItemName  string `json:"item_name"  tsq:"Item.Name"`  // 商品名称
	ItemPrice int64  `json:"item_price" tsq:"Item.Price"` // 商品价格

	ItemCategory string `json:"item_category" tsq:"Category.Name"` // 商品分类
}

func PageUserOrder(
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *tsq.PageReq,
	userID int64,
	cats ...string,
) (*tsq.PageResp[UserOrder], error) {
	t, _ := time.Parse(time.DateOnly, "2024-06-02")

	query := tsq.
		Select(DtoUserOrder.Cols()...).
		LeftJoin(User_OrgID, Org_ID).
		LeftJoin(User_ID, Order_UserID).
		LeftJoin(Order_ItemID, Item_ID).
		LeftJoin(Item_CategoryID, Category_ID).
		Where(
			UserOrder_UserID.EQVar(),
			UserOrder_ItemPrice.GESub(tsq.
				Select(Item_Price.Avg().Fn("(%s)/10")).
				Where(
					Item_CategoryID.InSub(tsq.
						Select(Category_ID).
						Where(Category_Name.In(cats...)).
						MustBuild()),
				).
				MustBuild()),
			UserOrder_OrderStatus.In(
				OrderStatusPending,
				OrderStatusPaid,
				OrderStatusShipped,
				OrderStatusCompleted,
			),
			UserOrder_ItemCategory.In(cats...),
			UserOrder_OrderTime.GTE(t),
		).
		GroupBy(
			UserOrder_ItemCategory,
		).
		KwSearch(TableOrder.KwList()...).
		MustBuild()

	return tsq.Page[UserOrder](ctx, tx, page, query, userID)
}
