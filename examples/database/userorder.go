package database

import (
	"context"
	"time"

	"github.com/juju/errors"

	"github.com/tmoeish/tsq"
)

// UserOrder 演示查询结果联表映射。
// @RESULT(
//
//	name="UserOrder",
//	join=[
//	  {left="User.OrgID", right="Org.ID"},
//	  {left="User.ID", right="Order.UserID"},
//	  {left="Order.ItemID", right="Item.ID"},
//	  {left="Item.CategoryID", right="Category.ID"}
//	]
//
// )
type UserOrder struct {
	UserID    int64  `json:"user_id"    tsq:"User.ID"`
	UserName  string `json:"user_name"  tsq:"User.Name"`
	UserEmail string `json:"user_email" tsq:"User.Email"`

	OrgName string `json:"org_name" tsq:"Org.Name"`

	OrderID     int64       `json:"order_id"     tsq:"Order.UID"`
	OrderAmount int64       `json:"order_amount" tsq:"Order.Amount"`
	OrderPrice  int64       `json:"order_price"  tsq:"Order.Price"`
	OrderStatus OrderStatus `json:"order_status" tsq:"Order.Status"`
	OrderTime   time.Time   `json:"order_time"   tsq:"Order.CreatedAt"`

	ItemID       int64  `json:"item_id"       tsq:"Item.ID"`
	ItemName     string `json:"item_name"     tsq:"Item.Name"`
	ItemPrice    int64  `json:"item_price"    tsq:"Item.Price"`
	ItemCategory string `json:"item_category" tsq:"Category.Name"`
}

var pageUserOrderQuery *tsq.Query

func init() {
	var err error

	pageUserOrderQuery, err = UserOrderFromUser().
		LeftJoinOrg(UserOrderJoinUserOrgIDToOrgID()).
		LeftJoinOrder(UserOrderJoinUserIDToOrderUserID()).
		LeftJoinItem(UserOrderJoinOrderItemIDToItemID()).
		LeftJoinCategory(UserOrderJoinItemCategoryIDToCategoryID()).
		SelectUserOrder().
		WhereUser(User_ID.EQVar()).
		WhereCategory(Category_Name.InVar()).
		Build()
	if err != nil {
		panic(errors.Annotate(err, "initialize pageUserOrderQuery"))
	}
}

// PageUserOrder 按用户和分类分页查询 Result 结果。
func PageUserOrder(
	ctx context.Context,
	tx tsq.SqlExecutor,
	page *tsq.PageReq,
	userID int64,
	categories ...string,
) (*tsq.PageResp[UserOrder], error) {
	return tsq.Page[UserOrder](ctx, tx, page, pageUserOrderQuery, userID, categories)
}
