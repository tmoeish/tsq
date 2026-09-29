package shop

import "time"

// Customer 是顾客。
//
//tsq:table name=customers
//tsq:managed created_at
//tsq:unique Email
type Customer struct {
	ID    int64  `db:"id"             json:"id"`
	Email string `db:"email,size:128" json:"email"`
	Name  string `db:"name,size:64"   json:"name"`
	// Phone 可以为空（NULL）。
	Phone *string `db:"phone,size:32" json:"phone"`
	// Level 有数据库默认值：插入时为 nil 就交给数据库填 'regular'，单行 Insert 会把它读回来。
	// default: 只能用在能存 NULL 的字段上，因为零值（""）也是一个值。
	Level *string `db:"level,size:16,default:'regular'" json:"level"`

	CreatedAt time.Time `db:"created_at" json:"created_at"`
}
