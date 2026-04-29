package database

import "github.com/tmoeish/tsq/examples/common"

// 用户表
// @TABLE(
//
//	created_at,
//	ux=[
//		{fields=["Name"]},
//	],
//	kw=["Name", "Email"],
//
// )
type User struct {
	common.ImmutableTable

	OrgID int64 `db:"org_id" json:"org_id"`

	Name  string `db:"name"  json:"name"`
	Email string `db:"email" json:"email"`
}
