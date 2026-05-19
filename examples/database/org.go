package database

import "github.com/tmoeish/tsq/v4/examples/common"

// 组织表
// @TABLE(
//
//	created_at,
//	ux=[
//		{fields=["Name"]},
//	],
//
// )
type Org struct {
	common.ImmutableTable

	Name string `db:"name"`
}
