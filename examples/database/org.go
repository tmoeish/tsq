package database

import "github.com/tmoeish/tsq/examples/common"

// 组织表
// @TABLE(
//
//	ux=[{fields=["Name"]}],
//	created_at
//
// )
type Org struct {
	common.ImmutableTable

	Name string `db:"name"`
}
