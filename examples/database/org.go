package database

import "github.com/tmoeish/tsq/examples/common"

// 组织表
// @TABLE(
//   ux=[{name="UxName", fields=["Name"]}]
// )

type Org struct {
	common.ImmutableTable

	Name string `db:"name"`
}
