package academy

import "encoding/json"

// Track groups courses into a learning path.
// @TABLE(
//
//	name="track",
//	pk="ID",
//	created_at,
//	ux=[
//		{fields=["Name"]},
//	],
//	search=["Name", "Description"],
//
// )
type Track struct {
	ImmutableTable

	// Name 是学习路径名称。
	Name string `db:"name,size:120" json:"name"`
	// Description 是学习路径的介绍说明。
	Description string `db:"description,size:1024" json:"description"`
	// SkillItems 演示显式 DDL type 覆盖，把结构化 JSON 原样存入数据库。
	SkillItems json.RawMessage `db:"skill_items,type:JSON" json:"skill_items"`
}
