package academy

// Track groups courses into a learning path.
// @TABLE(
//
//	name="track",
//	pk="ID",
//	created_at,
//	ux=[
//		{fields=["Name"]},
//	],
//	kw=["Name", "Description"],
//
// )
type Track struct {
	ImmutableTable

	// Name 是学习路径名称。
	Name string `db:"name,size:120" json:"name"`
	// Description 是学习路径的介绍说明。
	Description string `db:"description,size:1024" json:"description"`
}
