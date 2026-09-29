package shop

// Category 是商品分类。分类可以嵌套：ParentID 指向上级分类，顶级分类为 nil。
//
// 注解逐行说明：
//   - table：这是一张表，表名 categories，主键默认是 ID 字段，自增。
//   - unique：分类名唯一；同时生成 TableCategory.GetByName / FindByName / FetchByName。
//
//tsq:table name=categories
//tsq:unique Name
type Category struct {
	ID int64 `db:"id" json:"id"`
	// Name 的 size 决定 DDL 里的 VARCHAR 长度。
	Name string `db:"name,size:64" json:"name"`
	// ParentID 是指针，所以列可以为 NULL，生成的列类型是 tsq.NullColumn。
	ParentID *int64 `db:"parent_id" json:"parent_id"`
}
