// Package todo 是第 1 章的模型：只有一张表，用来走一遍"写结构体 → tsq gen → 使用"。
package todo

import "time"

// 在这个目录里跑 `go generate` 就会重新生成 todo.tsq.go 等文件。
// 你的项目里通常写成 `//go:generate tsq gen .`（先 go install 装好 tsq 命令）。
//go:generate go run github.com/tmoeish/tsq/v5/cmd/tsq gen .

// Todo 是一条待办事项。
//
// 结构体上方的 //tsq: 行是 TSQ 的注解，写法和 //go: 指令一样（双斜杠后不留空格）：
//   - //tsq:table 声明这是一张表。name= 是表名；主键默认是 ID 字段，默认自增。
//   - //tsq:managed created_at 让 Insert 自动填创建时间。
//
// 字段的 db 标签给出列名，逗号后是列选项：size:200 让 DDL 写成 VARCHAR(200)。
//
//tsq:table name=todos
//tsq:managed created_at
type Todo struct {
	ID        int64     `db:"id"             json:"id"`
	Title     string    `db:"title,size:200" json:"title"`
	Done      bool      `db:"done"           json:"done"`
	CreatedAt time.Time `db:"created_at"     json:"created_at"`
}
