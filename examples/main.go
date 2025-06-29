package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"log/slog"

	"github.com/juju/errors"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/examples/database"
	"gopkg.in/gorp.v2"
)

func main() {
	// 1. 连接 SQLite 内存数据库
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		slog.Info("数据库连接失败", "err", err, "stack", errors.ErrorStack(err))
		os.Exit(1)
	}
	defer func() {
		_ = db.Close()
	}()

	// 2. 初始化 gorp
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}

	// err = tsq.Init(dbmap, true, true, tsq.PrintCost, tsq.PrintError, tsq.PrintSQL)
	err = tsq.Init(dbmap, true, true, tsq.PrintError, tsq.PrintSQL)
	if err != nil {
		slog.Info("tsq.Init 失败", "err", err, "stack", errors.ErrorStack(err))
		os.Exit(1)
	}

	// 初始化数据库，执行 mock.sql 文件
	mockSQL, err := os.ReadFile("examples/database/mock.sql")
	if err != nil {
		slog.Info("读取 mock.sql 失败", "err", err, "stack", errors.ErrorStack(err))
		os.Exit(1)
	}
	_, err = db.Exec(string(mockSQL))
	if err != nil {
		slog.Info("执行 mock.sql 失败", "err", err, "stack", errors.ErrorStack(err))
		os.Exit(1)
	}

	// 3. 构造分页参数
	pageReq := &tsq.PageReq{
		Page:    1,
		Size:    10,
		Order:   "asc,desc",
		OrderBy: "user_id,order_id",
	}

	// 4. 调用 PageUserOrder，假设 user_id = 1
	ctx := context.Background()
	resp, err := database.PageUserOrder(ctx, dbmap, pageReq, 1, "图书", "视频", `杂fds""了''志`)
	if err != nil {
		slog.Info("PageUserOrder 失败", "err", err, "stack", errors.ErrorStack(err))
		os.Exit(1)
	}

	// 5. 打印结果
	rs, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println("-------------", string(rs))

	// 6. 运行批量插入演示
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("运行批量插入功能演示...")
	fmt.Println(strings.Repeat("=", 50))
	runBatchInsertDemo(ctx, dbmap)

	// RunAllExamples(ctx, dbmap)
}

// runBatchInsertDemo 运行批量插入演示
func runBatchInsertDemo(ctx context.Context, dbmap *gorp.DbMap) {
	slog.Info("=== 批量插入功能演示 ===")

	// 演示1：基本批量插入
	slog.Info("1. 基本批量插入用户数据")
	users := createTestUsers(100)

	err := tsq.BatchInsert(ctx, dbmap, users)
	if err != nil {
		slog.Error("批量插入用户失败", "err", err)
		return
	}
	slog.Info("成功插入用户", "count", len(users))

	// 验证插入结果
	count, err := dbmap.SelectInt("SELECT COUNT(*) FROM user")
	if err != nil {
		slog.Error("查询用户数量失败", "err", err)
		return
	}
	slog.Info("数据库中用户总数", "count", count)

	// 演示2：带选项的批量插入（忽略重复）
	slog.Info("2. 带选项的批量插入（忽略重复）")
	duplicateUsers := createTestUsers(50) // 创建一些重复的用户

	options := &tsq.BatchInsertOptions{
		BatchSize:    10,
		IgnoreErrors: true, // 忽略重复键错误
	}

	err = tsq.BatchInsert(ctx, dbmap, duplicateUsers, options)
	if err != nil {
		slog.Error("批量插入重复用户失败", "err", err)
		return
	}
	slog.Info("尝试插入用户", "count", len(duplicateUsers))

	// 验证插入结果
	count, err = dbmap.SelectInt("SELECT COUNT(*) FROM user")
	if err != nil {
		slog.Error("查询用户数量失败", "err", err)
		return
	}
	slog.Info("数据库中用户总数", "count", count)

	slog.Info("=== 批量插入演示完成 ===")
}

// createTestUsers 创建测试用户数据
func createTestUsers(count int) []*database.User {
	users := make([]*database.User, count)

	for i := 0; i < count; i++ {
		users[i] = &database.User{
			// ID 字段不设置，让数据库自动生成
			Name:  fmt.Sprintf("demo_user_%d", i+1),
			Email: fmt.Sprintf("demo_user_%d@example.com", i+1),
			OrgID: int64(i%10 + 1), // 分配到不同的组织
			// CT 字段也不设置，让数据库自动生成
		}
	}

	return users
}
