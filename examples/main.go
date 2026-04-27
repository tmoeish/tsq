package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/juju/errors"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/examples/database"
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

	// 2. 初始化数据库，执行 mock.sql 文件（包含表创建和数据）
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

	// 3. 初始化 tsq（表已存在，不需要自动创建）
	dbmap := &tsq.DbMap{Db: db, Dialect: tsq.SqliteDialect{}}

	// err = tsq.Init(dbmap, false, true, tsq.PrintCost, tsq.PrintError, tsq.PrintSQL)
	err = tsq.Init(dbmap, false, true, tsq.PrintError, tsq.PrintSQL)
	if err != nil {
		slog.Info("tsq.Init 失败", "err", err, "stack", errors.ErrorStack(err))
		os.Exit(1)
	}

	// 3. 运行分块插入演示
	ctx := context.Background()
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("运行批量插入功能演示...")
	fmt.Println(strings.Repeat("=", 50))
	runChunkedInsertDemo(ctx, dbmap)

	// RunAllExamples(ctx, dbmap)
}

// runChunkedInsertDemo 运行分块插入演示
func runChunkedInsertDemo(ctx context.Context, dbmap *tsq.DbMap) {
	slog.Info("=== 分块插入功能演示 ===")

	// 演示1：基本分块插入
	slog.Info("1. 基本分块插入用户数据")
	users := createTestUsers(100)

	err := tsq.ChunkedInsert(ctx, dbmap, users)
	if err != nil {
		slog.Error("分块插入用户失败", "err", err)
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

	// 演示2：带选项的分块插入（忽略重复）
	slog.Info("2. 带选项的分块插入（忽略重复）")
	duplicateUsers := createTestUsers(50) // 创建一些重复的用户

	options := &tsq.ChunkedInsertOptions{
		ChunkSize:    10,
		IgnoreErrors: true, // 忽略重复键错误
	}

	err = tsq.ChunkedInsert(ctx, dbmap, duplicateUsers, options)
	if err != nil {
		slog.Error("分块插入重复用户失败", "err", err)
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

	slog.Info("=== 分块插入演示完成 ===")
}

// createTestUsers 创建测试用户数据
func createTestUsers(count int) []*database.User {
	users := make([]*database.User, count)

	for i := range count {
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
