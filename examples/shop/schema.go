package shop

import "embed"

// Schema 装着 tsq gen 生成的三份 DDL。每份先是完整的建表语句，之后每次 tsq gen
// 改动了表结构，就追加一段以 "-- Migration:" 开头的迁移（这个包还没有改过结构，所以文件里只有建表）。
// 第 10 章演示新库怎么执行建表那一段。
//
//go:embed sqlite.sql mysql.sql postgres.sql
var Schema embed.FS
