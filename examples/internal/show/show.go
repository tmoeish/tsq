// Package show 是示例的输出工具：把 TSQ 执行的 SQL 和每一步的结果打印得便于阅读。
// 它不是 TSQ 的一部分，你的项目里用不到。
package show

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"sync/atomic"
)

// selectList 匹配一段 SELECT 列表；qualified 匹配其中的一项 "表"."列"。
var (
	selectList = regexp.MustCompile(`SELECT (DISTINCT )?(.+?) FROM `)
	qualified  = regexp.MustCompile(`^"([^"]+)"\."[^"]+"$`)
)

// abbreviate 把"一张表的一长串列"缩写成 ‹表 的 N 列›，其余原样保留。
// TSQ 实际发出的 SQL 总是逐列列出，从不写 SELECT *；缩写只是为了让输出好读。
func abbreviate(sql string) string {
	return selectList.ReplaceAllStringFunc(sql, func(m string) string {
		parts := selectList.FindStringSubmatch(m)
		items := strings.Split(parts[2], ", ")

		if len(items) < 6 {
			return m
		}

		table := ""

		for _, item := range items {
			q := qualified.FindStringSubmatch(item)
			if q == nil || (table != "" && q[1] != table) {
				return m
			}

			table = q[1]
		}

		return fmt.Sprintf("SELECT %s‹%s 的 %d 列› FROM ", parts[1], table, len(items))
	})
}

// SQLPrinter 是一个 tsq.Logger：把 WithSQLLogging 的语句打印成 "SQL>" 行，
// 把建表的 DDL 打印成 "DDL>" 行，警告打印成 "WARN>" 行，其余信息忽略。
// tsq.Logger 是 *slog.Logger 的一个子集，生产环境直接传 slog.Default() 即可。
type SQLPrinter struct {
	W  io.Writer
	on atomic.Bool
}

// On 开始打印。零值的 SQLPrinter 是静默的。
func (p *SQLPrinter) On() { p.on.Store(true) }

// Off 停止打印。
func (p *SQLPrinter) Off() { p.on.Store(false) }

// Enabled 实现 tsq.Logger。
func (p *SQLPrinter) Enabled(context.Context, slog.Level) bool { return p.on.Load() }

// LogAttrs 实现 tsq.Logger。
func (p *SQLPrinter) LogAttrs(_ context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	if !p.on.Load() {
		return
	}

	values := map[string]string{}
	for _, attr := range attrs {
		values[attr.Key] = attr.Value.String()
	}

	switch {
	case values["sql"] != "":
		line := "    SQL> " + abbreviate(values["sql"])
		if args := values["args"]; args != "" && args != "null" && args != "[]" {
			line += "   -- 参数 " + args
		}

		writef(p.W, "%s\n", line)
	case values["ddl"] != "":
		writef(p.W, "    DDL> %s\n", strings.Join(strings.Fields(values["ddl"]), " "))
	case level >= slog.LevelWarn:
		writef(p.W, "    WARN> %s\n", msg)
	}
}

// Step 打印一个小节标题。
func Step(w io.Writer, title string) {
	writef(w, "\n## %s\n", title)
}

// Resultf 打印一行结果，缩进在 SQL 下面。
func Resultf(w io.Writer, format string, args ...any) {
	writef(w, "    ⇒ "+format+"\n", args...)
}

// Yuan 把以分为单位的金额格式化成 ¥12.34。
func Yuan(cents int64) string {
	return fmt.Sprintf("¥%d.%02d", cents/100, cents%100)
}

// writef 写到终端或测试的缓冲区；写不进去时示例也无从补救，所以不返回错误。
func writef(w io.Writer, format string, args ...any) {
	if _, err := fmt.Fprintf(w, format, args...); err != nil {
		return
	}
}
