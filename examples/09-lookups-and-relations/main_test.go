package main

import (
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	var out strings.Builder
	if err := run(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"Get(3)：Swift 笔记本",
		"Find(404)：<nil>",
		"Fetch(5, 1, 3)：Go 语言编程、Aurora 手机、Swift 笔记本",
		"errors.Is(err, sql.ErrNoRows) = true",
		"GetByEmail：Ada",
		"订单 1 里商品 5 的数量：2",
		"a lookup by status is not unique",
		"订单 1（paid）：2 行明细",
		"明细 3：陶瓷马克杯 × 4",
		"一条语句 → 8 行",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
