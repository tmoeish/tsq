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
		"[电脑] Swift 笔记本 ¥6999.00",
		"订单 1  Ada  Go 语言编程 × 2 = ¥178.00",
		"LEFT JOIN",
		"Dan：没有订单",
		"cannot hold NULL",
		"电子产品 / 手机",
		"图书 ← —",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
