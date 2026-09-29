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
		"LIMIT $3",
		"`products`.`name` LIKE ?",
		"mysql    不支持 FULL JOIN",
		"手机类最便宜的：Aurora 手机",
		"span：list products（成功）",
		"span：tx （失败",
		"DialectOf(exec) = sqlite",
		"Size=5，本页 5 条，共 8 条",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
