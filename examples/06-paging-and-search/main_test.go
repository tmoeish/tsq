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
		"第 2/3 页，共 8 件，还有下一页：true",
		"ESCAPE '~'",
		`搜 "图书"：2 件`,
		`搜 ""：6 件`,
		"400 Bad Request：字段 stock",
		"第 3 页：[SQL 必知必会 陶瓷马克杯]",
		"Swift 笔记本：轻薄 14 英寸笔记本电脑",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
