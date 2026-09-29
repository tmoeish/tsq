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
		`CREATE TABLE IF NOT EXISTS "todos"`,
		"新行的 ID = 1",
		"ID 依次是 2、3、4",
		"#2 跑一遍 tsq gen",
		"还剩 3 条待办",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
