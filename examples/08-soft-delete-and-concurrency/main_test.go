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
		"已删除：true；看得见的商品 7 件，含已删除的 8 件",
		"FindBySKU(P-4002)：<nil>",
		"Op=delete，Need=a live row",
		"恢复后已删除：false",
		"下架缺货商品：2 件",
		"还看得见 5 件",
		"是乐观锁冲突：true",
		"第 2 次尝试成功，库存 49，version 2",
		"sqlite 不支持 FOR UPDATE",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
