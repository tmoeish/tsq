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
		`"products"."deleted_at" = 0`,
		"P-4001  陶瓷马克杯  ¥39.00  库存 500",
		"Bob <bob@example.com>",
		"分类 2：2 件商品",
		"Get: Ada，等级 vip",
		"Find 一个不存在的邮箱: <nil>",
		"缺货商品 2 件",
		"已下架：北欧台灯",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
