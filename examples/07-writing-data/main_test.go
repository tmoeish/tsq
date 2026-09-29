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
		"Level=regular",
		"数据库算出的小计 ¥117.00",
		"P-3001 重复的 SKU → 跳过",
		"version 1 → 2",
		"the row was read with only id, name, version",
		"整行 upsert：ID 仍是 1，手机号 <nil>",
		"只更新 name：Bob Builder，等级仍是 regular",
		"图书涨价 10%：4 行",
		"删掉已取消订单的明细：1 行",
		"下单成功：订单 6，金额 ¥7998.00",
		"库存不足",
		"回滚后笔记本库存仍是 10",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
