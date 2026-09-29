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
		`"customers"."id" IN (SELECT "orders"."customer_id"`,
		"Titan 工作站 ¥19999.00",
		"NOT EXISTS",
		`WITH "spend" AS`,
		"Bob ¥6999.00",
		"VIP 或 有已付款订单：[Ada Cai Dan]",
		"VIP 且 有已付款订单：[Ada]",
		"VIP 但 没有已付款订单：[Dan]",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
