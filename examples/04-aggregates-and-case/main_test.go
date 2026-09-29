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
		"手机：2 件，库存 70，均价 ¥4999.00，最高 ¥5999.00",
		"Ada：2 单，共 ¥4333.00",
		"被买过的不同商品：6 种",
		"refunded 订单总额：NULL",
		"GROUP BY 1 ORDER BY 1 ASC",
		"¥5000 以上：3 件",
		"ADA@EXAMPLE.COM（15 个字符，前三个是 ada）",
		"SELECT DISTINCT",
		"Titan 工作站 九折价 ¥17999.10",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
