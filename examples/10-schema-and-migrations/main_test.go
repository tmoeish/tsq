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
		"AUTOINCREMENT",
		"AUTO_INCREMENT",
		"Validate 通过",
		"*tsq.MissingTableError：缺表 categories",
		"add column phone",
		`ALTER TABLE "customers" ADD COLUMN "phone"`,
		"再次 Validate：通过",
		"索引 ux_products_sku [deleted_at sku]",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("输出里没有 %q：\n%s", want, out.String())
		}
	}
}
