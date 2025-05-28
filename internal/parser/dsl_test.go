package parser

import (
	"reflect"
	"testing"

	"github.com/tmoeish/tsq"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		input    string
		expect   []Token
		hasError bool
	}{
		{
			input: `name = "test"`,
			expect: []Token{
				{Type: TokenIdent, Value: "name"},
				{Type: TokenEqual, Value: "="},
				{Type: TokenString, Value: "test"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			input: `flag = true, num = 123`,
			expect: []Token{
				{Type: TokenIdent, Value: "flag"},
				{Type: TokenEqual, Value: "="},
				{Type: TokenBool, Value: "true"},
				{Type: TokenComma, Value: ","},
				{Type: TokenIdent, Value: "num"},
				{Type: TokenEqual, Value: "="},
				{Type: TokenNumber, Value: "123"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			input: `arr = [1, 2, 3]`,
			expect: []Token{
				{Type: TokenIdent, Value: "arr"},
				{Type: TokenEqual, Value: "="},
				{Type: TokenLBracket, Value: "["},
				{Type: TokenNumber, Value: "1"},
				{Type: TokenComma, Value: ","},
				{Type: TokenNumber, Value: "2"},
				{Type: TokenComma, Value: ","},
				{Type: TokenNumber, Value: "3"},
				{Type: TokenRBracket, Value: "]"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			input: `obj = {a = 1, b = "x"}`,
			expect: []Token{
				{Type: TokenIdent, Value: "obj"},
				{Type: TokenEqual, Value: "="},
				{Type: TokenLBrace, Value: "{"},
				{Type: TokenIdent, Value: "a"},
				{Type: TokenEqual, Value: "="},
				{Type: TokenNumber, Value: "1"},
				{Type: TokenComma, Value: ","},
				{Type: TokenIdent, Value: "b"},
				{Type: TokenEqual, Value: "="},
				{Type: TokenString, Value: "x"},
				{Type: TokenRBrace, Value: "}"},
				{Type: TokenEOF, Value: ""},
			},
		},
		{
			input:    `"unclosed`,
			hasError: true,
		},
		{
			input:  ``,
			expect: []Token{{Type: TokenEOF, Value: ""}},
		},
		{
			input:    `@invalid`,
			hasError: true,
		},
	}
	for _, tt := range tests {
		tokens, err := Tokenize(tt.input)
		if tt.hasError {
			if err == nil {
				t.Errorf("expected error for input %q", tt.input)
			}

			continue
		}

		if err != nil {
			t.Errorf("unexpected error for input %q: %v", tt.input, err)
			continue
		}

		if !reflect.DeepEqual(tokens, tt.expect) {
			t.Errorf("Tokenize(%q) = %#v, want %#v", tt.input, tokens, tt.expect)
		}
	}
}

func TestParseDSL(t *testing.T) {
	tokens, _ := Tokenize(`name = "t1", pk = "id,true", v, ct = "created", arr = [1, 2, 3], obj = {a = 1, b = "x"}`)

	ast, err := ParseDSL(tokens)
	if err != nil {
		t.Fatalf("ParseDSL error: %v", err)
	}

	if ast["name"] != DSLString("t1") {
		t.Errorf("name field error")
	}

	if ast["pk"] != DSLString("id,true") {
		t.Errorf("pk field error")
	}

	if ast["v"] != DSLBool(true) {
		t.Errorf("v field error")
	}

	if arr, ok := ast["arr"].(DSLArray); !ok || len(arr) != 3 {
		t.Errorf("arr field error")
	}

	if obj, ok := ast["obj"].(DSLObject); !ok || obj["a"] != DSLNumber(1) || obj["b"] != DSLString("x") {
		t.Errorf("obj field error")
	}
}

func TestParser_parseValue(t *testing.T) {
	tokens, _ := Tokenize(`"abc" true 123 [1,2] {a=1}`)

	p := NewParser(tokens)

	v, err := p.parseValue()
	if err != nil {
		t.Errorf("parseValue string error: %v", err)
	}

	if v != DSLString("abc") {
		t.Errorf("parseValue string error")
	}

	v, err = p.parseValue()
	if err != nil {
		t.Errorf("parseValue bool error: %v", err)
	}

	if v != DSLBool(true) {
		t.Errorf("parseValue bool error")
	}

	v, err = p.parseValue()
	if err != nil {
		t.Errorf("parseValue number error: %v", err)
	}

	if num, ok := v.(DSLNumber); !ok || float64(num) != 123 {
		t.Errorf("parseValue number error, got %v", v)
	}

	v, err = p.parseValue()
	if err != nil {
		t.Errorf("parseValue array error: %v", err)
	}

	if arr, ok := v.(DSLArray); !ok || len(arr) != 2 || arr[0] != DSLNumber(1) || arr[1] != DSLNumber(2) {
		t.Errorf("parseValue array error, got %v", v)
	}

	v, err = p.parseValue()
	if err != nil {
		t.Errorf("parseValue object error: %v", err)
	}

	if obj, ok := v.(DSLObject); !ok || obj["a"] != DSLNumber(1) {
		t.Errorf("parseValue object error, got %v", v)
	}
}

func TestParser_parseArray(t *testing.T) {
	tokens, _ := Tokenize(`[1, "x", true]`)
	p := NewParser(tokens)

	arr, err := p.parseArray()
	if err != nil {
		t.Errorf("parseArray error: %v", err)
	}

	if len(arr) != 3 {
		t.Errorf("parseArray length error")
	}

	if arr[0] != DSLNumber(1) || arr[1] != DSLString("x") || arr[2] != DSLBool(true) {
		t.Errorf("parseArray value error")
	}
}

func TestParser_parseObject(t *testing.T) {
	tokens, _ := Tokenize(`{a=1, b="x"}`)
	p := NewParser(tokens)

	obj, err := p.parseObject()
	if err != nil {
		t.Errorf("parseObject error: %v", err)
	}

	if obj["a"] != DSLNumber(1) || obj["b"] != DSLString("x") {
		t.Errorf("parseObject value error")
	}
}

func Test_parseNumber(t *testing.T) {
	num, err := parseNumber("123")
	if err != nil {
		t.Errorf("parseNumber error: %v", err)
	}

	if num != 123 {
		t.Errorf("parseNumber error")
	}

	num, err = parseNumber("0")
	if err != nil {
		t.Errorf("parseNumber zero error: %v", err)
	}

	if num != 0 {
		t.Errorf("parseNumber zero error")
	}

	// 测试无效数字
	_, err = parseNumber("12a3")
	if err == nil {
		t.Errorf("expected error for invalid number")
	}
}

func TestDSLErrors(t *testing.T) {
	// 测试未闭合字符串错误
	_, err := Tokenize(`"unclosed`)
	if err == nil {
		t.Errorf("expected unclosed string error")
	}

	// 测试无效字符错误
	_, err = Tokenize(`@invalid`)
	if err == nil {
		t.Errorf("expected invalid character error")
	}

	// 测试解析错误
	tokens := []Token{
		{Type: TokenIdent, Value: "test"},
		{Type: TokenEqual, Value: "="},
		{Type: TokenLBracket, Value: "["},
		{Type: TokenEOF, Value: ""},
	}

	_, err = ParseDSL(tokens)
	if err == nil {
		t.Errorf("expected parse error for unclosed array")
	}
}

func Test_genTableInfoFromAST(t *testing.T) {
	ast := DSLObject{
		"name": DSLString("t1"),
		"pk":   DSLString("id,false"),
		"v":    DSLString("V1"),
		"ct":   DSLBool(true),
		"mt":   DSLString("mtime"),
		"dt":   DSLBool(true),
		"ux": DSLArray{
			DSLObject{"name": DSLString("ux1"), "fields": DSLArray{DSLString("f1"), DSLString("f2")}},
		},
		"idx": DSLArray{
			DSLObject{"fields": DSLArray{DSLString("f3")}},
		},
		"kw": DSLArray{
			DSLString("f2"), DSLString("f3"),
		},
	}
	structFields := map[string]struct{}{"id": {}, "V1": {}, "CT": {}, "mtime": {}, "DT": {}, "f1": {}, "f2": {}, "f3": {}}
	info, err := genTableInfoFromAST("MyTable", ast, true, structFields)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 验证基本字段
	if info.Table != "t1" {
		t.Errorf("Table name error: got %s, want t1", info.Table)
	}

	if info.ID != "id" || info.AI != false {
		t.Errorf("Primary key error: ID=%s, AI=%v", info.ID, info.AI)
	}

	if info.V != "V1" {
		t.Errorf("Version field error: got %s, want V1", info.V)
	}

	if info.CT != DefaultCTField {
		t.Errorf("Create time field error: got %s, want %s", info.CT, DefaultCTField)
	}

	if info.MT != "mtime" {
		t.Errorf("Modify time field error: got %s, want mtime", info.MT)
	}

	if info.DT != DefaultDTField {
		t.Errorf("Delete time field error: got %s, want %s", info.DT, DefaultDTField)
	}

	// 验证唯一索引
	if len(info.UxList) != 1 {
		t.Errorf("Unique index count error: got %d, want 1", len(info.UxList))
	} else {
		ux := info.UxList[0]
		if ux.Name != "ux1" {
			t.Errorf("Unique index name error: got %s, want ux1", ux.Name)
		}

		if len(ux.Fields) != 2 || ux.Fields[0] != "f1" || ux.Fields[1] != "f2" {
			t.Errorf("Unique index fields error: got %v, want [f1, f2]", ux.Fields)
		}
	}

	// 验证普通索引
	if len(info.IdxList) != 1 {
		t.Errorf("Index count error: got %d, want 1", len(info.IdxList))
	} else {
		idx := info.IdxList[0]
		if idx.Name != "idxf3" {
			t.Errorf("Index name error: got %s, want idxf3", idx.Name)
		}

		if len(idx.Fields) != 1 || idx.Fields[0] != "f3" {
			t.Errorf("Index fields error: got %v, want [f3]", idx.Fields)
		}
	}

	// 验证关键词
	if len(info.KwList) != 2 {
		t.Errorf("Keyword count error: got %d, want 2", len(info.KwList))
	} else {
		if info.KwList[0] != "f2" || info.KwList[1] != "f3" {
			t.Errorf("Keywords error: got %v, want [f2, f3]", info.KwList)
		}
	}
}

func Test_isAlphaNum_isAlpha_isDigit(t *testing.T) {
	if !isAlpha('a') || !isAlpha('Z') || isAlpha('1') {
		t.Errorf("isAlpha error")
	}

	if !isAlphaNum('a') || !isAlphaNum('1') || isAlphaNum('-') {
		t.Errorf("isAlphaNum error")
	}

	if !isDigit('0') || !isDigit('9') || isDigit('a') {
		t.Errorf("isDigit error")
	}
}

func Test_validateTableInfoAgainstStruct(t *testing.T) {
	structFields := map[string]struct{}{
		"id": {}, "name": {}, "age": {}, "email": {}, "created": {}, "updated": {},
	}
	// 1. 字段不存在
	info := &tsq.TableInfo{ID: "not_exist"}
	err := validateTableInfoAgainstStruct(info, structFields, "User")
	if err == nil || !IsErrorType(err, ErrorTypeDSLFieldNotFound) {
		t.Errorf("should detect field not found, got: %v", err)
	}
	// 2. ux/idx fields 内部有重复
	info = &tsq.TableInfo{
		UxList: []tsq.IndexInfo{{Name: "ux1", Fields: []string{"name", "name"}}},
	}
	err = validateTableInfoAgainstStruct(info, structFields, "User")
	if err == nil || !IsErrorType(err, ErrorTypeDSLIndexFieldDuplicate) {
		t.Errorf("should detect index field duplicate, got: %v", err)
	}
	// 3. ux/idx 列表有重复定义
	info = &tsq.TableInfo{
		UxList: []tsq.IndexInfo{{Name: "ux1", Fields: []string{"name", "email"}}, {Name: "ux2", Fields: []string{"name", "email"}}},
	}
	err = validateTableInfoAgainstStruct(info, structFields, "User")
	if err == nil || !IsErrorType(err, ErrorTypeDSLIndexDuplicate) {
		t.Errorf("should detect index duplicate, got: %v", err)
	}
	// 4. 正常通过
	info = &tsq.TableInfo{
		ID:      "id",
		UxList:  []tsq.IndexInfo{{Name: "ux1", Fields: []string{"name", "email"}}},
		IdxList: []tsq.IndexInfo{{Name: "idx1", Fields: []string{"age"}}},
	}
	err = validateTableInfoAgainstStruct(info, structFields, "User")
	if err != nil {
		t.Errorf("should pass valid case, got: %v", err)
	}
}
