package parser

import (
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/serenize/snaker"

	"github.com/tmoeish/tsq"
)

// ========== DSL AST 解析器实现 ========== //

// TokenType 表示 DSL 的 token 类型
// 标识符、字符串、符号、数组、对象、逗号等
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdent
	TokenString
	TokenNumber
	TokenBool
	TokenComma
	TokenEqual
	TokenLBracket // [
	TokenRBracket // ]
	TokenLBrace   // {
	TokenRBrace   // }
	TokenUnknown
)

type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

// ========== DSL AST 结构定义 ========== //
type (
	DSLNode   any
	DSLObject map[string]DSLNode
	DSLArray  []DSLNode
	DSLString string
	DSLBool   bool
	DSLNumber float64
)

// Tokenize 将 DSL 字符串分割为 Token 列表
func Tokenize(input string) ([]Token, error) {
	var tokens []Token

	i := 0
	skipSpace := func() {
		for i < len(input) && (input[i] == ' ' || input[i] == '\t' || input[i] == '\n' || input[i] == '\r') {
			i++
		}
	}

	skipSpace()

	for i < len(input) {
		c := input[i]
		if c == '=' {
			tokens = append(tokens, Token{Type: TokenEqual, Value: "=", Pos: i})
			i++

			skipSpace()

			continue
		}

		if c == ',' {
			tokens = append(tokens, Token{Type: TokenComma, Value: ",", Pos: i})
			i++

			skipSpace()

			continue
		}

		if c == '[' {
			tokens = append(tokens, Token{Type: TokenLBracket, Value: "[", Pos: i})
			i++

			skipSpace()

			continue
		}

		if c == ']' {
			tokens = append(tokens, Token{Type: TokenRBracket, Value: "]", Pos: i})
			i++

			skipSpace()

			continue
		}

		if c == '{' {
			tokens = append(tokens, Token{Type: TokenLBrace, Value: "{", Pos: i})
			i++

			skipSpace()

			continue
		}

		if c == '}' {
			tokens = append(tokens, Token{Type: TokenRBrace, Value: "}", Pos: i})
			i++

			skipSpace()

			continue
		}

		if c == '"' {
			// 字符串
			j := i + 1
			for j < len(input) && input[j] != '"' {
				if input[j] == '\\' && j+1 < len(input) {
					j += 2
				} else {
					j++
				}
			}

			if j >= len(input) {
				return nil, NewDSLUnclosedStringError(input, i)
			}

			unquoted, err := strconv.Unquote(input[i : j+1])
			if err != nil {
				unquoted = input[i+1 : j]
			}

			tokens = append(tokens, Token{Type: TokenString, Value: unquoted, Pos: i})
			i = j + 1

			skipSpace()

			continue
		}

		// 标识符/布尔/数字
		if isAlpha(c) {
			j := i
			for j < len(input) && (isAlphaNum(input[j]) || input[j] == '_') {
				j++
			}

			val := input[i:j]
			if val == "true" || val == "false" {
				tokens = append(tokens, Token{Type: TokenBool, Value: val, Pos: i})
			} else {
				tokens = append(tokens, Token{Type: TokenIdent, Value: val, Pos: i})
			}

			i = j

			skipSpace()

			continue
		}

		// 数字
		if isDigit(c) {
			j := i
			for j < len(input) && isDigit(input[j]) {
				j++
			}

			tokens = append(tokens, Token{Type: TokenNumber, Value: input[i:j], Pos: i})
			i = j

			skipSpace()

			continue
		}

		// 未知字符
		return nil, NewDSLTokenizeError(input, i, c)
	}

	tokens = append(tokens, Token{Type: TokenEOF, Value: "", Pos: len(input)})

	return tokens, nil
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isAlphaNum(c byte) bool {
	return isAlpha(c) || isDigit(c)
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// Parser 结构体
type Parser struct {
	tokens []Token
	pos    int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

func (p *Parser) peek() Token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}

	return Token{Type: TokenEOF}
}

func (p *Parser) next() Token {
	tok := p.peek()
	p.pos++

	return tok
}

func (p *Parser) expect(tt TokenType) (Token, error) {
	tok := p.next()
	if tok.Type != tt {
		if tt == TokenRBracket && tok.Type == TokenEOF {
			return tok, NewDSLArrayMissingClosingBracketError(tok.Pos)
		}

		if tt == TokenRBrace && tok.Type == TokenEOF {
			return tok, NewDSLMissingBraceError(tok.Pos)
		}

		return tok, NewDSLUnexpectedTokenError(getTokenTypeName(tt), tok.Value, tok.Pos)
	}

	return tok, nil
}

// ParseDSL 入口，解析为对象
func ParseDSL(tokens []Token) (DSLObject, error) {
	p := NewParser(tokens)
	obj := DSLObject{}

	for p.peek().Type != TokenEOF {
		if p.peek().Type == TokenComma {
			p.next()
			continue
		}

		key, val, err := p.parseKeyValueOrIdent()
		if err != nil {
			return nil, errors.Trace(err)
		}

		if key != "" {
			if _, exists := obj[key]; exists {
				return nil, errors.Trace(NewDSLDuplicateKeyError(key, p.tokens[p.pos-1].Pos))
			}

			obj[key] = val
		} else {
			return nil, errors.Trace(unexpectedDSLTopLevelTokenError(p.peek()))
		}
	}

	return obj, nil
}

// parseKeyValueOrIdent 解析 key=value 或单独 ident
func (p *Parser) parseKeyValueOrIdent() (string, DSLNode, error) {
	tok := p.peek()
	if tok.Type == TokenIdent {
		key := p.next().Value

		if p.peek().Type == TokenEqual {
			p.next() // skip =

			val, err := p.parseValue()
			if err != nil {
				return "", nil, errors.Trace(err)
			}

			return key, val, nil
		}
		// 简写 v/ct/mt/dt
		return key, DSLBool(true), nil
	}

	return "", nil, nil
}

func unexpectedDSLTokenError(tok Token) error {
	actual := tok.Value
	if actual == "" {
		actual = getTokenTypeName(tok.Type)
	}

	return NewDSLUnexpectedTokenError("identifier", actual, tok.Pos)
}

func unexpectedDSLTopLevelTokenError(tok Token) error {
	actual := tok.Value
	if actual == "" {
		actual = getTokenTypeName(tok.Type)
	}

	return NewDSLUnexpectedTopLevelTokenError(actual, tok.Pos)
}

func unexpectedDSLObjectTokenError(tok Token) error {
	actual := tok.Value
	if actual == "" {
		actual = getTokenTypeName(tok.Type)
	}

	return NewDSLUnexpectedObjectTokenError(actual, tok.Pos)
}

// parseValue 解析 value
func (p *Parser) parseValue() (DSLNode, error) {
	tok := p.peek()
	switch tok.Type {
	case TokenString:
		return DSLString(p.next().Value), nil
	case TokenBool:
		p.next()
		return DSLBool(tok.Value == "true"), nil
	case TokenNumber:
		p.next()

		num, err := parseNumber(tok.Value)
		if err != nil {
			return nil, NewDSLInvalidNumberError(tok.Value, tok.Pos)
		}

		return DSLNumber(num), nil
	case TokenLBracket:
		return p.parseArray()
	case TokenLBrace:
		return p.parseObject()
	case TokenIdent:
		return DSLString(p.next().Value), nil
	default:
		actual := tok.Value
		if actual == "" {
			actual = getTokenTypeName(tok.Type)
		}

		return nil, NewDSLUnexpectedValueTokenError(actual, tok.Pos)
	}
}

func (p *Parser) parseArray() (DSLArray, error) {
	_, err := p.expect(TokenLBracket)
	if err != nil {
		return nil, errors.Trace(err)
	}

	arr := DSLArray{}

	for p.peek().Type != TokenRBracket && p.peek().Type != TokenEOF {
		if p.peek().Type == TokenComma {
			p.next()
			continue
		}

		val, err := p.parseValue()
		if err != nil {
			return nil, errors.Trace(err)
		}

		arr = append(arr, val)
	}

	_, err = p.expect(TokenRBracket)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return arr, nil
}

func (p *Parser) parseObject() (DSLObject, error) {
	_, err := p.expect(TokenLBrace)
	if err != nil {
		return nil, errors.Trace(err)
	}

	obj := DSLObject{}

	for p.peek().Type != TokenRBrace && p.peek().Type != TokenEOF {
		if p.peek().Type == TokenComma {
			p.next()
			continue
		}

		key, val, err := p.parseKeyValueOrIdent()
		if err != nil {
			return nil, errors.Trace(err)
		}

		if key != "" {
			if _, exists := obj[key]; exists {
				return nil, NewDSLDuplicateKeyError(key, p.tokens[p.pos-1].Pos)
			}

			obj[key] = val
		} else {
			return nil, unexpectedDSLObjectTokenError(p.peek())
		}
	}

	_, err = p.expect(TokenRBrace)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return obj, nil
}

func parseNumber(s string) (float64, error) {
	// 验证输入只包含数字
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return 0, NewDSLInvalidNumberError(s, i)
		}
	}

	// 使用标准库解析，支持更多数字格式
	num, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return num, nil
}

// getTokenTypeName 获取 token 类型名称
func getTokenTypeName(tt TokenType) string {
	switch tt {
	case TokenEOF:
		return "EOF"
	case TokenIdent:
		return "identifier"
	case TokenString:
		return "string"
	case TokenNumber:
		return "number"
	case TokenBool:
		return "boolean"
	case TokenComma:
		return "comma"
	case TokenEqual:
		return "equal"
	case TokenLBracket:
		return "left bracket"
	case TokenRBracket:
		return "right bracket"
	case TokenLBrace:
		return "left brace"
	case TokenRBrace:
		return "right brace"
	default:
		return "unknown"
	}
}

// genTableInfoFromAST 将 AST 映射到 tsq.TableInfo
func genTableInfoFromAST(
	name string,
	ast DSLObject,
	isTable bool,
	structFields map[string]struct{},
) (*tsq.TableInfo, error) {
	info := &tsq.TableInfo{
		IsResult: !isTable,
	}

	if isTable {
		info.Table = snaker.CamelToSnake(name)
	}
	// 默认值
	if isTable {
		info.ID = DefaultPKField
		info.AI = true
	}

	for k, v := range ast {
		switch k {
		case "name":
			s, ok := v.(DSLString)
			if !ok {
				return nil, errors.Trace(NewDSLValueTypeError(k, "string", v))
			}

			if string(s) != "" {
				info.Table = string(s)
			}
		case "pk":
			s, ok := v.(DSLString)
			if !ok {
				return nil, errors.Trace(NewDSLValueTypeError(k, "string like \"ID\" or \"ID,true\"", v))
			}

			id, auto, err := parsePrimaryKeyDSL(string(s))
			if err != nil {
				return nil, errors.Trace(err)
			}

			info.ID = id
			info.AI = auto
		case "version":
			if s, ok := v.(DSLString); ok {
				info.VersionField = string(s)
			} else if b, ok := v.(DSLBool); ok && bool(b) {
				info.VersionField = DefaultVersionField
			} else if _, ok := v.(DSLBool); !ok {
				return nil, NewDSLValueTypeError(k, "string or boolean", v)
			}
		case "created_at":
			if s, ok := v.(DSLString); ok {
				info.CreatedAtField = string(s)
			} else if b, ok := v.(DSLBool); ok && bool(b) {
				info.CreatedAtField = DefaultCreatedAtField
			} else if _, ok := v.(DSLBool); !ok {
				return nil, NewDSLValueTypeError(k, "string or boolean", v)
			}
		case "updated_at":
			if s, ok := v.(DSLString); ok {
				info.UpdatedAtField = string(s)
			} else if b, ok := v.(DSLBool); ok && bool(b) {
				info.UpdatedAtField = DefaultUpdatedAtField
			} else if _, ok := v.(DSLBool); !ok {
				return nil, NewDSLValueTypeError(k, "string or boolean", v)
			}
		case "deleted_at":
			if s, ok := v.(DSLString); ok {
				info.DeletedAtField = string(s)
			} else if b, ok := v.(DSLBool); ok && bool(b) {
				info.DeletedAtField = DefaultDeletedAtField
			} else if _, ok := v.(DSLBool); !ok {
				return nil, NewDSLValueTypeError(k, "string or boolean", v)
			}
		case "ux":
			arr, ok := v.(DSLArray)
			if !ok {
				return nil, NewDSLValueTypeError(k, "array of index objects", v)
			}

			for _, node := range arr {
				obj, ok := node.(DSLObject)
				if !ok {
					return nil, NewDSLArrayEntryTypeError(k, "object with fields=[...]", node)
				}

				idx := tsq.IndexInfo{}

				for k2, v2 := range obj {
					switch k2 {
					case "name":
						s, ok := v2.(DSLString)
						if !ok {
							return nil, NewDSLValueTypeError(k2, "string", v2)
						}

						idx.Name = string(s)
					case "fields":
						arr2, ok := v2.(DSLArray)
						if !ok {
							return nil, NewDSLValueTypeError(k2, "array of Go field names", v2)
						}

						for _, f := range arr2 {
							fs, ok := f.(DSLString)
							if !ok {
								return nil, NewDSLArrayEntryTypeError(k2, "string Go field name", f)
							}

							idx.Fields = append(idx.Fields, string(fs))
						}
					default:
						return nil, NewDSLUnknownIndexKeyError(k2)
					}
				}

				if len(idx.Fields) == 0 {
					return nil, NewDSLEmptyArrayError("fields")
				}

				info.UxList = append(info.UxList, idx)
			}

		case "idx":
			arr, ok := v.(DSLArray)
			if !ok {
				return nil, NewDSLValueTypeError(k, "array of index objects", v)
			}

			for _, node := range arr {
				obj, ok := node.(DSLObject)
				if !ok {
					return nil, NewDSLArrayEntryTypeError(k, "object with fields=[...]", node)
				}

				idx := tsq.IndexInfo{}

				for k2, v2 := range obj {
					switch k2 {
					case "name":
						s, ok := v2.(DSLString)
						if !ok {
							return nil, NewDSLValueTypeError(k2, "string", v2)
						}

						idx.Name = string(s)
					case "fields":
						arr2, ok := v2.(DSLArray)
						if !ok {
							return nil, NewDSLValueTypeError(k2, "array of Go field names", v2)
						}

						for _, f := range arr2 {
							fs, ok := f.(DSLString)
							if !ok {
								return nil, NewDSLArrayEntryTypeError(k2, "string Go field name", f)
							}

							idx.Fields = append(idx.Fields, string(fs))
						}
					default:
						return nil, NewDSLUnknownIndexKeyError(k2)
					}
				}

				if len(idx.Fields) == 0 {
					return nil, NewDSLEmptyArrayError("fields")
				}

				info.IdxList = append(info.IdxList, idx)
			}

		case "kw":
			arr, ok := v.(DSLArray)
			if !ok {
				return nil, NewDSLValueTypeError(k, "array of Go field names", v)
			}

			for _, node := range arr {
				s, ok := node.(DSLString)
				if !ok {
					return nil, NewDSLArrayEntryTypeError(k, "string Go field name", node)
				}

				info.KwList = append(info.KwList, string(s))
			}
		default:
			return nil, NewDSLUnknownTableKeyError(k)
		}
	}

	normalizeIndexNames(info.UxList, "ux", info.Table)
	normalizeIndexNames(info.IdxList, "idx", info.Table)

	// 新增：校验 DSL 字段和索引
	err := validateTableInfoAgainstStruct(info, structFields, name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return info, nil
}

func defaultIndexName(prefix, table string, fields []string) string {
	parts := make([]string, 0, len(fields)+2)
	parts = append(parts, prefix, snaker.CamelToSnake(table))

	for _, field := range fields {
		parts = append(parts, snaker.CamelToSnake(field))
	}

	return strings.Join(parts, "_")
}

func normalizeIndexNames(indexes []tsq.IndexInfo, prefix, table string) {
	for i := range indexes {
		switch {
		case indexes[i].Name == "":
			indexes[i].Name = defaultIndexName(prefix, table, indexes[i].Fields)
		case strings.HasPrefix(indexes[i].Name, "Ux") && !strings.Contains(indexes[i].Name, "_"):
			indexes[i].Name = snaker.CamelToSnake(indexes[i].Name)
		case strings.HasPrefix(indexes[i].Name, "Idx") && !strings.Contains(indexes[i].Name, "_"):
			indexes[i].Name = snaker.CamelToSnake(indexes[i].Name)
		}
	}
}

// validateTableInfoAgainstStruct 校验 DSL 字段和索引
func validateTableInfoAgainstStruct(info *tsq.TableInfo, structFields map[string]struct{}, structName string) error {
	// 1. 字段存在性校验
	for _, field := range []string{info.ID, info.VersionField, info.CreatedAtField, info.UpdatedAtField, info.DeletedAtField} {
		if field != "" && structFields != nil {
			if _, ok := structFields[field]; !ok {
				return NewDSLFieldNotFoundError(field, structName)
			}
		}
	}
	// 2. ux/idx 校验
	seen := map[string]string{} // key: fields串, value: indexName

	for _, idx := range append(info.UxList, info.IdxList...) {
		fieldSet := map[string]struct{}{}

		for _, f := range idx.Fields {
			if structFields != nil {
				if _, ok := structFields[f]; !ok {
					return NewDSLFieldNotFoundError(f, structName)
				}
			}

			if _, ok := fieldSet[f]; ok {
				return NewDSLIndexFieldDuplicateError(idx.Name, f)
			}

			fieldSet[f] = struct{}{}
		}

		key := strings.Join(idx.Fields, ",")
		if _, ok := seen[key]; ok {
			return NewDSLIndexDuplicateError(idx.Name, key)
		}

		seen[key] = idx.Name
	}

	// 3. kwList 校验
	for _, kw := range info.KwList {
		if _, ok := structFields[kw]; !ok {
			return NewDSLFieldNotFoundError(kw, structName)
		}
	}

	return nil
}

func parsePrimaryKeyDSL(value string) (string, bool, error) {
	parts := strings.Split(value, ",")
	if len(parts) == 0 {
		return "", false, NewDSLInvalidPrimaryKeyError(value, "primary key field name is empty")
	}

	id := strings.TrimSpace(parts[0])
	if id == "" {
		return "", false, NewDSLInvalidPrimaryKeyError(value, "primary key field name is empty")
	}

	if strings.Contains(id, " ") || strings.Contains(id, "\t") || strings.Contains(id, "\n") {
		return "", false, NewDSLInvalidPrimaryKeyError(value, "field name must not contain whitespace")
	}

	if strings.Contains(id, ";") || strings.Contains(id, "=") || strings.Contains(id, ":") {
		return "", false, NewDSLInvalidPrimaryKeyError(value, "field name contains invalid characters")
	}

	if strings.Contains(id, ",") {
		return "", false, errors.Trace(errors.New("composite primary keys are not supported"))
	}

	auto := true

	switch len(parts) {
	case 1:
		return id, auto, nil
	case 2:
		switch strings.TrimSpace(parts[1]) {
		case "true":
			auto = true
		case "false":
			auto = false
		default:
			return "", false, NewDSLInvalidPrimaryKeyError(value, "auto-increment flag must be true or false")
		}
	default:
		return "", false, NewDSLInvalidPrimaryKeyError(value, "too many comma-separated parts")
	}

	return id, auto, nil
}
