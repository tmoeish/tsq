// internal/parser/table.go
//
// 负责从 Go AST 注释中解析表（@TABLE）和 DTO（@DTO）元数据，生成 tsq.TableInfo 结构体。
// 支持自定义 DSL 解析、索引与查询生成、元数据排序等。

package parser

import (
	"go/ast"
	"sort"
	"strings"

	"github.com/tmoeish/tsq"
)

// ParseTableInfo 从注释组中解析表元数据，返回 TableInfo 结构体
func ParseTableInfo(
	structName string,
	commentGroup []*ast.CommentGroup,
	structFields map[string]struct{},
) (*tsq.TableInfo, error) {
	if commentGroup == nil {
		return nil, nil
	}

	// 解析注解，填充 meta
	info, err := parseDSL(structName, commentGroup, structFields)
	if err != nil {
		return nil, err
	}

	if info == nil {
		return nil, nil
	}

	// 生成查询索引列表
	generateQueryList(info)

	// 排序所有列表
	sortTableInfoLists(info)

	return info, nil
}

// CleanCommentPrefix 去除一行注释的前缀和多余空白
func CleanCommentPrefix(line string) string {
	line = strings.TrimLeft(line, " \t")
	for _, prefix := range []string{"//", "/*", "*", "*/"} {
		if strings.HasPrefix(line, prefix) {
			line = strings.TrimPrefix(line, prefix)
			line = strings.TrimLeft(line, " \t")
		}
	}

	return line
}

// CleanBlockComment 去除块注释前缀和后缀
func CleanBlockComment(text string) string {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "//") {
		text = strings.TrimPrefix(text, "//")
		text = strings.TrimSpace(text)
	}

	if strings.HasPrefix(text, "/*") {
		text = strings.TrimPrefix(text, "/*")
		text = strings.TrimSuffix(text, "*/")
		text = strings.TrimSpace(text)
	}

	return text
}

// parseDSL 解析所有注释中的注解（@TABLE/@DTO），直接填充 info
func parseDSL(
	structName string,
	commentGroup []*ast.CommentGroup,
	structFields map[string]struct{},
) (*tsq.TableInfo, error) {
	for _, comments := range commentGroup {
		// 合并整个注释组，并健壮去除每行注释前缀
		var lines []string
		for _, comment := range comments.List {
			lines = append(lines, CleanCommentPrefix(comment.Text))
		}

		text := strings.Join(lines, "\n")
		text = strings.TrimSpace(text)

		if strings.Contains(text, "@TABLE") {
			return parseTableDSL(structName, text, structFields)
		} else if strings.Contains(text, "@DTO") {
			return parseDTODSL(structName, text, structFields)
		}
	}

	return nil, nil
}

// parseTableDSL 解析 @TABLE DSL 并填充 meta
func parseTableDSL(
	structName string,
	text string,
	structFields map[string]struct{},
) (*tsq.TableInfo, error) {
	// 去除注释前缀
	text = CleanBlockComment(text)
	// 提取括号内内容
	start := strings.Index(text, "(")
	end := strings.LastIndex(text, ")")

	if start == -1 || end == -1 || end <= start {
		return nil, nil
	}

	content := text[start+1 : end]
	content = strings.ReplaceAll(content, "\n", " ")
	content = strings.ReplaceAll(content, "\r", " ")
	content = strings.TrimSpace(content)

	tokens, err := Tokenize(content)
	if err != nil {
		return nil, err
	}

	ast, err := ParseDSL(tokens)
	if err != nil {
		return nil, err
	}

	return genTableInfoFromAST(structName, ast, true, structFields)
}

// parseDTODSL 解析 @DTO DSL 并填充 meta
func parseDTODSL(
	structName string,
	text string,
	structFields map[string]struct{},
) (*tsq.TableInfo, error) {
	// 去除注释前缀
	text = CleanBlockComment(text)
	// 提取括号内内容
	start := strings.Index(text, "(")
	end := strings.LastIndex(text, ")")

	if start == -1 || end == -1 || end <= start {
		return &tsq.TableInfo{}, nil
	}

	content := text[start+1 : end]
	content = strings.ReplaceAll(content, "\n", " ")
	content = strings.ReplaceAll(content, "\r", " ")
	content = strings.TrimSpace(content)

	tokens, err := Tokenize(content)
	if err != nil {
		return nil, err
	}

	ast, err := ParseDSL(tokens)
	if err != nil {
		return nil, err
	}

	return genTableInfoFromAST(structName, ast, false, structFields)
}

// generateQueryList 生成查询索引列表，支持普通、集合、前缀等多种组合
func generateQueryList(meta *tsq.TableInfo) {
	queryMap := make(map[string]bool)

	for _, idx := range meta.IdxList {
		// 普通 query
		queryName := strings.Join(idx.Fields, "And")
		if !queryMap[queryName] {
			meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
				Name:   queryName,
				Fields: idx.Fields,
				IsSet:  false,
			})
			queryMap[queryName] = true
		}

		// set query
		setName := queryName + "In"
		if !queryMap[setName] {
			meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
				Name:   setName,
				Fields: idx.Fields,
				IsSet:  true,
			})
			queryMap[setName] = true
		}

		// 前缀索引
		for j := len(idx.Fields); j > 0; j-- {
			prefixQueryName := strings.Join(idx.Fields[:j], "And")
			if !queryMap[prefixQueryName] {
				meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
					Name:   prefixQueryName,
					Fields: idx.Fields[:j],
					IsSet:  false,
				})
				queryMap[prefixQueryName] = true
			}

			setName := prefixQueryName + "In"
			if !queryMap[setName] {
				meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
					Name:   setName,
					Fields: idx.Fields[:j],
					IsSet:  true,
				})
				queryMap[setName] = true
			}
		}
	}

	for _, ux := range meta.UxList {
		for j := len(ux.Fields) - 1; j > 0; j-- {
			prefixQueryName := strings.Join(ux.Fields[:j], "And")
			if !queryMap[prefixQueryName] {
				meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
					Name:   prefixQueryName,
					Fields: ux.Fields[:j],
					IsSet:  false,
				})
				queryMap[prefixQueryName] = true
			}

			setName := prefixQueryName + "In"
			if !queryMap[setName] {
				meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
					Name:   setName,
					Fields: ux.Fields[:j],
					IsSet:  true,
				})
				queryMap[setName] = true
			}
		}
	}
}

// sortTableInfoLists 对元数据中的各种列表进行排序，保证输出有序
func sortTableInfoLists(meta *tsq.TableInfo) {
	sort.Slice(meta.UxList, func(i, j int) bool {
		return meta.UxList[i].Name < meta.UxList[j].Name
	})
	sort.Slice(meta.IdxList, func(i, j int) bool {
		return meta.IdxList[i].Name < meta.IdxList[j].Name
	})
	sort.Slice(meta.QueryList, func(i, j int) bool {
		return meta.QueryList[i].Name < meta.QueryList[j].Name
	})
}
