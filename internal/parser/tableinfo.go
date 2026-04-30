// internal/parser/table.go
//
// 负责从 Go AST 注释中解析表（@TABLE）和结果结构（@RESULT）元数据，生成 tsq.TableInfo 结构体。
// 支持自定义 DSL 解析、索引与查询生成、元数据排序等。

package parser

import (
	"go/ast"
	"go/token"
	"sort"
	"strings"

	"github.com/juju/errors"

	"github.com/tmoeish/tsq"
)

// ParseTableInfo 从注释组中解析表元数据，返回 TableInfo 结构体
func ParseTableInfo(
	structName string,
	commentGroup []*ast.CommentGroup,
	structFields map[string]struct{},
	fileSet *token.FileSet,
) (*tsq.TableInfo, error) {
	if commentGroup == nil {
		return nil, nil
	}

	locator := newCommentLocator(commentGroup, fileSet)

	// 解析注解，填充 meta
	info, err := parseDSL(structName, commentGroup, structFields)
	if err != nil {
		return nil, errors.Trace(locator.attach(err))
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

type commentLocator struct {
	lines        []commentSourceLine
	contentLines []commentSourceLine
}

type commentSourceLine struct {
	filename string
	line     int
	text     string
}

func newCommentLocator(commentGroups []*ast.CommentGroup, fileSet *token.FileSet) commentLocator {
	if fileSet == nil {
		return commentLocator{}
	}

	lines := make([]commentSourceLine, 0)

	for _, group := range commentGroups {
		for _, comment := range group.List {
			pos := fileSet.Position(comment.Pos())

			rawLines := strings.Split(comment.Text, "\n")
			for i, rawLine := range rawLines {
				lines = append(lines, commentSourceLine{
					filename: pos.Filename,
					line:     pos.Line + i,
					text:     CleanCommentPrefix(rawLine),
				})
			}
		}
	}

	return commentLocator{
		lines:        lines,
		contentLines: extractAnnotationContentLines(lines),
	}
}

func (l commentLocator) attach(err error) error {
	parserErr := GetParserError(err)
	if parserErr == nil || len(l.lines) == 0 {
		return err
	}

	line, ok := l.findLine(parserErr)
	if !ok {
		return err
	}

	return attachParserErrorLocation(err, line.filename, line.line)
}

func (l commentLocator) findLine(parserErr *ParserError) (commentSourceLine, bool) {
	if pos, ok := parserErr.Context["position"].(int); ok && pos >= 0 {
		if line, found := l.findLineByOffset(pos); found {
			return line, true
		}
	}

	for _, key := range []string{"field", "key", "token", "actual"} {
		value, ok := parserErr.Context[key].(string)
		if !ok || value == "" {
			continue
		}

		if line, found := l.findLineByValue(value); found {
			return line, true
		}
	}

	if line, found := l.findLineContaining("@TABLE"); found {
		return line, true
	}

	if line, found := l.findLineContaining("@RESULT"); found {
		return line, true
	}

	return l.lines[0], true
}

func (l commentLocator) findLineByOffset(offset int) (commentSourceLine, bool) {
	if len(l.contentLines) > 0 {
		if line, found := findLineByOffsetInLines(l.contentLines, offset); found {
			return line, true
		}
	}

	return findLineByOffsetInLines(l.lines, offset)
}

func findLineByOffsetInLines(lines []commentSourceLine, offset int) (commentSourceLine, bool) {
	remaining := offset
	for _, line := range lines {
		if remaining <= len(line.text) {
			return line, true
		}

		remaining -= len(line.text) + 1
	}

	if len(lines) == 0 {
		return commentSourceLine{}, false
	}

	return lines[len(lines)-1], true
}

func (l commentLocator) findLineByValue(value string) (commentSourceLine, bool) {
	if value == "" {
		return commentSourceLine{}, false
	}

	quoted := `"` + value + `"`
	for _, line := range l.lines {
		if strings.Contains(line.text, quoted) {
			return line, true
		}
	}

	for _, line := range l.lines {
		if containsIdentifier(line.text, value) {
			return line, true
		}
	}

	return commentSourceLine{}, false
}

func (l commentLocator) findLineContaining(substr string) (commentSourceLine, bool) {
	for _, line := range l.lines {
		if strings.Contains(line.text, substr) {
			return line, true
		}
	}

	return commentSourceLine{}, false
}

func containsIdentifier(text, identifier string) bool {
	if identifier == "" {
		return false
	}

	idx := strings.Index(text, identifier)
	for idx >= 0 {
		beforeOK := idx == 0 || !isIdentifierChar(text[idx-1])
		afterIdx := idx + len(identifier)

		afterOK := afterIdx == len(text) || !isIdentifierChar(text[afterIdx])
		if beforeOK && afterOK {
			return true
		}

		next := strings.Index(text[idx+1:], identifier)
		if next == -1 {
			return false
		}

		idx += next + 1
	}

	return false
}

func isIdentifierChar(ch byte) bool {
	return ch == '_' ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9')
}

func extractAnnotationContentLines(lines []commentSourceLine) []commentSourceLine {
	if len(lines) == 0 {
		return nil
	}

	texts := make([]string, 0, len(lines))
	for _, line := range lines {
		texts = append(texts, line.text)
	}

	text := strings.Join(texts, "\n")
	for _, keyword := range []string{"@TABLE", "@RESULT"} {
		start, end, ok := findAnnotationContentRange(text, keyword)
		if !ok {
			continue
		}

		return sliceCommentLinesByOffsets(lines, start, end)
	}

	return nil
}

func findAnnotationContentRange(text, keyword string) (int, int, bool) {
	idx, ok := findAnnotationKeyword(text, keyword)
	if !ok {
		return 0, 0, false
	}

	searchStart := idx + len(keyword)
	afterKeyword := text[searchStart:]

	trimmedAfterKeyword := strings.TrimLeft(afterKeyword, " \t\r\n")
	if trimmedAfterKeyword == "" || trimmedAfterKeyword[0] != '(' {
		return 0, 0, false
	}

	start := searchStart + len(afterKeyword) - len(trimmedAfterKeyword)
	count := 0
	inString := false
	escaped := false

	for i := start; i < len(text); i++ {
		if inString {
			if escaped {
				escaped = false
				continue
			}

			switch text[i] {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}

			continue
		}

		switch text[i] {
		case '"':
			inString = true
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				return start + 1, i, true
			}
		}
	}

	return start + 1, len(text), true
}

func sliceCommentLinesByOffsets(lines []commentSourceLine, start, end int) []commentSourceLine {
	if start < 0 {
		start = 0
	}

	if end < start {
		end = start
	}

	result := make([]commentSourceLine, 0)
	offset := 0

	for _, line := range lines {
		lineStart := offset
		lineEnd := lineStart + len(line.text)

		if start <= lineEnd && end >= lineStart {
			fragmentStart := max(start, lineStart)
			fragmentEnd := min(end, lineEnd)

			text := ""
			if fragmentEnd > fragmentStart {
				text = line.text[fragmentStart-lineStart : fragmentEnd-lineStart]
			}

			result = append(result, commentSourceLine{
				filename: line.filename,
				line:     line.line,
				text:     text,
			})
		}

		offset = lineEnd + 1
	}

	return result
}

// CleanCommentPrefix 去除一行注释的前缀和多余空白
func CleanCommentPrefix(line string) string {
	line = strings.TrimLeft(line, " \t")
	for _, prefix := range []string{"//", "/*", "*", "*/"} {
		if after, ok := strings.CutPrefix(line, prefix); ok {
			line = after
			line = strings.TrimLeft(line, " \t")
		}
	}

	return line
}

// CleanBlockComment 去除块注释前缀和后缀
func CleanBlockComment(text string) string {
	text = strings.TrimSpace(text)
	if after, ok := strings.CutPrefix(text, "//"); ok {
		text = after
		text = strings.TrimSpace(text)
	}

	if after, ok := strings.CutPrefix(text, "/*"); ok {
		text = after
		text = strings.TrimSuffix(text, "*/")
		text = strings.TrimSpace(text)
	}

	return text
}

// extractDSLContent 提取 @TABLE/@RESULT 后第一个括号内的内容，支持前置括号
func extractDSLContent(text, keyword string) (string, error) {
	text = CleanBlockComment(text)

	idx, ok := findAnnotationKeyword(text, keyword)
	if !ok {
		return "", nil
	}

	searchStart := idx + len(keyword)
	afterKeyword := text[searchStart:]

	trimmedAfterKeyword := strings.TrimLeft(afterKeyword, " \t\r\n")
	if trimmedAfterKeyword == "" {
		return "", nil
	}

	if trimmedAfterKeyword[0] != '(' {
		return "", NewDSLAnnotationMissingOpeningParenError(keyword, text, searchStart+len(afterKeyword)-len(trimmedAfterKeyword))
	}

	start := searchStart + len(afterKeyword) - len(trimmedAfterKeyword)

	count := 0
	inString := false
	escaped := false

	for i := start; i < len(text); i++ {
		if inString {
			if escaped {
				escaped = false
				continue
			}

			switch text[i] {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}

			continue
		}

		switch text[i] {
		case '"':
			inString = true
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				return text[start+1 : i], nil
			}
		}
	}

	if inString {
		return "", NewDSLUnclosedStringError(text, len(text)-1)
	}

	return "", NewDSLAnnotationMissingClosingParenError(keyword, text, start)
}

func findAnnotationKeyword(text, keyword string) (int, bool) {
	offset := 0

	for {
		idx := strings.Index(text[offset:], keyword)
		if idx == -1 {
			return -1, false
		}

		idx += offset
		end := idx + len(keyword)

		if isAnnotationLineStart(text, idx) && (end == len(text) || isAnnotationBoundary(text[end])) {
			return idx, true
		}

		offset = end
	}
}

func isAnnotationLineStart(text string, idx int) bool {
	i := idx - 1
	for i >= 0 {
		switch text[i] {
		case ' ', '\t':
			i--
			continue
		case '\n', '\r':
			return true
		}

		break
	}

	if i < 0 {
		return true
	}

	if text[i] == '/' && i > 0 && text[i-1] == '/' {
		return isOnlyWhitespaceSinceLineStart(text, i-2)
	}

	if text[i] == '*' {
		if i > 0 && text[i-1] == '/' {
			return isOnlyWhitespaceSinceLineStart(text, i-2)
		}

		return isOnlyWhitespaceSinceLineStart(text, i-1)
	}

	return false
}

func isOnlyWhitespaceSinceLineStart(text string, idx int) bool {
	for i := idx; i >= 0; i-- {
		switch text[i] {
		case ' ', '\t':
			continue
		case '\n', '\r':
			return true
		default:
			return false
		}
	}

	return true
}

func isAnnotationBoundary(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '(':
		return true
	default:
		return false
	}
}

// parseDSL 解析所有注释中的注解（@TABLE/@RESULT），直接填充 info
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

		if _, ok := findAnnotationKeyword(text, "@TABLE"); ok {
			return parseTableDSL(structName, text, structFields)
		} else if _, ok := findAnnotationKeyword(text, "@RESULT"); ok {
			return parseResultDSL(structName, text, structFields)
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

	content, err := extractDSLContent(text, "@TABLE")
	if err != nil {
		return nil, errors.Trace(err)
	}

	if content == "" {
		return genTableInfoFromAST(structName, DSLObject{}, true, structFields)
	}

	content = strings.ReplaceAll(content, "\n", " ")
	content = strings.ReplaceAll(content, "\r", " ")
	content = strings.TrimSpace(content)

	tokens, err := Tokenize(content)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dsl, err := ParseDSL(tokens)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return genTableInfoFromAST(structName, dsl, true, structFields)
}

// parseResultDSL 解析 @RESULT DSL 并填充 meta
func parseResultDSL(
	structName string,
	text string,
	structFields map[string]struct{},
) (*tsq.TableInfo, error) {
	// 去除注释前缀
	text = CleanBlockComment(text)

	content, err := extractDSLContent(text, "@RESULT")
	if err != nil {
		return nil, errors.Trace(err)
	}

	if content == "" {
		return &tsq.TableInfo{IsResult: true}, nil
	}

	content = strings.ReplaceAll(content, "\n", " ")
	content = strings.ReplaceAll(content, "\r", " ")
	content = strings.TrimSpace(content)

	tokens, err := Tokenize(content)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dsl, err := ParseDSL(tokens)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return genTableInfoFromAST(structName, dsl, false, structFields)
}

// generateQueryList 生成查询索引列表，支持普通、集合、前缀等多种组合
func generateQueryList(meta *tsq.TableInfo) {
	queryMap := make(map[string]bool)

	for _, idx := range meta.IdxList {
		// 普通 query
		queryName := strings.Join(idx.Fields, "And")
		if !queryMap[queryName] {
			meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
				Name:       queryName,
				SourceName: idx.Name,
				Fields:     idx.Fields,
				IsSet:      false,
			})
			queryMap[queryName] = true
		}

		// set query
		setName := queryName + "In"
		if !queryMap[setName] {
			meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
				Name:       setName,
				SourceName: idx.Name,
				Fields:     idx.Fields,
				IsSet:      true,
			})
			queryMap[setName] = true
		}

		// 前缀索引
		for j := len(idx.Fields); j > 0; j-- {
			prefixQueryName := strings.Join(idx.Fields[:j], "And")
			if !queryMap[prefixQueryName] {
				meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
					Name:       prefixQueryName,
					SourceName: idx.Name,
					Fields:     idx.Fields[:j],
					IsSet:      false,
				})
				queryMap[prefixQueryName] = true
			}

			setName := prefixQueryName + "In"
			if !queryMap[setName] {
				meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
					Name:       setName,
					SourceName: idx.Name,
					Fields:     idx.Fields[:j],
					IsSet:      true,
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
					Name:       prefixQueryName,
					SourceName: ux.Name,
					Fields:     ux.Fields[:j],
					IsSet:      false,
				})
				queryMap[prefixQueryName] = true
			}

			setName := prefixQueryName + "In"
			if !queryMap[setName] {
				meta.QueryList = append(meta.QueryList, tsq.IndexInfo{
					Name:       setName,
					SourceName: ux.Name,
					Fields:     ux.Fields[:j],
					IsSet:      true,
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
