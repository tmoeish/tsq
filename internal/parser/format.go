package parser

import (
	"go/ast"
	goparser "go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const formatterIndent = "\t"

type annotationLocation struct {
	keyword   string
	content   string
	startLine int
	endLine   int
}

type textEdit struct {
	start       int
	end         int
	replacement string
}

// FormatPackage rewrites TSQ DSL annotations for one Go package and returns the
// files that changed.
func FormatPackage(packagePath string) ([]string, error) {
	buildPkg, err := loadSinglePackage(packagePath)
	if err != nil {
		return nil, err
	}

	changed := make([]string, 0, len(buildPkg.GoFiles))

	for _, filename := range buildPkg.GoFiles {
		if shouldSkipFile(filename) {
			continue
		}

		fullPath := filepath.Join(buildPkg.Dir, filename)

		fileChanged, err := formatSourceFile(fullPath)
		if err != nil {
			return nil, err
		}

		if fileChanged {
			changed = append(changed, fullPath)
		}
	}

	sort.Strings(changed)

	return changed, nil
}

func formatSourceFile(filename string) (bool, error) {
	src, err := os.ReadFile(filename)
	if err != nil {
		return false, err
	}

	fileSet := token.NewFileSet()

	file, err := goparser.ParseFile(fileSet, filename, src, goparser.ParseComments)
	if err != nil {
		return false, err
	}

	groups := collectStructAnnotationGroups(file, fileSet)
	edits := make([]textEdit, 0, len(groups))

	for _, group := range groups {
		replacement, err := formatCommentGroup(group)
		if err != nil {
			return false, err
		}

		start := fileSet.Position(group.Pos()).Offset

		end := fileSet.Position(group.End()).Offset
		if string(src[start:end]) == replacement {
			continue
		}

		edits = append(edits, textEdit{
			start:       start,
			end:         end,
			replacement: replacement,
		})
	}

	if len(edits) == 0 {
		return false, nil
	}

	sort.Slice(edits, func(i, j int) bool {
		return edits[i].start > edits[j].start
	})

	updated := append([]byte(nil), src...)
	for _, edit := range edits {
		updated = append(updated[:edit.start], append([]byte(edit.replacement), updated[edit.end:]...)...)
	}

	if err := writeFileAtomically(filename, updated); err != nil {
		return false, err
	}

	return true, nil
}

func collectStructAnnotationGroups(file *ast.File, fileSet *token.FileSet) []*ast.CommentGroup {
	commentMap := ast.NewCommentMap(fileSet, file, file.Comments)
	seen := make(map[token.Pos]struct{})
	groups := make([]*ast.CommentGroup, 0)

	addGroups := func(comments []*ast.CommentGroup) {
		for _, group := range comments {
			if group == nil {
				continue
			}

			if _, ok := seen[group.Pos()]; ok {
				continue
			}

			seen[group.Pos()] = struct{}{}
			groups = append(groups, group)
		}
	}

	for node, comments := range commentMap {
		switch n := node.(type) {
		case *ast.GenDecl:
			for _, spec := range n.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if ok && isStructType(typeSpec.Type) {
					addGroups(comments)
					break
				}
			}
		case *ast.TypeSpec:
			if isStructType(n.Type) {
				addGroups(comments)
			}
		}
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Pos() < groups[j].Pos()
	})

	return groups
}

func formatCommentGroup(group *ast.CommentGroup) (string, error) {
	if len(group.List) == 0 {
		return "", nil
	}

	if strings.HasPrefix(group.List[0].Text, "//") {
		return formatLineCommentGroup(group)
	}

	return formatBlockCommentGroup(group)
}

func formatLineCommentGroup(group *ast.CommentGroup) (string, error) {
	rawLines := make([]string, len(group.List))
	cleanedLines := make([]string, len(group.List))

	for i, comment := range group.List {
		rawLines[i] = comment.Text
		cleanedLines[i] = CleanCommentPrefix(comment.Text)
	}

	loc, ok, err := locateAnnotation(cleanedLines)
	if err != nil || !ok {
		return strings.Join(rawLines, "\n"), err
	}

	formattedLines, err := formatAnnotationLines(loc.keyword, loc.content, "")
	if err != nil {
		return "", err
	}
	formattedLines = padLineCommentPreformattedBlock(formattedLines)

	prefix := trimTrailingBlankCommentLines(rawLines[:loc.startLine])
	suffix := trimLeadingBlankCommentLines(rawLines[loc.endLine+1:])
	suffix = trimTrailingBlankCommentLines(suffix)

	replacement := make([]string, 0, len(prefix)+len(formattedLines)+len(suffix))
	replacement = append(replacement, prefix...)
	replacement = append(replacement, renderLineCommentLines(formattedLines)...)
	replacement = append(replacement, suffix...)

	return strings.Join(replacement, "\n"), nil
}

func formatBlockCommentGroup(group *ast.CommentGroup) (string, error) {
	raw := group.List[0].Text
	body := strings.TrimPrefix(raw, "/*")
	body = strings.TrimSuffix(body, "*/")

	bodyLines := strings.Split(body, "\n")

	cleanedLines := make([]string, len(bodyLines))
	for i, line := range bodyLines {
		cleanedLines[i] = CleanCommentPrefix(line)
	}

	loc, ok, err := locateAnnotation(cleanedLines)
	if err != nil || !ok {
		return raw, err
	}

	baseIndent := ""
	if idx := strings.Index(bodyLines[loc.startLine], loc.keyword); idx >= 0 {
		baseIndent = bodyLines[loc.startLine][:idx]
	}

	formattedLines, err := formatAnnotationLines(loc.keyword, loc.content, baseIndent)
	if err != nil {
		return "", err
	}

	prefix := trimTrailingBlankCommentLines(bodyLines[:loc.startLine])
	suffix := trimLeadingBlankCommentLines(bodyLines[loc.endLine+1:])
	suffix = trimTrailingBlankCommentLines(suffix)

	if len(prefix) > 0 && len(formattedLines) > 0 && isIndentedCommentLine(formattedLines[0]) {
		prefix = append(prefix, "")
	}

	if len(suffix) > 0 && len(formattedLines) > 0 && isIndentedCommentLine(formattedLines[len(formattedLines)-1]) {
		suffix = append([]string{""}, suffix...)
	}

	replacement := make([]string, 0, len(prefix)+len(formattedLines)+len(suffix))
	replacement = append(replacement, prefix...)
	replacement = append(replacement, formattedLines...)
	replacement = append(replacement, suffix...)

	if len(replacement) == 0 {
		return "/**/", nil
	}

	return "/*" + strings.Join(replacement, "\n") + "\n*/", nil
}

func trimLeadingBlankCommentLines(lines []string) []string {
	start := 0
	for start < len(lines) && CleanCommentPrefix(lines[start]) == "" {
		start++
	}

	return lines[start:]
}

func trimTrailingBlankCommentLines(lines []string) []string {
	end := len(lines)
	for end > 0 && CleanCommentPrefix(lines[end-1]) == "" {
		end--
	}

	return lines[:end]
}

func padLineCommentPreformattedBlock(lines []string) []string {
	if len(lines) < 3 || !isIndentedCommentLine(lines[1]) {
		return lines
	}

	padded := make([]string, 0, len(lines)+2)
	padded = append(padded, lines[0], "")
	padded = append(padded, lines[1:len(lines)-1]...)
	padded = append(padded, "", lines[len(lines)-1])

	return padded
}

func isIndentedCommentLine(line string) bool {
	return strings.TrimLeft(line, " \t") != line
}

func locateAnnotation(lines []string) (annotationLocation, bool, error) {
	text := strings.Join(lines, "\n")
	tableIdx, tableOK := findAnnotationKeyword(text, "@TABLE")
	resultIdx, resultOK := findAnnotationKeyword(text, "@RESULT")

	switch {
	case tableOK && (!resultOK || tableIdx < resultIdx):
		return locateAnnotationKeyword(text, lines, "@TABLE")
	case resultOK:
		return locateAnnotationKeyword(text, lines, "@RESULT")
	default:
		return annotationLocation{}, false, nil
	}
}

func locateAnnotationKeyword(
	text string,
	lines []string,
	keyword string,
) (annotationLocation, bool, error) {
	idx, ok := findAnnotationKeyword(text, keyword)
	if !ok {
		return annotationLocation{}, false, nil
	}

	searchStart := idx + len(keyword)
	afterKeyword := text[searchStart:]
	trimmedAfterKeyword := strings.TrimLeft(afterKeyword, " \t\r\n")

	endOffset := searchStart
	content := ""

	if trimmedAfterKeyword != "" {
		if trimmedAfterKeyword[0] != '(' {
			return annotationLocation{}, false, NewDSLMissingBracketError(text, searchStart+len(afterKeyword)-len(trimmedAfterKeyword))
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
					content = text[start+1 : i]
					endOffset = i + 1

					goto done
				}
			}
		}

		if inString {
			return annotationLocation{}, false, NewDSLUnclosedStringError(text, len(text)-1)
		}

		return annotationLocation{}, false, NewDSLMissingBracketError(text, start)
	}

done:
	startLine, _ := offsetToLineCol(lines, idx)

	endLine, _ := offsetToLineCol(lines, endOffset)
	if endLine > 0 {
		prevLineStart := lineStartOffset(lines, endLine)
		if endOffset == prevLineStart {
			endLine--
		}
	}

	return annotationLocation{
		keyword:   keyword,
		content:   content,
		startLine: startLine,
		endLine:   endLine,
	}, true, nil
}

func offsetToLineCol(lines []string, offset int) (int, int) {
	remaining := offset

	for i, line := range lines {
		lineLen := len(line)
		if remaining <= lineLen {
			return i, remaining
		}

		remaining -= lineLen
		if i < len(lines)-1 {
			if remaining == 0 {
				return i + 1, 0
			}

			remaining--
		}
	}

	last := len(lines) - 1
	if last < 0 {
		return 0, 0
	}

	return last, len(lines[last])
}

func lineStartOffset(lines []string, line int) int {
	offset := 0
	for i := range line {
		offset += len(lines[i]) + 1
	}

	return offset
}

func formatAnnotationLines(keyword, content, baseIndent string) ([]string, error) {
	content = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(content, "\r", " "), "\n", " "))
	if content == "" {
		return []string{baseIndent + keyword}, nil
	}

	tokens, err := Tokenize(content)
	if err != nil {
		return nil, err
	}

	obj, err := ParseDSL(tokens)
	if err != nil {
		return nil, err
	}

	if inline, ok := renderRootInline(obj); ok {
		return []string{baseIndent + keyword + "(" + inline + ")"}, nil
	}

	indent := baseIndent + formatterIndent

	lines := []string{baseIndent + keyword + "("}
	for _, key := range orderedRootKeys(obj) {
		lines = append(lines, renderRootEntry(key, obj[key], indent)...)
	}

	lines = append(lines, baseIndent+")")

	return lines, nil
}

func renderRootInline(obj DSLObject) (string, bool) {
	if len(obj) != 1 {
		return "", false
	}

	keys := orderedRootKeys(obj)
	if len(keys) != 1 {
		return "", false
	}

	value, ok := renderInlineValue(obj[keys[0]])
	if !ok {
		return "", false
	}

	entry := renderKeyValue(keys[0], obj[keys[0]], value)
	if len(entry) > 48 {
		return "", false
	}

	return entry, true
}

func renderRootEntry(key string, node DSLNode, indent string) []string {
	switch value := node.(type) {
	case DSLArray:
		if isObjectArray(value) {
			lines := []string{indent + key + "=["}
			itemIndent := indent + formatterIndent

			for _, item := range value {
				obj, _ := item.(DSLObject)
				lines = append(lines, itemIndent+renderInlineObject(obj)+",")
			}

			lines = append(lines, indent+"],")

			return lines
		}
	}

	inline, _ := renderInlineValue(node)

	return []string{indent + renderKeyValue(key, node, inline) + ","}
}

func renderInlineObject(obj DSLObject) string {
	parts := make([]string, 0, len(obj))
	for _, key := range orderedObjectKeys(obj) {
		inline, ok := renderInlineValue(obj[key])
		if !ok {
			inline = "null"
		}

		parts = append(parts, renderKeyValue(key, obj[key], inline))
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

func renderKeyValue(key string, node DSLNode, renderedValue string) string {
	if b, ok := node.(DSLBool); ok && bool(b) {
		return key
	}

	return key + "=" + renderedValue
}

func renderInlineValue(node DSLNode) (string, bool) {
	switch value := node.(type) {
	case DSLString:
		return strconv.Quote(string(value)), true
	case DSLBool:
		if bool(value) {
			return "", true
		}

		return "false", true
	case DSLNumber:
		return strconv.FormatFloat(float64(value), 'f', -1, 64), true
	case DSLArray:
		items := make([]string, 0, len(value))
		for _, item := range value {
			rendered, ok := renderInlineValue(item)
			if !ok {
				return "", false
			}

			items = append(items, rendered)
		}

		return "[" + strings.Join(items, ", ") + "]", true
	case DSLObject:
		return renderInlineObject(value), true
	default:
		return "", false
	}
}

func orderedRootKeys(obj DSLObject) []string {
	order := []string{
		"name",
		"pk",
		"version",
		"created_at",
		"updated_at",
		"deleted_at",
		"ux",
		"idx",
		"kw",
	}

	return orderedKeys(obj, order)
}

func orderedObjectKeys(obj DSLObject) []string {
	return orderedKeys(obj, []string{"name", "fields"})
}

func orderedKeys(obj DSLObject, preferred []string) []string {
	keys := make([]string, 0, len(obj))
	seen := make(map[string]struct{}, len(obj))

	for _, key := range preferred {
		if _, ok := obj[key]; ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}

	extras := make([]string, 0, len(obj)-len(keys))
	for key := range obj {
		if _, ok := seen[key]; ok {
			continue
		}
		extras = append(extras, key)
	}

	sort.Strings(extras)

	return append(keys, extras...)
}

func isObjectArray(arr DSLArray) bool {
	if len(arr) == 0 {
		return false
	}

	for _, item := range arr {
		if _, ok := item.(DSLObject); !ok {
			return false
		}
	}

	return true
}

func renderLineCommentLines(lines []string) []string {
	rendered := make([]string, 0, len(lines))
	for _, line := range lines {
		switch {
		case line == "":
			rendered = append(rendered, "//")
		case strings.HasPrefix(line, "\t"):
			rendered = append(rendered, "//"+line)
		default:
			rendered = append(rendered, "// "+line)
		}
	}

	return rendered
}

func writeFileAtomically(filename string, src []byte) error {
	perm := os.FileMode(0o644)
	if info, err := os.Stat(filename); err == nil {
		perm = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return err
	}

	dir := filepath.Dir(filename)
	pattern := "." + filepath.Base(filename) + ".tmp-*"

	tmpFile, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return err
	}

	tmpName := tmpFile.Name()

	defer func() {
		_ = os.Remove(tmpName)
	}()

	if err := tmpFile.Chmod(perm); err != nil {
		_ = tmpFile.Close()
		return err
	}

	if _, err := tmpFile.Write(src); err != nil {
		_ = tmpFile.Close()
		return err
	}

	if err := tmpFile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpName, filename)
}
