package parser

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// MigratePackage rewrites legacy @TABLE / @RESULT annotations as //tsq: directive
// lines and returns the files it changed.
//
// It exists because v5 changed the annotation syntax, and a project with a few
// dozen annotated structs should not have to convert them by hand. It reads the
// old DSL with the old parser rather than by pattern-matching text, so anything
// the generator used to accept converts exactly.
func MigratePackage(packagePath string) ([]string, error) {
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

		fileChanged, err := migrateSourceFile(fullPath)
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

// migrateSourceFile converts every legacy annotation in one file.
func migrateSourceFile(filename string) (bool, error) {
	src, err := os.ReadFile(filename)
	if err != nil {
		return false, err
	}

	fileSet := token.NewFileSet()

	file, err := parser.ParseFile(fileSet, filename, src, parser.ParseComments)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", filename, err)
	}

	groups := collectStructAnnotationGroups(file, fileSet)
	if len(groups) == 0 {
		return false, nil
	}

	lines := strings.Split(string(src), "\n")

	// Rewrite from the bottom so earlier line numbers stay valid.
	type replacement struct {
		startLine  int
		endLine    int
		directives []string
	}

	replacements := make([]replacement, 0, len(groups))

	for _, group := range groups {
		text := commentGroupText(group)

		keyword := ""
		if _, ok := findAnnotationKeyword(text, "@TABLE"); ok {
			keyword = "@TABLE"
		} else if _, ok := findAnnotationKeyword(text, "@RESULT"); ok {
			keyword = "@RESULT"
		}

		if keyword == "" {
			continue
		}

		directives, err := directivesForAnnotation(text, keyword)
		if err != nil {
			return false, fmt.Errorf("%s: %w", filename, err)
		}

		start := fileSet.Position(group.Pos()).Line
		end := fileSet.Position(group.End()).Line

		replacements = append(replacements, replacement{
			startLine:  start,
			endLine:    end,
			directives: directives,
		})
	}

	if len(replacements) == 0 {
		return false, nil
	}

	sort.Slice(replacements, func(i, j int) bool {
		return replacements[i].startLine > replacements[j].startLine
	})

	for _, r := range replacements {
		// A comment group can hold prose above the annotation; keep those lines.
		prose := keptProseLines(lines[r.startLine-1 : r.endLine])

		out := append([]string{}, lines[:r.startLine-1]...)
		out = append(out, prose...)
		out = append(out, r.directives...)
		out = append(out, lines[r.endLine:]...)
		lines = out
	}

	updated := strings.Join(lines, "\n")
	if updated == string(src) {
		return false, nil
	}

	return true, writeFileAtomically(filename, []byte(updated))
}

// keptProseLines returns the comment lines that precede the annotation keyword.
func keptProseLines(groupLines []string) []string {
	kept := make([]string, 0, len(groupLines))

	for _, line := range groupLines {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "@TABLE") || strings.Contains(trimmed, "@RESULT") {
			break
		}

		kept = append(kept, line)
	}

	// A trailing bare "//" only separated prose from the annotation block.
	for len(kept) > 0 && strings.TrimSpace(kept[len(kept)-1]) == "//" {
		kept = kept[:len(kept)-1]
	}

	return kept
}

// commentGroupText joins a comment group with its markers stripped.
func commentGroupText(group *ast.CommentGroup) string {
	var lines []string
	for _, comment := range group.List {
		lines = append(lines, CleanCommentPrefix(comment.Text))
	}

	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// directivesForAnnotation renders one legacy annotation as directive lines.
func directivesForAnnotation(text, keyword string) ([]string, error) {
	content, err := extractDSLContent(CleanBlockComment(text), keyword)
	if err != nil {
		return nil, err
	}

	obj := DSLObject{}

	if strings.TrimSpace(content) != "" {
		normalized := strings.NewReplacer("\n", " ", "\r", " ").Replace(content)

		tokens, err := Tokenize(strings.TrimSpace(normalized))
		if err != nil {
			return nil, err
		}

		obj, err = ParseDSL(tokens)
		if err != nil {
			return nil, err
		}
	}

	if keyword == "@RESULT" {
		return renderResultDirectives(obj), nil
	}

	return renderTableDirectives(obj)
}

func renderResultDirectives(obj DSLObject) []string {
	directives := []string{DirectivePrefix + "result"}

	if search := dslStringList(obj["search"]); len(search) > 0 {
		directives = append(directives, DirectivePrefix+"search "+strings.Join(search, ","))
	}

	return directives
}

func renderTableDirectives(obj DSLObject) ([]string, error) {
	table := DirectivePrefix + "table"

	if name, ok := obj["name"].(DSLString); ok && string(name) != "" {
		table += " name=" + string(name)
	}

	if pk, ok := obj["pk"].(DSLString); ok && string(pk) != "" {
		field, auto, err := parsePrimaryKeyDSL(string(pk))
		if err != nil {
			return nil, err
		}

		table += " pk=" + field
		if !auto {
			table += " assigned"
		}
	}

	directives := []string{table}

	var managed []string

	for _, role := range []struct {
		key          string
		defaultField string
	}{
		{"version", DefaultVersionField},
		{"created_at", DefaultCreatedAtField},
		{"updated_at", DefaultUpdatedAtField},
		{"deleted_at", DefaultDeletedAtField},
	} {
		value, present := obj[role.key]
		if !present {
			continue
		}

		switch v := value.(type) {
		case DSLBool:
			if bool(v) {
				managed = append(managed, role.key)
			}
		case DSLString:
			if string(v) == role.defaultField {
				managed = append(managed, role.key)
			} else {
				managed = append(managed, role.key+"="+string(v))
			}
		}
	}

	if len(managed) > 0 {
		directives = append(directives, DirectivePrefix+"managed "+strings.Join(managed, " "))
	}

	directives = append(directives, renderIndexDirectives(obj["ux"], "unique")...)
	directives = append(directives, renderIndexDirectives(obj["idx"], "index")...)

	if search := dslStringList(obj["search"]); len(search) > 0 {
		directives = append(directives, DirectivePrefix+"search "+strings.Join(search, ","))
	}

	return directives, nil
}

func renderIndexDirectives(node DSLNode, name string) []string {
	arr, ok := node.(DSLArray)
	if !ok {
		return nil
	}

	directives := make([]string, 0, len(arr))

	for _, entry := range arr {
		obj, ok := entry.(DSLObject)
		if !ok {
			continue
		}

		fields := dslStringList(obj["fields"])
		if len(fields) == 0 {
			continue
		}

		line := DirectivePrefix + name + " " + strings.Join(fields, ",")

		// Only an explicitly named index keeps its name: a derived one is
		// derived the same way by the new parser.
		if idxName, ok := obj["name"].(DSLString); ok && string(idxName) != "" {
			line += " name=" + string(idxName)
		}

		directives = append(directives, line)
	}

	return directives
}

func dslStringList(node DSLNode) []string {
	arr, ok := node.(DSLArray)
	if !ok {
		return nil
	}

	values := make([]string, 0, len(arr))

	for _, entry := range arr {
		if s, ok := entry.(DSLString); ok {
			values = append(values, string(s))
		}
	}

	return values
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
