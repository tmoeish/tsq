package parser

import (
	"container/list"
	"fmt"
	"go/ast"
	"log/slog"
	"sort"
	"strings"
	"unicode"

	"github.com/serenize/snaker"

	"github.com/tmoeish/tsq/v4/internal/genmodel"
)

var reservedImportAliases = map[string]struct{}{
	"context": {},
	"tsq":     {},
	"errors":  {},
	"tsqsql":  {},
	"tsqtime": {},
}

// StructInfo is a parsed struct declaration plus its embedded-type resolution state.
type StructInfo struct {
	*genmodel.StructInfo

	embeddedTypes     map[genmodel.TypeInfo]bool // embedded struct types
	embeddedResolving bool                       // embedded fields are being resolved (cycle guard)
	embeddedResolved  bool                       // embedded fields have been resolved
}

// parseStructDeclaration parses one struct declaration.
func parseStructDeclaration(
	packageAliases map[string]genmodel.PackageInfo, // import alias -> package
	currentPkg genmodel.PackageInfo, // package being parsed
	structName string, // struct name
	structType *ast.StructType, // struct AST node
	structMap map[genmodel.TypeInfo]*StructInfo, // structs parsed so far
	parsedPackages map[genmodel.PackageInfo]bool, // packages parsed so far
	pendingPackages *list.List, // packages still to parse
) error {
	typeInfo := genmodel.TypeInfo{
		Package:  currentPkg,
		TypeName: structName,
	}

	slog.Debug("parsing struct", "typeInfo", typeInfo)

	// Parse embedded fields.
	embeddedTypes, err := parseEmbeddedFields(packageAliases, currentPkg, structType)
	if err != nil {
		return err
	}

	// Queue the embedded fields' packages for parsing.
	for embeddedType := range embeddedTypes {
		if embeddedType.Package == currentPkg {
			continue
		}

		if _, alreadyParsed := parsedPackages[embeddedType.Package]; !alreadyParsed {
			pendingPackages.PushBack(embeddedType.Package.Path)

			parsedPackages[embeddedType.Package] = true
		}
	}

	// Parse named fields.
	fieldMap, err := parseNamedFields(packageAliases, currentPkg, structType)
	if err != nil {
		return err
	}

	// Build the struct info.
	structMap[typeInfo] = &StructInfo{
		StructInfo: &genmodel.StructInfo{ // table metadata is filled in later
			TypeInfo: typeInfo,
			FieldMap: fieldMap,
			Recv:     genRecv(structName),
		},
		embeddedTypes:    embeddedTypes,
		embeddedResolved: len(embeddedTypes) == 0, // nothing to resolve without embedded fields
	}

	return nil
}

// resolveImportDependencies computes the imports a struct's fields need.
func (s *StructInfo) resolveImportDependencies() {
	// Collect every package that must be imported.
	requiredPackages := s.collectRequiredPackages()

	// Resolve package name conflicts.
	s.ImportMap = s.resolvePackageNameConflicts(requiredPackages)
}

// collectRequiredPackages collects every package that must be imported.
func (s *StructInfo) collectRequiredPackages() map[genmodel.PackageInfo]bool {
	packages := make(map[genmodel.PackageInfo]bool)

	for _, field := range s.FieldMap {
		fieldPkg := field.Type.Package

		// Skip primitive types and types from the current package.
		if fieldPkg.Path == "" || fieldPkg == s.TypeInfo.Package {
			continue
		}

		packages[fieldPkg] = true
	}

	return packages
}

// resolvePackageNameConflicts assigns unique aliases to packages sharing a name.
func (s *StructInfo) resolvePackageNameConflicts(
	packages map[genmodel.PackageInfo]bool,
) map[string]string {
	// Group by package name.
	nameGroups := make(map[string][]string)
	for pkg := range packages {
		nameGroups[pkg.Name] = append(nameGroups[pkg.Name], pkg.Path)
	}

	// Produce the final import map.
	imports := make(map[string]string)
	usedAliases := cloneAliasSet(reservedImportAliases)

	packageNames := make([]string, 0, len(nameGroups))
	for packageName := range nameGroups {
		packageNames = append(packageNames, packageName)
	}

	sort.Strings(packageNames)

	for _, packageName := range packageNames {
		paths := nameGroups[packageName]
		sort.Strings(paths)

		for _, importPath := range paths {
			alias := nextAvailableImportAlias(packageName, usedAliases)
			imports[importPath] = alias
		}
	}

	return imports
}

func cloneAliasSet(source map[string]struct{}) map[string]struct{} {
	cloned := make(map[string]struct{}, len(source))
	for alias := range source {
		cloned[alias] = struct{}{}
	}

	return cloned
}

func nextAvailableImportAlias(base string, usedAliases map[string]struct{}) string {
	if base == "" {
		base = "pkg"
	}

	if _, exists := usedAliases[base]; !exists {
		usedAliases[base] = struct{}{}
		return base
	}

	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s%d", base, i)
		if _, exists := usedAliases[candidate]; exists {
			continue
		}

		usedAliases[candidate] = struct{}{}

		return candidate
	}
}

// resolveFieldsInfo finalizes field package names and sorts the field list.
func (s *StructInfo) resolveFieldsInfo() {
	// Update field package names.
	s.updateFieldPackageNames()

	// Build Fields from FieldMap.
	s.buildFieldList()

	// Sort the field list.
	s.sortFieldList()
}

// updateFieldPackageNames rewrites field package names using the import map.
func (s *StructInfo) updateFieldPackageNames() {
	for fieldName, field := range s.FieldMap {
		fieldPkg := &field.Type.Package

		if fieldPkg.Path == "" || *fieldPkg == s.TypeInfo.Package {
			// Primitive or current-package type: no package qualifier.
			fieldPkg.Name = ""
		} else {
			// External type: use the alias from the import map.
			fieldPkg.Name = s.ImportMap[fieldPkg.Path]
		}

		// Write the updated field back into FieldMap.
		s.FieldMap[fieldName] = field
	}
}

// buildFieldList rebuilds Fields from FieldMap using a fresh slice.
func (s *StructInfo) buildFieldList() {
	fields := make([]genmodel.FieldInfo, 0, len(s.FieldMap))

	for _, field := range s.FieldMap {
		fields = append(fields, field)
	}

	s.Fields = fields
}

// sortFieldList sorts fields into a stable output order.
func (s *StructInfo) sortFieldList() {
	sort.Slice(s.Fields, func(i, j int) bool {
		return s.Fields[i].Name < s.Fields[j].Name
	})
}

// genRecv derives a receiver name from a type name by joining the initials of its words.
func genRecv(typeName string) string {
	parts := strings.Split(snaker.CamelToSnake(typeName), "_")
	result := make([]rune, 0, len(parts))

	for _, part := range parts {
		if len(part) == 0 {
			continue
		}

		runes := []rune(part)
		result = append(result, unicode.ToLower(runes[0]))
	}

	return string(result)
}
