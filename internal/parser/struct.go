package parser

import (
	"container/list"
	"fmt"
	"go/ast"
	"log/slog"
	"sort"
	"strings"
	"unicode"

	"github.com/juju/errors"
	"github.com/serenize/snaker"
	"github.com/tmoeish/tsq"
)

var reservedImportAliases = map[string]struct{}{
	"context": {},
	"tsq":     {},
	"errors":  {},
	"tsqsql":  {},
	"tsqtime": {},
}

// StructInfo 表示一个解析后的结构体信息
type StructInfo struct {
	*tsq.StructInfo

	embeddedTypes     map[tsq.TypeInfo]bool // 嵌入的结构体类型
	embeddedResolving bool                  // 嵌入字段是否正在解析
	embeddedResolved  bool                  // 嵌入字段是否已解析
}

// parseStructDeclaration 解析单个结构体定义
func parseStructDeclaration(
	packageAliases map[string]tsq.PackageInfo, // 包别名映射
	currentPkg tsq.PackageInfo, // 当前包信息
	structName string, // 结构体名称
	structType *ast.StructType, // AST 结构体类型
	structMap map[tsq.TypeInfo]*StructInfo, // 已解析的结构体映射
	parsedPackages map[tsq.PackageInfo]bool, // 已解析的包
	pendingPackages *list.List, // 待解析的包列表
) error {
	typeInfo := tsq.TypeInfo{
		Package:  currentPkg,
		TypeName: structName,
	}

	slog.Debug("parsing struct", "typeInfo", typeInfo)

	// 解析嵌入字段
	embeddedTypes, err := parseEmbeddedFields(packageAliases, currentPkg, structType)
	if err != nil {
		return errors.Trace(err)
	}

	// 将嵌入字段的包添加到待解析列表
	for embeddedType := range embeddedTypes {
		if embeddedType.Package == currentPkg {
			continue
		}

		if _, alreadyParsed := parsedPackages[embeddedType.Package]; !alreadyParsed {
			pendingPackages.PushBack(embeddedType.Package.Path)

			parsedPackages[embeddedType.Package] = true
		}
	}

	// 解析具名字段
	fieldMap, err := parseNamedFields(packageAliases, currentPkg, structType)
	if err != nil {
		return errors.Trace(err)
	}

	// 创建结构体对象
	structMap[typeInfo] = &StructInfo{
		StructInfo: &tsq.StructInfo{ // 初始化表元数据
			TypeInfo: typeInfo,
			FieldMap: fieldMap,
			Recv:     genRecv(structName),
		},
		embeddedTypes:    embeddedTypes,
		embeddedResolved: len(embeddedTypes) == 0, // 没有嵌入字段则标记为已解析
	}

	return nil
}

// resolveImportDependencies 解析结构体的导入依赖
func (s *StructInfo) resolveImportDependencies() {
	// 收集所有需要导入的包
	requiredPackages := s.collectRequiredPackages()

	// 处理包名冲突
	s.ImportMap = s.resolvePackageNameConflicts(requiredPackages)
}

// collectRequiredPackages 收集所有需要导入的包
func (s *StructInfo) collectRequiredPackages() map[tsq.PackageInfo]bool {
	packages := make(map[tsq.PackageInfo]bool)

	for _, field := range s.FieldMap {
		fieldPkg := field.Type.Package

		// 跳过原始类型和当前包的类型
		if fieldPkg.Path == "" || fieldPkg == s.TypeInfo.Package {
			continue
		}

		packages[fieldPkg] = true
	}

	return packages
}

// resolvePackageNameConflicts 解决包名冲突
func (s *StructInfo) resolvePackageNameConflicts(
	packages map[tsq.PackageInfo]bool,
) map[string]string {
	// 按包名分组
	nameGroups := make(map[string][]string)
	for pkg := range packages {
		nameGroups[pkg.Name] = append(nameGroups[pkg.Name], pkg.Path)
	}

	// 生成最终的导入映射
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

// resolveFieldsInfo 解析字段信息，设置正确的包名并排序
func (s *StructInfo) resolveFieldsInfo() {
	// 更新字段的包名信息
	s.updateFieldPackageNames()

	// 从 FieldMap 构建 Fields
	s.buildFieldList()

	// 对字段列表进行排序
	s.sortFieldList()
}

// updateFieldPackageNames 更新字段的包名信息
func (s *StructInfo) updateFieldPackageNames() {
	for fieldName, field := range s.FieldMap {
		fieldPkg := &field.Type.Package

		if fieldPkg.Path == "" || *fieldPkg == s.TypeInfo.Package {
			// 原始类型或当前包的类型，清空包名
			fieldPkg.Name = ""
		} else {
			// 外部包的类型，使用导入映射中的包名
			fieldPkg.Name = s.ImportMap[fieldPkg.Path]
		}

		// 更新 FieldMap 中的字段
		s.FieldMap[fieldName] = field
	}
}

// buildFieldList rebuilds Fields from FieldMap using a fresh slice.
func (s *StructInfo) buildFieldList() {
	fields := make([]tsq.FieldInfo, 0, len(s.FieldMap))

	for _, field := range s.FieldMap {
		fields = append(fields, field)
	}

	s.Fields = fields
}

// sortFieldList 对字段列表进行排序
func (s *StructInfo) sortFieldList() {
	sort.Slice(s.Fields, func(i, j int) bool {
		return s.Fields[i].Name < s.Fields[j].Name
	})
}

// genRecv 从类型名生成接收器名称，通过连接各部分的首字母
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
