package parser

import (
	"container/list"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"path"
	"reflect"
	"strings"

	"log/slog"

	"github.com/juju/errors"
	"github.com/tmoeish/tsq"
)

// ParseResult 解析结果
type ParseResult struct {
	Structs   []*StructInfo // 解析到的结构体列表
	Directory string        // 目标目录路径
}

// Parse 解析指定路径的包，返回所有带有表注解的结构体和目录路径
func Parse(packagePath string) ([]*tsq.StructInfo, string, error) {
	result, err := parsePackage(packagePath)
	if err != nil {
		return nil, "", errors.Annotatef(err, "failed to parse package %s", packagePath)
	}

	list := make([]*tsq.StructInfo, len(result.Structs))
	for i, internal := range result.Structs {
		list[i] = internal.StructInfo
	}

	return list, result.Directory, nil
}

// parsePackage 解析包的完整流程
func parsePackage(packagePath string) (*ParseResult, error) {
	parseState := &ParseState{
		structMap:       make(map[tsq.TypeInfo]*StructInfo),
		parsedPackages:  make(map[tsq.PackageInfo]bool),
		pendingPackages: list.New(),
	}

	if err := parseState.parsePackagesRecursively(packagePath); err != nil {
		return nil, errors.Annotate(err, "failed to recursively parse package")
	}

	if err := parseState.resolveAllEmbeddedFields(); err != nil {
		return nil, errors.Annotate(err, "failed to parse embedded fields")
	}

	packageInfo, err := parseState.getPackageInfo(packagePath)
	if err != nil {
		return nil, errors.Annotate(err, "failed to get package info")
	}

	if err := parseState.parseTableMetadata(packageInfo); err != nil {
		return nil, errors.Annotate(err, "failed to parse table metadata")
	}

	result, err := parseState.filterAndProcessResults(packagePath)
	if err != nil {
		return nil, errors.Annotate(err, "failed to filter and process parse results")
	}

	return result, nil
}

// ParseState 包含解析过程中的状态信息
type ParseState struct {
	structMap       map[tsq.TypeInfo]*StructInfo // 已解析的结构体映射
	parsedPackages  map[tsq.PackageInfo]bool     // 已解析的包集合
	pendingPackages *list.List                   // 待解析的包队列
}

// parsePackagesRecursively 递归解析包
func (ps *ParseState) parsePackagesRecursively(packagePath string) error {
	ps.pendingPackages.PushBack(packagePath)

	for ps.pendingPackages.Len() > 0 {
		element := ps.pendingPackages.Front()
		ps.pendingPackages.Remove(element)
		currentPath := element.Value.(string)

		if err := ps.parseSinglePackage(currentPath); err != nil {
			return errors.Annotatef(err, "failed to recursively parse package %s", currentPath)
		}
	}

	return nil
}

// resolveAllEmbeddedFields 解析所有结构体的嵌入字段
func (ps *ParseState) resolveAllEmbeddedFields() error {
	for _, structInfo := range ps.structMap {
		if err := resolveEmbeddedFields(structInfo, ps.structMap); err != nil {
			return errors.Annotatef(err, "failed to parse embedded fields: %v", structInfo.TypeInfo)
		}
	}

	return nil
}

// getPackageInfo 获取包信息
func (ps *ParseState) getPackageInfo(packagePath string) (tsq.PackageInfo, error) {
	buildPkg, err := build.Default.Import(packagePath, "", 0)
	if err != nil {
		return tsq.PackageInfo{}, errors.Annotatef(err, "failed to process directory: %s", packagePath)
	}

	return tsq.PackageInfo{
		Path: buildPkg.ImportPath,
		Name: buildPkg.Name,
	}, nil
}

// parseTableMetadata 解析表元数据
func (ps *ParseState) parseTableMetadata(pkg tsq.PackageInfo) error {
	buildPkg, err := build.Default.Import(pkg.Path, "", 0)
	if err != nil {
		return errors.Annotatef(err, "failed to process directory: %s", pkg.Path)
	}

	fileSet := token.NewFileSet()

	for _, filename := range buildPkg.GoFiles {
		if shouldSkipFile(filename) {
			continue
		}

		fullPath := path.Join(buildPkg.Dir, filename)

		file, err := parser.ParseFile(fileSet, fullPath, nil, parser.ParseComments)
		if err != nil {
			return errors.Annotatef(err, "failed to parse file: %s", fullPath)
		}

		if err := ps.processFileComments(file, fileSet, pkg); err != nil {
			return errors.Annotatef(err, "failed to process file comments: %s", fullPath)
		}
	}

	return nil
}

// shouldSkipFile 判断是否应该跳过文件
func shouldSkipFile(filename string) bool {
	if strings.HasSuffix(filename, TSQFileSuffix) {
		return true
	}

	if !strings.HasSuffix(filename, GoFileSuffix) {
		return true
	}

	return false
}

// processFileComments 处理文件中的注释
func (ps *ParseState) processFileComments(
	file *ast.File,
	fileSet *token.FileSet,
	pkg tsq.PackageInfo,
) error {
	commentMap := ast.NewCommentMap(fileSet, file, file.Comments)

	for node, comments := range commentMap {
		switch n := node.(type) {
		case *ast.GenDecl:
			if err := ps.processGenDecl(n, comments, pkg); err != nil {
				return err
			}
		case *ast.TypeSpec:
			if err := ps.processTypeSpec(n, comments, pkg); err != nil {
				return err
			}
		default:
			slog.Debug("skip node type", "type", reflect.TypeOf(node))
		}
	}

	return nil
}

// processGenDecl 处理通用声明节点
func (ps *ParseState) processGenDecl(
	genDecl *ast.GenDecl,
	comments []*ast.CommentGroup,
	pkg tsq.PackageInfo,
) error {
	for _, spec := range genDecl.Specs {
		typeSpec, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}

		if !isStructType(typeSpec.Type) {
			continue
		}

		if err := ps.processStructTypeSpec(typeSpec, comments, pkg); err != nil {
			return err
		}
	}

	return nil
}

// processTypeSpec 处理类型声明节点
func (ps *ParseState) processTypeSpec(
	typeSpec *ast.TypeSpec,
	comments []*ast.CommentGroup,
	pkg tsq.PackageInfo,
) error {
	if !isStructType(typeSpec.Type) {
		return nil
	}

	return ps.processStructTypeSpec(typeSpec, comments, pkg)
}

// processStructTypeSpec 处理结构体类型声明
func (ps *ParseState) processStructTypeSpec(
	typeSpec *ast.TypeSpec,
	comments []*ast.CommentGroup,
	pkg tsq.PackageInfo,
) error {
	structName := typeSpec.Name.Name
	typeInfo := tsq.TypeInfo{Package: pkg, TypeName: structName}

	structInfo, exists := ps.structMap[typeInfo]
	if !exists {
		return nil
	}
	// 构建字段集合
	fields := make(map[string]struct{})
	for name := range structInfo.FieldMap {
		fields[name] = struct{}{}
	}

	tableMeta, err := ParseTableInfo(structName, comments, fields)
	if err != nil {
		return err
	}

	if tableMeta != nil {
		structInfo.TableInfo = tableMeta
	}

	return nil
}

// isStructType 判断是否为结构体类型
func isStructType(typeExpr ast.Expr) bool {
	_, ok := typeExpr.(*ast.StructType)
	return ok
}

// filterAndProcessResults 过滤并处理解析结果
func (ps *ParseState) filterAndProcessResults(packagePath string) (*ParseResult, error) {
	buildPkg, err := build.Default.Import(packagePath, "", 0)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to process directory: %s", packagePath)
	}

	var results []*StructInfo

	for _, structInfo := range ps.structMap {
		if structInfo.TableInfo == nil {
			continue
		}

		structInfo.resolveImportDependencies()
		structInfo.resolveFieldsInfo()
		results = append(results, structInfo)
	}

	return &ParseResult{
		Structs:   results,
		Directory: buildPkg.Dir,
	}, nil
}

// parseSinglePackage 解析单个包
func (ps *ParseState) parseSinglePackage(packagePath string) error {
	buildPkg, err := build.Default.Import(packagePath, "", 0)
	if err != nil {
		return errors.Annotatef(err, "failed to process pkg: %s", packagePath)
	}

	pkg := tsq.PackageInfo{
		Path: buildPkg.ImportPath,
		Name: buildPkg.Name,
	}

	slog.Debug("parsing package", "packagePath", packagePath)

	fileSet := token.NewFileSet()

	for _, filename := range buildPkg.GoFiles {
		if shouldSkipFile(filename) {
			continue
		}

		fullPath := path.Join(buildPkg.Dir, filename)
		slog.Debug("parsing file", "fullPath", fullPath)

		file, err := parser.ParseFile(fileSet, fullPath, nil, parser.ParseComments)
		if err != nil {
			return errors.Annotatef(err, "failed to parse file: %s", fullPath)
		}

		packageAliases := parsePackageAliases(file)

		err = ps.parseStructDeclarations(file, packageAliases, pkg)
		if err != nil {
			return errors.Annotatef(err, "failed to parse struct declarations: %s", fullPath)
		}
	}

	return nil
}

// parseStructDeclarations 解析文件中的结构体声明
func (ps *ParseState) parseStructDeclarations(
	file *ast.File,
	packageAliases map[string]tsq.PackageInfo,
	pkg tsq.PackageInfo,
) error {
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}

		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}

			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}

			err := parseStructDeclaration(
				packageAliases, pkg, typeSpec.Name.Name, structType,
				ps.structMap, ps.parsedPackages, ps.pendingPackages,
			)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// parsePackageAliases 解析文件中的包别名
func parsePackageAliases(file *ast.File) map[string]tsq.PackageInfo {
	packageAliases := make(map[string]tsq.PackageInfo)

	for _, importSpec := range file.Imports {
		importPath := strings.Trim(importSpec.Path.Value, `"`)

		pkg, err := getPackageInfo(importPath)
		if err != nil {
			// 这里选择跳过错误包，或可根据需要向上返回错误
			continue
		}

		if importSpec.Name != nil {
			// 显式别名
			packageAliases[importSpec.Name.Name] = pkg
		} else {
			// 使用包名作为别名
			packageAliases[pkg.Name] = pkg
		}
	}

	return packageAliases
}

// getPackageInfo 根据导入路径获取包信息
func getPackageInfo(importPath string) (tsq.PackageInfo, error) {
	buildPkg, err := build.Default.Import(importPath, "", 0)
	if err != nil {
		return tsq.PackageInfo{}, errors.Annotatef(err, "failed to get package: %s", importPath)
	}

	return tsq.PackageInfo{
		Path: buildPkg.ImportPath,
		Name: buildPkg.Name,
	}, nil
}

// resolveEmbeddedFields 解析嵌入字段
func resolveEmbeddedFields(
	structInfo *StructInfo,
	allStructs map[tsq.TypeInfo]*StructInfo,
) error {
	if structInfo.embeddedResolved {
		return nil
	}

	for embeddedType := range structInfo.embeddedTypes {
		embeddedStruct, found := allStructs[embeddedType]
		if !found {
			return errors.Errorf("embedded struct %s not found", embeddedType)
		}

		if !embeddedStruct.embeddedResolved {
			if err := resolveEmbeddedFields(embeddedStruct, allStructs); err != nil {
				return errors.Annotatef(err, "failed to recursively parse embedded struct %s", embeddedType)
			}
		}

		if err := copyEmbeddedFields(structInfo, embeddedStruct); err != nil {
			return errors.Annotatef(err, "failed to copy embedded fields: %s", embeddedType)
		}
	}

	structInfo.embeddedResolved = true

	return nil
}

// copyEmbeddedFields 复制嵌入结构的字段
func copyEmbeddedFields(targetStruct *StructInfo, embeddedStruct *StructInfo) error {
	for fieldName, field := range embeddedStruct.FieldMap {
		if _, exists := targetStruct.FieldMap[fieldName]; exists {
			return errors.Errorf("field %s already exists in struct %v", fieldName, targetStruct.TypeInfo)
		}

		targetStruct.FieldMap[fieldName] = field
	}

	return nil
}
