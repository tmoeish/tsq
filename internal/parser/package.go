package parser

import (
	"container/list"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/tools/go/packages"

	"github.com/tmoeish/tsq/v4/internal/genmodel"
)

// ParseResult 解析结果
type ParseResult struct {
	Structs   []*StructInfo // 解析到的结构体列表
	Directory string        // 目标目录路径
}

// Parse 解析指定路径的包，返回所有带有表注解的结构体和目录路径
func Parse(packagePath string) ([]*genmodel.StructInfo, string, error) {
	result, err := parsePackage(packagePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse package %s"+": %w", packagePath, err)
	}

	infos := make([]*genmodel.StructInfo, len(result.Structs))
	for i, internal := range result.Structs {
		infos[i] = internal.StructInfo
	}

	return infos, result.Directory, nil
}

// parsePackage 解析包的完整流程
func parsePackage(packagePath string) (*ParseResult, error) {
	parseState := &ParseState{
		structMap:       make(map[genmodel.TypeInfo]*StructInfo),
		parsedPackages:  make(map[genmodel.PackageInfo]bool),
		pendingPackages: list.New(),
		loader:          newPackageLoader(),
	}

	pipeline := parsePipeline{
		targetPath: packagePath,
		state:      parseState,
	}

	if err := pipeline.collectStructs(); err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to recursively parse package", err)
	}

	if err := pipeline.resolveEmbeds(); err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to parse embedded fields", err)
	}

	packageInfo, err := pipeline.targetPackageInfo()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to get package info", err)
	}

	if err := pipeline.annotateTables(packageInfo); err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to parse table metadata", err)
	}

	result, err := pipeline.finalize()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to filter and process parse results", err)
	}

	return result, nil
}

// ParseState 包含解析过程中的状态信息
type ParseState struct {
	structMap       map[genmodel.TypeInfo]*StructInfo // 已解析的结构体映射
	parsedPackages  map[genmodel.PackageInfo]bool     // 已解析的包集合
	pendingPackages *list.List                        // 待解析的包队列
	loader          *packageLoader
}

type parsePipeline struct {
	targetPath string
	state      *ParseState
}

func (p parsePipeline) collectStructs() error {
	return p.state.parsePackagesRecursively(p.targetPath)
}

func (p parsePipeline) resolveEmbeds() error {
	return p.state.resolveAllEmbeddedFields()
}

func (p parsePipeline) targetPackageInfo() (genmodel.PackageInfo, error) {
	return p.state.getPackageInfo(p.targetPath)
}

func (p parsePipeline) annotateTables(pkg genmodel.PackageInfo) error {
	return p.state.parseTableMetadata(pkg)
}

func (p parsePipeline) finalize() (*ParseResult, error) {
	return p.state.filterAndProcessResults(p.targetPath)
}

type loadedPackage struct {
	Dir        string
	ImportPath string
	Name       string
	GoFiles    []string
	Imports    map[string]genmodel.PackageInfo
}

// parsePackagesRecursively 递归解析包
func (ps *ParseState) parsePackagesRecursively(packagePath string) error {
	ps.pendingPackages.PushBack(packagePath)

	for ps.pendingPackages.Len() > 0 {
		element := ps.pendingPackages.Front()
		ps.pendingPackages.Remove(element)
		currentPath := element.Value.(string)

		if err := ps.parseSinglePackage(currentPath); err != nil {
			return fmt.Errorf("failed to recursively parse package %s"+": %w", currentPath, err)
		}
	}

	return nil
}

// resolveAllEmbeddedFields 解析所有结构体的嵌入字段
func (ps *ParseState) resolveAllEmbeddedFields() error {
	for _, structInfo := range ps.structMap {
		if err := resolveEmbeddedFields(structInfo, ps.structMap); err != nil {
			return fmt.Errorf("failed to parse embedded fields: %v"+": %w", structInfo.TypeInfo, err)
		}
	}

	return nil
}

// getPackageInfo 获取包信息
func (ps *ParseState) getPackageInfo(packagePath string) (genmodel.PackageInfo, error) {
	buildPkg, err := ps.importBuildPackage(packagePath)
	if err != nil {
		return genmodel.PackageInfo{}, fmt.Errorf("failed to process directory: %s"+": %w", packagePath, err)
	}

	return genmodel.PackageInfo{
		Path: buildPkg.ImportPath,
		Name: buildPkg.Name,
	}, nil
}

// parseTableMetadata 解析表元数据
func (ps *ParseState) parseTableMetadata(pkg genmodel.PackageInfo) error {
	buildPkg, err := ps.importBuildPackage(pkg.Path)
	if err != nil {
		return fmt.Errorf("failed to process directory: %s"+": %w", pkg.Path, err)
	}

	fileSet := token.NewFileSet()

	for _, filename := range buildPkg.GoFiles {
		if shouldSkipFile(filename) {
			continue
		}

		fullPath := filepath.Join(buildPkg.Dir, filename)

		file, err := parser.ParseFile(fileSet, fullPath, nil, parser.ParseComments)
		if err != nil {
			return fmt.Errorf("failed to parse file: %s"+": %w", fullPath, err)
		}

		if err := ps.processFileComments(file, fileSet, pkg); err != nil {
			return fmt.Errorf("%s: %w", "failed to parse @TABLE/@RESULT annotations", err)
		}
	}

	return nil
}

// shouldSkipFile 判断是否应该跳过文件
func shouldSkipFile(filename string) bool {
	if strings.HasSuffix(filename, TSQFileSuffix) {
		return true
	}

	if strings.HasSuffix(filename, "_test.go") {
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
	pkg genmodel.PackageInfo,
) error {
	commentMap := ast.NewCommentMap(fileSet, file, file.Comments)

	for node, comments := range commentMap {
		switch n := node.(type) {
		case *ast.GenDecl:
			if err := ps.processGenDecl(n, comments, fileSet, pkg); err != nil {
				return err
			}
		case *ast.TypeSpec:
			if err := ps.processTypeSpec(n, comments, fileSet, pkg); err != nil {
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
	fileSet *token.FileSet,
	pkg genmodel.PackageInfo,
) error {
	for _, spec := range genDecl.Specs {
		typeSpec, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}

		if !isStructType(typeSpec.Type) {
			continue
		}

		if err := ps.processStructTypeSpec(typeSpec, comments, fileSet, pkg); err != nil {
			return err
		}
	}

	return nil
}

// processTypeSpec 处理类型声明节点
func (ps *ParseState) processTypeSpec(
	typeSpec *ast.TypeSpec,
	comments []*ast.CommentGroup,
	fileSet *token.FileSet,
	pkg genmodel.PackageInfo,
) error {
	if !isStructType(typeSpec.Type) {
		return nil
	}

	return ps.processStructTypeSpec(typeSpec, comments, fileSet, pkg)
}

// processStructTypeSpec 处理结构体类型声明
func (ps *ParseState) processStructTypeSpec(
	typeSpec *ast.TypeSpec,
	comments []*ast.CommentGroup,
	fileSet *token.FileSet,
	pkg genmodel.PackageInfo,
) error {
	structName := typeSpec.Name.Name
	typeInfo := genmodel.TypeInfo{Package: pkg, TypeName: structName}

	structInfo, exists := ps.structMap[typeInfo]
	if !exists {
		return nil
	}
	// 构建字段集合
	fields := make(map[string]struct{})
	for name := range structInfo.FieldMap {
		fields[name] = struct{}{}
	}

	tableMeta, err := ParseTableInfo(structName, comments, fields, fileSet)
	if err != nil {
		return err
	}

	if tableMeta != nil {
		structInfo.TableMeta = tableMeta
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
	buildPkg, err := ps.importBuildPackage(packagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to process directory: %s"+": %w", packagePath, err)
	}

	targetPkg := genmodel.PackageInfo{
		Path: buildPkg.ImportPath,
		Name: buildPkg.Name,
	}

	var results []*StructInfo

	for _, structInfo := range ps.structMap {
		if structInfo.TableMeta == nil {
			continue
		}

		if structInfo.TypeInfo.Package != targetPkg {
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
	buildPkg, err := ps.importBuildPackage(packagePath)
	if err != nil {
		return fmt.Errorf("failed to process pkg: %s"+": %w", packagePath, err)
	}

	pkg := genmodel.PackageInfo{
		Path: buildPkg.ImportPath,
		Name: buildPkg.Name,
	}

	slog.Debug("parsing package", "packagePath", packagePath)

	fileSet := token.NewFileSet()

	for _, filename := range buildPkg.GoFiles {
		if shouldSkipFile(filename) {
			continue
		}

		fullPath := filepath.Join(buildPkg.Dir, filename)
		slog.Debug("parsing file", "fullPath", fullPath)

		file, err := parser.ParseFile(fileSet, fullPath, nil, parser.ParseComments)
		if err != nil {
			return fmt.Errorf("failed to parse file: %s"+": %w", fullPath, err)
		}

		packageAliases, err := parsePackageAliases(file)
		if err != nil {
			return fmt.Errorf("failed to resolve package aliases: %s"+": %w", fullPath, err)
		}

		err = ps.parseStructDeclarations(file, packageAliases, pkg)
		if err != nil {
			return fmt.Errorf("failed to parse struct declarations: %s"+": %w", fullPath, err)
		}
	}

	return nil
}

// parseStructDeclarations 解析文件中的结构体声明
func (ps *ParseState) parseStructDeclarations(
	file *ast.File,
	packageAliases map[string]genmodel.PackageInfo,
	pkg genmodel.PackageInfo,
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
				return err
			}
		}
	}

	return nil
}

// parsePackageAliases 解析文件中的包别名
func parsePackageAliases(file *ast.File) (map[string]genmodel.PackageInfo, error) {
	packageAliases := make(map[string]genmodel.PackageInfo)

	for _, importSpec := range file.Imports {
		importPath := strings.Trim(importSpec.Path.Value, `"`)

		pkg, err := getPackageInfo(importPath)
		if err != nil {
			return nil, err
		}

		if importSpec.Name != nil {
			switch importSpec.Name.Name {
			case "_":
				continue
			case ".":
				return nil, fmt.Errorf("dot imports are not supported: %s", importPath)
			}

			if existing, ok := packageAliases[importSpec.Name.Name]; ok {
				return nil, fmt.Errorf(
					"duplicate import alias %q for %s and %s",
					importSpec.Name.Name,
					existing.Path,
					importPath,
				)
			}

			// 显式别名
			packageAliases[importSpec.Name.Name] = pkg

			continue
		}

		if existing, ok := packageAliases[pkg.Name]; ok {
			return nil, fmt.Errorf(
				"duplicate import alias %q for %s and %s",
				pkg.Name,
				existing.Path,
				importPath,
			)
		}

		// 使用包名作为别名
		packageAliases[pkg.Name] = pkg
	}

	return packageAliases, nil
}

// getPackageInfo 根据导入路径获取包信息
func getPackageInfo(importPath string) (genmodel.PackageInfo, error) {
	pkg, err := loadSinglePackage(importPath)
	if err != nil {
		return genmodel.PackageInfo{}, NewPackageImportError(importPath, err)
	}

	return genmodel.PackageInfo{
		Path: pkg.ImportPath,
		Name: pkg.Name,
	}, nil
}

// resolveEmbeddedFields 解析嵌入字段
func resolveEmbeddedFields(
	structInfo *StructInfo,
	allStructs map[genmodel.TypeInfo]*StructInfo,
) error {
	if structInfo.embeddedResolved {
		return nil
	}

	if structInfo.embeddedResolving {
		return NewEmbeddedCycleError(structInfo.TypeInfo.String())
	}

	structInfo.embeddedResolving = true

	defer func() {
		structInfo.embeddedResolving = false
	}()

	for embeddedType := range structInfo.embeddedTypes {
		embeddedStruct, found := allStructs[embeddedType]
		if !found {
			return fmt.Errorf("embedded struct %s not found", embeddedType)
		}

		if !embeddedStruct.embeddedResolved {
			if err := resolveEmbeddedFields(embeddedStruct, allStructs); err != nil {
				return fmt.Errorf("failed to recursively parse embedded struct %s"+": %w", embeddedType, err)
			}
		}

		if err := copyEmbeddedFields(structInfo, embeddedStruct); err != nil {
			return fmt.Errorf("failed to copy embedded fields: %s"+": %w", embeddedType, err)
		}
	}

	structInfo.embeddedResolved = true

	return nil
}

func importBuildPackage(packagePath string) (*loadedPackage, error) {
	return defaultPackageLoader.load(packagePath)
}

func loadSinglePackage(packagePath string) (*loadedPackage, error) {
	return defaultPackageLoader.loadUncached(packagePath)
}

func (ps *ParseState) importBuildPackage(packagePath string) (*loadedPackage, error) {
	if ps != nil && ps.loader != nil {
		return ps.loader.load(packagePath)
	}

	return importBuildPackage(packagePath)
}

type packageLoader struct {
	mu    sync.Mutex
	cache map[string]*loadedPackage
}

func newPackageLoader() *packageLoader {
	return &packageLoader{
		cache: make(map[string]*loadedPackage),
	}
}

var defaultPackageLoader = newPackageLoader()

func (l *packageLoader) load(packagePath string) (*loadedPackage, error) {
	key, cfg, pattern, err := resolveLoadRequest(packagePath)
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	cached, ok := l.cache[key]
	l.mu.Unlock()

	if ok {
		return cloneLoadedPackage(cached), nil
	}

	pkg, err := l.loadWithConfig(cfg, pattern, packagePath)
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	// Double-check: another goroutine may have populated the cache while we
	// were loading outside the lock (TOCTOU). Prefer the cached copy if present.
	if existing, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return cloneLoadedPackage(existing), nil
	}

	l.cache[key] = cloneLoadedPackage(pkg)
	l.mu.Unlock()

	return pkg, nil
}

func (l *packageLoader) loadUncached(packagePath string) (*loadedPackage, error) {
	_, cfg, pattern, err := resolveLoadRequest(packagePath)
	if err != nil {
		return nil, err
	}

	return l.loadWithConfig(cfg, pattern, packagePath)
}

func resolveLoadRequest(packagePath string) (string, *packages.Config, string, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedModule,
	}

	pattern := packagePath

	if filepath.IsAbs(packagePath) {
		cfg.Dir = packagePath
		pattern = "."
	} else if strings.HasPrefix(packagePath, ".") {
		absPath, err := filepath.Abs(packagePath)
		if err != nil {
			return "", nil, "", err
		}

		if _, statErr := os.Stat(absPath); statErr != nil {
			if _, currentFile, _, ok := runtime.Caller(0); ok {
				candidate := filepath.Clean(filepath.Join(filepath.Dir(currentFile), packagePath))
				if _, candidateErr := os.Stat(candidate); candidateErr == nil {
					absPath = candidate
				}
			}
		}

		cfg.Dir = absPath
		pattern = "."
	}

	key := packagePath
	if cfg.Dir != "" {
		key = cfg.Dir + "::" + pattern
	}

	return key, cfg, pattern, nil
}

func (l *packageLoader) loadWithConfig(
	cfg *packages.Config,
	pattern string,
	packagePath string,
) (*loadedPackage, error) {
	pkgs, err := packages.Load(cfg, pattern)
	if err != nil {
		return nil, err
	}

	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			return nil, fmt.Errorf("failed to load package %s", packagePath)
		}
	}

	if len(pkgs) == 0 {
		return nil, fmt.Errorf("package %s not found", packagePath)
	}

	pkg := pkgs[0]
	goFiles := pkg.GoFiles

	if len(goFiles) == 0 {
		goFiles = pkg.CompiledGoFiles
	}

	result := &loadedPackage{
		Name:       pkg.Name,
		ImportPath: pkg.PkgPath,
		GoFiles:    make([]string, 0, len(goFiles)),
		Imports:    make(map[string]genmodel.PackageInfo, len(pkg.Imports)),
	}

	for _, file := range goFiles {
		if result.Dir == "" {
			result.Dir = filepath.Dir(file)
		}

		result.GoFiles = append(result.GoFiles, filepath.Base(file))
	}

	if result.ImportPath == "" && result.Dir != "" {
		result.ImportPath = result.Dir
	}

	for importPath, imported := range pkg.Imports {
		result.Imports[importPath] = genmodel.PackageInfo{
			Path: imported.PkgPath,
			Name: imported.Name,
		}
	}

	return result, nil
}

func cloneLoadedPackage(pkg *loadedPackage) *loadedPackage {
	if pkg == nil {
		return nil
	}

	cloned := &loadedPackage{
		Dir:        pkg.Dir,
		ImportPath: pkg.ImportPath,
		Name:       pkg.Name,
		GoFiles:    append([]string(nil), pkg.GoFiles...),
		Imports:    make(map[string]genmodel.PackageInfo, len(pkg.Imports)),
	}

	maps.Copy(cloned.Imports, pkg.Imports)

	return cloned
}

// copyEmbeddedFields 复制嵌入结构的字段
func copyEmbeddedFields(targetStruct, embeddedStruct *StructInfo) error {
	for fieldName, field := range embeddedStruct.FieldMap {
		if _, exists := targetStruct.FieldMap[fieldName]; exists {
			return fmt.Errorf("field %s already exists in struct %v", fieldName, targetStruct.TypeInfo)
		}

		targetStruct.FieldMap[fieldName] = field
	}

	return nil
}
