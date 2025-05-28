# 贡献指南

感谢您对 TSQ 项目的关注和贡献！本文档将指导您如何为项目做出贡献。

## 🚀 快速开始

1. **Fork 项目**
   ```bash
   # 在 GitHub 上 fork https://github.com/tmoeish/tsq
   ```

2. **克隆到本地**
   ```bash
   git clone https://github.com/YOUR_USERNAME/tsq.git
   cd tsq
   ```

3. **设置开发环境**
   ```bash
   # 安装依赖
   make mod-download
   
   # 运行测试确保环境正常
   make test
   ```

## 📝 开发规范

### 代码规范

- 遵循 Go 官方代码规范
- 使用 `gofmt` 格式化代码
- 通过 `go vet` 检查
- 通过 `golangci-lint` 静态分析

```bash
# 运行所有检查
make fmt vet lint
```

### 提交规范

我们使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**类型说明：**
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档更新
- `style`: 代码格式修改
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

**示例：**
```
feat(parser): 添加对 @IDX 注解的支持

- 支持单字段和复合字段索引
- 更新模板生成索引信息
- 添加相关测试用例

Closes #123
```

### 分支规范

- `main`: 主分支，保持稳定
- `develop`: 开发分支
- `feature/xxx`: 功能分支
- `fix/xxx`: 修复分支
- `docs/xxx`: 文档分支

## 🔧 开发流程

1. **创建分支**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **开发和测试**
   ```bash
   # 开发代码
   # ...
   
   # 运行测试
   make test
   
   # 代码检查
   make fmt vet lint
   ```

3. **提交代码**
   ```bash
   git add .
   git commit -m "feat: your feature description"
   ```

4. **推送分支**
   ```bash
   git push origin feature/your-feature-name
   ```

5. **创建 Pull Request**
   - 在 GitHub 上创建 PR
   - 填写详细的描述
   - 关联相关的 Issue

## 🧪 测试指南

### 运行测试

```bash
# 运行所有测试
make test

# 运行覆盖率测试
make test-coverage

# 运行特定包的测试
go test -v ./parser/...
```

### 编写测试

- 为新功能编写单元测试
- 测试文件名以 `_test.go` 结尾
- 测试函数名以 `Test` 开头
- 使用 `testify` 库进行断言

```go
func TestNewFeature(t *testing.T) {
    // 准备测试数据
    // ...
    
    // 执行测试
    result := YourFunction(input)
    
    // 验证结果
    assert.Equal(t, expected, result)
}
```

## 📖 文档贡献

### 更新文档

- README.md: 项目主要文档
- Go Doc: 代码注释文档
- 示例代码: `sample/` 目录

### 文档规范

- 使用清晰简洁的中文
- 提供完整的代码示例
- 包含必要的截图或图表
- 保持与代码同步更新

## 🐛 报告问题

### 报告 Bug

1. 搜索现有 Issues 避免重复
2. 使用 Bug 报告模板
3. 提供详细的重现步骤
4. 包含环境信息和错误日志

### 功能建议

1. 使用功能请求模板
2. 详细描述功能需求
3. 说明使用场景和收益
4. 讨论实现方案

## 🏗️ 构建和发布

### 本地构建

```bash
# 构建二进制文件
make build

# 安装到 GOPATH/bin
make install

# 更新示例代码
make update-sample
```

### 发布流程

发布由维护者负责：

1. 更新版本号
2. 更新 CHANGELOG.md
3. 创建 Git tag
4. 通过 GitHub Actions 自动发布

## 📋 检查清单

在提交 PR 之前，请确保：

- [ ] 代码通过所有测试
- [ ] 代码通过格式检查和静态分析
- [ ] 添加了必要的测试用例
- [ ] 更新了相关文档
- [ ] 提交信息符合规范
- [ ] PR 描述清晰完整

## 💬 获取帮助

- 通过 [GitHub Issues](https://github.com/tmoeish/tsq/issues) 提问
- 查看现有的问题和解答
- 参考项目文档和示例代码

---

如果您有任何问题或建议，欢迎随时联系我们！ 