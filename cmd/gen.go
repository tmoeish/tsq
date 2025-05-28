package cmd

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/internal/parser"
	"mvdan.cc/gofumpt/format"
)

var (
	//go:embed tsq.go.tmpl
	tplTxt string

	//go:embed tsq_dto.go.tmpl
	dtoTplTxt string

	tplFlag    string
	dtoTplFlag string
	v          bool
)

func init() {
	GenCmd.Flags().StringVarP(&tplFlag, "tpl", "t", "", "tsq template file")
	GenCmd.Flags().StringVarP(&dtoTplFlag, "dtotpl", "", "", "tsq dto template file")
	GenCmd.Flags().BoolVarP(&v, "verbose", "v", false, "verbose output")
}

var GenCmd = &cobra.Command{
	Use:   "gen [package]",
	Short: "`gen` generates tsq.go file for each table in package",
	Run: func(cmd *cobra.Command, args []string) {
		if v {
			logrus.SetLevel(logrus.DebugLevel)
		}

		if len(args) < 1 {
			_ = cmd.Usage()
			return
		}

		if len(tplFlag) > 0 {
			tplBytes, err := os.ReadFile(tplFlag)
			if err != nil {
				fmt.Fprintf(os.Stderr, "os.ReadFile 失败: %v\n详细堆栈:\n%+v\n", err, errors.ErrorStack(err))
				os.Exit(1)
			}
			tplTxt = string(tplBytes)
		}

		if len(dtoTplFlag) > 0 {
			tplBytes, err := os.ReadFile(dtoTplFlag)
			if err != nil {
				fmt.Fprintf(os.Stderr, "os.ReadFile 失败: %v\n详细堆栈:\n%+v\n", err, errors.ErrorStack(err))
				os.Exit(1)
			}
			dtoTplTxt = string(tplBytes)
		}

		pPath := args[0]
		list, dir, err := parser.Parse(pPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parser.Parse 失败: %v\n详细堆栈:\n%+v\n", err, errors.ErrorStack(err))
			os.Exit(1)
		}

		for i := range list {
			list[i].SetTSQVersion(tsq.Version)
		}

		tpl, err := template.New("tsq.go.tmpl").Funcs(TemplateFuncs()).Parse(tplTxt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "template.Parse 失败: %v\n详细堆栈:\n%+v\n", err, errors.ErrorStack(err))
			os.Exit(1)
		}

		dtoTpl, err := template.New("tsq_dto.go.tmpl").Funcs(TemplateFuncs()).Parse(dtoTplTxt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "template.Parse 失败: %v\n详细堆栈:\n%+v\n", err, errors.ErrorStack(err))
			os.Exit(1)
		}

		for _, s := range list {
			if s.TableInfo == nil || len(s.Fields) == 0 {
				logrus.Errorf("结构体 %s 解析失败，表名: %s，字段数: %d，详细: 结构体未能正确生成 TableInfo 或无有效字段。请检查 DSL 注解与结构体定义是否一致。", s.TypeInfo.TypeName, s.Table, len(s.Fields))
				continue
			}
			if len(s.Table) == 0 {
				for i, f := range s.Fields {
					s.Fields[i].Column = strings.Replace(f.Column, ".", "_", 1)
				}
				if err := genDTO(s, dtoTpl, dir); err != nil {
					logrus.Errorf("生成 DTO 文件失败: 结构体: %s, 错误: %v", s.TypeInfo.TypeName, err)
				}
			} else {
				if err := gen(s, tpl, dir); err != nil {
					logrus.Errorf("生成表文件失败: 结构体: %s, 表名: %s, 错误: %v", s.TypeInfo.TypeName, s.Table, err)
				}
			}
		}
	},
}

func gen(data *tsq.StructInfo, t *template.Template, dir string) error {
	filename := fmt.Sprintf("%s_tsq.go", strings.ToLower(data.TypeInfo.TypeName))
	filename = path.Join(dir, filename)
	logrus.Debugf("gen %s with data %s", filename, tsq.PrettyJSON(data))

	buf := new(bytes.Buffer)

	if err := t.Execute(buf, data); err != nil {
		bs := tsq.PrettyJSON(data)
		return errors.Annotatef(err, "模板渲染失败: %s, 数据: %s", filename, string(bs))
	}

	src, err := format.Source(buf.Bytes(), format.Options{})
	if err != nil {
		_ = os.WriteFile(filename, buf.Bytes(), 0o644)
		return errors.Annotatef(err, "Go 代码格式化失败: %s", filename)
	}

	err = os.WriteFile(filename, src, 0o644)
	if err != nil {
		return errors.Annotatef(err, "写文件失败: %s", filename)
	}

	return nil
}

func genDTO(data *tsq.StructInfo, t *template.Template, dir string) error {
	filename := fmt.Sprintf("%s_dto_tsq.go", strings.ToLower(data.TypeInfo.TypeName))
	filename = path.Join(dir, filename)
	buf := new(bytes.Buffer)

	if err := t.Execute(buf, data); err != nil {
		bs := tsq.PrettyJSON(data)
		return errors.Annotatef(err, "DTO模板渲染失败: %s, 数据: %s", filename, string(bs))
	}

	src, err := format.Source(buf.Bytes(), format.Options{})
	if err != nil {
		_ = os.WriteFile(filename, buf.Bytes(), 0o644)
		return errors.Annotatef(err, "Go 代码格式化失败: %s", filename)
	}

	err = os.WriteFile(filename, src, 0o644)
	if err != nil {
		return errors.Annotatef(err, "写文件失败: %s", filename)
	}

	return nil
}
