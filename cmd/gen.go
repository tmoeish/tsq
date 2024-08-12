package cmd

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"os"
	"path"
	"strings"
	"text/template"
	"unicode"

	"github.com/juju/errors"
	"github.com/serenize/snaker"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tmoeish/tsq/parser"
)

var (
	//go:embed tsq.go.tmpl
	tpl string

	t string
	v bool
)

func init() {
	GenCmd.Flags().StringVarP(&t, "template", "t", "", "template file")
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

		if len(t) > 0 {
			tplBytes, err := os.ReadFile(t)
			if err != nil {
				logrus.Fatalln("os.ReadFile", err)
			}
			tpl = string(tplBytes)
		}

		pPath := args[0]
		list, dir := parser.Parse(pPath)
		t, err := template.New("tsq.gotpl").Funcs(template.FuncMap{
			"ToUpper":      strings.ToUpper,
			"ToLower":      strings.ToLower,
			"UpperInitial": UpperInitial,
			"LowerInitial": LowerInitial,
			"CamelToSnake": snaker.CamelToSnake,
			"FieldType":    FieldType,
		}).Parse(tpl)
		if err != nil {
			logrus.Fatalln("template.Parse", errors.ErrorStack(err))
		}
		for _, s := range list {
			gen(s, t, dir)
		}
	},
}

func gen(s *parser.Struct, t *template.Template, dir string) {
	e := s.TableMeta
	if e.ID == "" {
		e.ID = "ID"
	}
	if e.Version == "" {
		e.Version = "V"
	}

	filename := fmt.Sprintf("%s_tsq.go", strings.ToLower(s.Name.Name))
	filename = path.Join(dir, filename)
	buf := new(bytes.Buffer)
	if err := t.Execute(buf, s); err != nil {
		logrus.Fatalln("tpl.exec", errors.ErrorStack(err))
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		_ = os.WriteFile(filename, buf.Bytes(), 0644)
		logrus.Fatalln("format.Source", filename, errors.ErrorStack(err))
	}
	err = os.WriteFile(filename, src, 0644)
	if err != nil {
		logrus.Fatalln("os.WriteFile", errors.ErrorStack(err))
	}
}

func UpperInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}

func LowerInitial(str string) string {
	for i, v := range str {
		return string(unicode.ToLower(v)) + str[i+1:]
	}
	return ""
}

func FieldType(f parser.Field) string {
	sb := new(strings.Builder)
	if f.Arr {
		sb.WriteString("[]")
	}
	if f.Ptr {
		sb.WriteString("*")
	}
	if f.Typ.Pkg.Name != "" {
		sb.WriteString(f.Typ.Pkg.Name)
		sb.WriteString(".")
	}
	sb.WriteString(f.Typ.Name)
	return sb.String()
}
