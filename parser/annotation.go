package parser

import (
	"strings"
)

func NewAnnotation(comment string) *Annotation {
	annotation := new(Annotation)
	s := strings.ReplaceAll(comment, " ", "")
	s = strings.ReplaceAll(comment, "\t", "")
	if !strings.HasPrefix(s, "//@") {
		return nil
	}

	s = strings.TrimPrefix(s, "//")
	s = strings.TrimSuffix(s, ")")

	tmp := strings.Split(s, "(")
	annotation.Name = tmp[0]
	if len(tmp) == 1 {
		return annotation
	}

	kv := strings.Split(tmp[1], "=")
	if len(kv) > 1 {
		annotation.Key = kv[0]
		annotation.Vals = strings.Split(kv[1], ",")
	} else {
		annotation.Vals = strings.Split(kv[0], ",")
		if len(annotation.Vals) == 1 {
			annotation.Key = annotation.Vals[0]
		}
	}

	return annotation
}

type Annotation struct {
	Name string
	Key  string
	Vals []string
}
