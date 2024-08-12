package parser

import "strings"

func NewTags(s string) map[string]string {
	s = strings.Replace(s, "`", "", 2)

	list := strings.Split(s, " ")
	tags := make(map[string]string)
	for _, i := range list {
		tag := NewTag(strings.TrimSpace(i))
		tags[tag.Name] = tag.Value
	}
	return tags
}

func NewTag(s string) Tag {
	tmp := strings.Split(strings.TrimSpace(s), ":")
	value := strings.TrimSpace(tmp[1])
	value = strings.Replace(value, `"`, ``, 2)
	return Tag{
		Name:  strings.TrimSpace(tmp[0]),
		Value: value,
	}
}

type Tag struct {
	Name  string
	Value string
}
