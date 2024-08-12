package parser

import (
	"reflect"
	"testing"
)

func TestNewAnnotation(t *testing.T) {
	type args struct {
		comment string
	}
	tests := []struct {
		name string
		args args
		want *Annotation
	}{
		{
			"simple",
			args{
				comment: "//@TABLE",
			},
			&Annotation{
				Name: "@TABLE",
			},
		},
		{
			"with key",
			args{
				comment: "//@TABLE(tag)",
			},
			&Annotation{
				Name: "@TABLE",
				Key:  "tag",
				Vals: []string{"tag"},
			},
		},
		{
			"with key and value",
			args{
				comment: "//@IDX(UxName=Name)",
			},
			&Annotation{
				Name: "@IDX",
				Key:  "UxName",
				Vals: []string{"Name"},
			},
		},
		{
			"with key and values",
			args{
				comment: "//@IDX(NsRepo=NamespaceID,RepositoryID)",
			},
			&Annotation{
				Name: "@IDX",
				Key:  "NsRepo",
				Vals: []string{"NamespaceID", "RepositoryID"},
			},
		},
		{
			"with empty key and 1 value",
			args{
				comment: "//@KW(Name)",
			},
			&Annotation{
				Name: "@KW",
				Key:  "Name",
				Vals: []string{"Name"},
			},
		},
		{
			"with empty key and 2 values",
			args{
				comment: "//@KW(Name,DisplayName)",
			},
			&Annotation{
				Name: "@KW",
				Key:  "",
				Vals: []string{"Name", "DisplayName"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewAnnotation(tt.args.comment); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newAnnotation() = %v, want %v", got, tt.want)
			}
		})
	}
}
