package parser

import (
	"go/ast"
	"sort"
	"strings"

	"github.com/serenize/snaker"
)

type TableMeta struct {
	Table     string
	CustomID  bool
	ID        string
	Version   string
	CT        string
	MT        string
	DT        string
	KwList    []string
	UxList    UxList
	IdxList   IdxList
	QueryList IdxList

	UxMap  map[string][]string // name -> fieldNames
	IdxMap map[string][]string // name -> fieldNames
}

type UxList []IDX

type IdxList []IDX

type IDX struct {
	Name   string
	Fields []string
}

func ParseTableMeta(
	name string,
	commentGroup []*ast.CommentGroup,
) *TableMeta {
	if commentGroup == nil {
		return nil
	}

	em := &TableMeta{
		UxMap:  make(map[string][]string),
		IdxMap: make(map[string][]string),
	}

	isTable := false
	for _, comments := range commentGroup {
		for _, comment := range comments.List {
			an := NewAnnotation(comment.Text)
			if an == nil {
				continue
			}
			switch an.Name {
			case "@TABLE":
				isTable = true
				if len(an.Key) > 0 {
					em.Table = an.Key
				} else {
					em.Table = snaker.CamelToSnake(name)
				}
			case "@PK":
				em.ID = an.Key
			case "@CustomID":
				em.CustomID = true
			case "@V":
				if len(an.Key) > 0 {
					em.Version = an.Key
				} else {
					em.Version = "V"
				}
			case "@CT":
				if len(an.Key) > 0 {
					em.CT = an.Key
				} else {
					em.CT = "CT"
				}
			case "@MT":
				if len(an.Key) > 0 {
					em.MT = an.Key
				} else {
					em.MT = "MT"
				}
			case "@DT":
				if len(an.Key) > 0 {
					em.DT = an.Key
				} else {
					em.DT = "DT"
				}
			case "@UX":
				em.UxList = append(em.UxList, IDX{
					Name:   strings.Join(an.Vals, "And"),
					Fields: an.Vals,
				})
			case "@IDX":
				em.IdxList = append(em.IdxList, IDX{
					Name:   strings.Join(an.Vals, "And"),
					Fields: an.Vals,
				})
			case "@KW":
				em.KwList = an.Vals
			}
		}
	}
	if !isTable {
		return nil
	}

	tmp := map[string]bool{}
	for _, idx := range em.IdxList {
		for j := len(idx.Fields); j > 0; j-- {
			queryName := strings.Join(idx.Fields[:j], "And")
			if !tmp[queryName] {
				em.QueryList = append(em.QueryList, IDX{
					Name:   queryName,
					Fields: idx.Fields[:j],
				})
				tmp[queryName] = true
			}
		}
	}

	sort.Slice(em.UxList, func(i, j int) bool {
		return em.UxList[i].Name < em.UxList[j].Name
	})
	sort.Slice(em.IdxList, func(i, j int) bool {
		return em.IdxList[i].Name < em.IdxList[j].Name
	})
	sort.Slice(em.QueryList, func(i, j int) bool {
		return em.QueryList[i].Name < em.QueryList[j].Name
	})

	return em
}
