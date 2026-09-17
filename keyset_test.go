package tsq

import (
	"context"
	"slices"
	"testing"
)

func TestPageKeysetWalksEveryRowOnce(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "d", "b", "a", "c", "bb", "e", "ab")

	q := Select(User__Cols...).From(Users).Search(Searchable(User_Name)).MustBuild()
	// version is 0 on every row, so the pages break ties on name and then id.
	order := []OrderBy{User_Version.Asc(), User_Name.Desc(), User_ID.Asc()}

	all, err := Select(User__Cols...).From(Users).OrderBy(order...).MustBuild().List(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	var walked []int64

	k := Keyset{Size: 3, OrderBy: order}

	for pages := 0; ; pages++ {
		page, err := q.PageKeyset(ctx, rt, k)
		if err != nil {
			t.Fatal(err)
		}

		for _, row := range page.Data {
			walked = append(walked, row.ID)
		}

		if pages == 0 {
			// A row inserted before the position does not shift later pages.
			if err := Users.Insert(ctx, rt, &user{Name: "z", Email: "z@example.com"}); err != nil {
				t.Fatal(err)
			}
		}

		if !page.HasNext() {
			break
		}

		k.After = page.Next
	}

	want := make([]int64, 0, len(all))
	for _, row := range all {
		want = append(want, row.ID)
	}

	if !slices.Equal(walked, want) {
		t.Fatalf("walked %v, want %v", walked, want)
	}

	// The keyword is an argument here too.
	page, err := q.PageKeyset(ctx, rt, Keyset{Size: 10, OrderBy: order}, Keyword("b"))
	if err != nil || len(page.Data) != 3 || page.HasNext() {
		t.Fatalf("searched page = %+v, %v", page, err)
	}

	first, err := q.PageKeyset(ctx, rt, Keyset{Size: 1, OrderBy: order})
	if err != nil {
		t.Fatal(err)
	}

	bad := map[string]struct {
		q *Query[user]
		k Keyset
	}{
		"no order":         {q, Keyset{}},
		"builder order":    {Select(User__Cols...).From(Users).OrderBy(User_ID.Asc()).MustBuild(), Keyset{OrderBy: order}},
		"not unique":       {q, Keyset{OrderBy: []OrderBy{User_Name.Asc()}}},
		"not selected":     {Select(User_ID).From(Users).MustBuild(), Keyset{OrderBy: order}},
		"grouped":          {SelectDistinct(User__Cols...).From(Users).MustBuild(), Keyset{OrderBy: order}},
		"other order":      {q, Keyset{OrderBy: []OrderBy{User_Name.Asc(), User_ID.Asc()}, After: first.Next}},
		"garbage cursor":   {q, Keyset{OrderBy: order, After: "not a cursor"}},
		"truncated cursor": {q, Keyset{OrderBy: order, After: first.Next[:len(first.Next)-4]}},
	}

	for name, tc := range bad {
		if _, err := tc.q.PageKeyset(ctx, rt, tc.k); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestPageRequestKeysetResolvesSortFields(t *testing.T) {
	req := &PageRequest{Size: 5, Page: 9, OrderBy: "name", Order: "desc", After: "cursor"}

	k, err := req.Keyset(User_Name)
	if err != nil {
		t.Fatal(err)
	}

	if k.Size != 5 || k.After != "cursor" || len(k.OrderBy) != 1 || k.OrderBy[0].direction != DESC {
		t.Fatalf("keyset = %+v", k)
	}

	if _, err := (&PageRequest{OrderBy: "email"}).Keyset(User_Name); !isErr[*UnknownSortFieldError](err) {
		t.Fatalf("unknown field = %v", err)
	}
}
