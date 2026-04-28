package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/juju/errors"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/examples/database"
)

type exampleSummary struct {
	CRUD      crudSummary        `json:"crud"`
	Alias     aliasSummary       `json:"alias"`
	Aggregate []aggregateSummary `json:"aggregate"`
	Keyword   keywordSummary     `json:"keyword"`
	DTO       dtoSummary         `json:"dto"`
	InVar     inVarSummary       `json:"in_var"`
	Chunked   chunkedSummary     `json:"chunked"`
}

type crudSummary struct {
	InsertedID          int64  `json:"inserted_id"`
	UpdatedDescription  string `json:"updated_description"`
	DeletedSuccessfully bool   `json:"deleted_successfully"`
}

type aliasSummary struct {
	UserName string `json:"user_name"`
	OrgName  string `json:"org_name"`
}

type aggregateSummary struct {
	Category      string  `json:"category"`
	OrderCount    int64   `json:"order_count"`
	AverageAmount float64 `json:"average_amount"`
}

type keywordSummary struct {
	Keyword string   `json:"keyword"`
	Total   int64    `json:"total"`
	Names   []string `json:"names"`
}

type dtoSummary struct {
	Total int64               `json:"total"`
	First *database.UserOrder `json:"first,omitempty"`
}

type inVarSummary struct {
	CategoryIDs []int64  `json:"category_ids"`
	ItemNames   []string `json:"item_names"`
}

type chunkedSummary struct {
	Inserted int64 `json:"inserted"`
	Updated  int64 `json:"updated"`
	Deleted  int64 `json:"deleted"`
	Before   int64 `json:"before"`
	After    int64 `json:"after"`
}

type aliasedUserOrgRow struct {
	UserName string
	OrgName  string
}

type categoryAggregateRow struct {
	Category      string
	OrderCount    int64
	AverageAmount float64
}

func main() {
	ctx := context.Background()
	dbmap, cleanup, err := openExampleDB()
	if err != nil {
		slog.Error("open example db", "error", errors.ErrorStack(err))
		os.Exit(1)
	}
	defer cleanup()

	summary, err := runAllExamples(ctx, dbmap)
	if err != nil {
		slog.Error("run examples", "error", errors.ErrorStack(err))
		os.Exit(1)
	}

	output, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		slog.Error("marshal summary", "error", errors.ErrorStack(err))
		os.Exit(1)
	}

	fmt.Println(string(output))
}

func openExampleDB() (*tsq.DbMap, func(), error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cleanup := func() {
		_ = db.Close()
	}

	mockSQL, err := readMockSQL()
	if err != nil {
		cleanup()
		return nil, nil, errors.Trace(err)
	}

	if _, err := db.Exec(string(mockSQL)); err != nil {
		cleanup()
		return nil, nil, errors.Annotate(err, "seed mock.sql")
	}

	dbmap := &tsq.DbMap{Db: db, Dialect: tsq.SqliteDialect{}}
	if err := tsq.Init(dbmap, false, true); err != nil {
		cleanup()
		return nil, nil, errors.Annotate(err, "init tsq")
	}

	return dbmap, cleanup, nil
}

func readMockSQL() ([]byte, error) {
	candidates := []string{
		"examples/database/mock.sql",
		"database/mock.sql",
	}

	var lastErr error
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err == nil {
			return data, nil
		}

		lastErr = err
	}

	return nil, errors.Annotate(lastErr, "read mock.sql")
}

func runAllExamples(ctx context.Context, dbmap *tsq.DbMap) (*exampleSummary, error) {
	crud, err := runCRUDDemo(ctx, dbmap)
	if err != nil {
		return nil, errors.Annotate(err, "crud demo")
	}

	alias, err := runAliasDemo(ctx, dbmap)
	if err != nil {
		return nil, errors.Annotate(err, "alias demo")
	}

	aggregate, err := runAggregateDemo(ctx, dbmap)
	if err != nil {
		return nil, errors.Annotate(err, "aggregate demo")
	}

	keyword, err := runKeywordDemo(ctx, dbmap)
	if err != nil {
		return nil, errors.Annotate(err, "keyword demo")
	}

	dto, err := runDTODemo(ctx, dbmap)
	if err != nil {
		return nil, errors.Annotate(err, "dto demo")
	}

	inVar, err := runInVarDemo(ctx, dbmap)
	if err != nil {
		return nil, errors.Annotate(err, "invar demo")
	}

	chunked, err := runChunkedDemo(ctx, dbmap)
	if err != nil {
		return nil, errors.Annotate(err, "chunked demo")
	}

	return &exampleSummary{
		CRUD:      *crud,
		Alias:     *alias,
		Aggregate: aggregate,
		Keyword:   *keyword,
		DTO:       *dto,
		InVar:     *inVar,
		Chunked:   *chunked,
	}, nil
}

func runCRUDDemo(ctx context.Context, dbmap *tsq.DbMap) (*crudSummary, error) {
	category := &database.Category{
		CategoryContent: database.CategoryContent{
			Type:        database.CategoryTypeArticle,
			Name:        "TSQ 示例分类",
			Description: "DTO、分页和聚合演示使用的分类",
		},
	}

	if err := category.Insert(ctx, dbmap); err != nil {
		return nil, errors.Trace(err)
	}

	category.Description = "DTO、分页和聚合演示使用的分类（已更新）"
	if err := category.Update(ctx, dbmap); err != nil {
		return nil, errors.Trace(err)
	}

	updated, err := database.GetCategoryByNameOrErr(ctx, dbmap, category.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := updated.Delete(ctx, dbmap); err != nil {
		return nil, errors.Trace(err)
	}

	deleted, err := database.GetCategoryByName(ctx, dbmap, category.Name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &crudSummary{
		InsertedID:          category.ID,
		UpdatedDescription:  updated.Description,
		DeletedSuccessfully: deleted == nil,
	}, nil
}

func runAliasDemo(ctx context.Context, dbmap *tsq.DbMap) (*aliasSummary, error) {
	orgAlias := "user_org"
	orgID := database.Org_ID.As(orgAlias)
	orgName := database.Org_Name.As(orgAlias).Into(func(holder any) any {
		return &holder.(*aliasedUserOrgRow).OrgName
	}, "org_name")
	userName := database.User_Name.Into(func(holder any) any {
		return &holder.(*aliasedUserOrgRow).UserName
	}, "user_name")

	query, err := tsq.
		Select(userName, orgName).
		LeftJoin(database.User_OrgID, orgID).
		Where(database.User_ID.EQ(1)).
		Build()
	if err != nil {
		return nil, errors.Annotate(err, "build alias query")
	}

	row, err := tsq.GetOrErr[aliasedUserOrgRow](ctx, dbmap, query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &aliasSummary{
		UserName: row.UserName,
		OrgName:  row.OrgName,
	}, nil
}

func runAggregateDemo(ctx context.Context, dbmap *tsq.DbMap) ([]aggregateSummary, error) {
	categoryName := database.Category_Name.Into(func(holder any) any {
		return &holder.(*categoryAggregateRow).Category
	}, "category")
	orderCount := database.Order_UID.Count().Into(func(holder any) any {
		return &holder.(*categoryAggregateRow).OrderCount
	}, "order_count")
	averageAmount := database.Order_Amount.Avg().Into(func(holder any) any {
		return &holder.(*categoryAggregateRow).AverageAmount
	}, "average_amount")

	query, err := tsq.
		Select(categoryName, orderCount, averageAmount).
		LeftJoin(database.Category_ID, database.Item_CategoryID).
		LeftJoin(database.Item_ID, database.Order_ItemID).
		GroupBy(database.Category_Name).
		Having(database.Order_UID.Count().GT(0)).
		Build()
	if err != nil {
		return nil, errors.Annotate(err, "build aggregate query")
	}

	rows, err := tsq.List[categoryAggregateRow](ctx, dbmap, query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	summaries := make([]aggregateSummary, 0, len(rows))
	for _, row := range rows {
		summaries = append(summaries, aggregateSummary{
			Category:      row.Category,
			OrderCount:    row.OrderCount,
			AverageAmount: row.AverageAmount,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Category < summaries[j].Category
	})

	return summaries, nil
}

func runKeywordDemo(ctx context.Context, dbmap *tsq.DbMap) (*keywordSummary, error) {
	pageReq := &tsq.PageReq{
		Page:    1,
		Size:    2,
		OrderBy: "id",
		Order:   "asc",
		Keyword: tsq.EscapeKeywordSearch("张"),
	}
	if err := pageReq.ValidateStrict(); err != nil {
		return nil, errors.Trace(err)
	}

	resp, err := database.PageUser(ctx, dbmap, pageReq)
	if err != nil {
		return nil, errors.Trace(err)
	}

	names := make([]string, 0, len(resp.Data))
	for _, user := range resp.Data {
		names = append(names, user.Name)
	}

	return &keywordSummary{
		Keyword: "张",
		Total:   resp.Total,
		Names:   names,
	}, nil
}

func runDTODemo(ctx context.Context, dbmap *tsq.DbMap) (*dtoSummary, error) {
	pageReq := &tsq.PageReq{
		Page:    1,
		Size:    3,
		OrderBy: "user_id,order_id",
		Order:   "asc,asc",
	}
	if err := pageReq.ValidateStrict(); err != nil {
		return nil, errors.Trace(err)
	}

	resp, err := database.PageUserOrder(ctx, dbmap, pageReq, 1, "图书", "视频")
	if err != nil {
		return nil, errors.Trace(err)
	}

	var first *database.UserOrder
	if len(resp.Data) > 0 {
		first = resp.Data[0]
	}

	return &dtoSummary{
		Total: resp.Total,
		First: first,
	}, nil
}

func runInVarDemo(ctx context.Context, dbmap *tsq.DbMap) (*inVarSummary, error) {
	query, err := tsq.
		Select(database.TableItemCols...).
		Where(database.Item_CategoryID.InVar()).
		Build()
	if err != nil {
		return nil, errors.Annotate(err, "build invar query")
	}

	categoryIDs := []int64{1}
	items, err := tsq.List[database.Item](ctx, dbmap, query, categoryIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	sort.Strings(names)

	return &inVarSummary{
		CategoryIDs: categoryIDs,
		ItemNames:   names,
	}, nil
}

func runChunkedDemo(ctx context.Context, dbmap *tsq.DbMap) (*chunkedSummary, error) {
	before, err := database.CountUser(ctx, dbmap)
	if err != nil {
		return nil, errors.Trace(err)
	}

	users := []*database.User{
		{OrgID: 1, Name: "chunk_user_alpha", Email: "alpha@example.com"},
		{OrgID: 1, Name: "chunk_user_beta", Email: "beta@example.com"},
		{OrgID: 2, Name: "chunk_user_gamma", Email: "gamma@example.com"},
	}

	if err := tsq.ChunkedInsert(ctx, dbmap, users, &tsq.ChunkedInsertOptions{ChunkSize: 2}); err != nil {
		return nil, errors.Trace(err)
	}

	nameQuery, err := tsq.
		Select(database.TableUserCols...).
		Where(database.User_Name.InVar()).
		Build()
	if err != nil {
		return nil, errors.Annotate(err, "build chunked user lookup query")
	}

	names := []string{"chunk_user_alpha", "chunk_user_beta", "chunk_user_gamma"}
	insertedUsers, err := tsq.List[database.User](ctx, dbmap, nameQuery, names)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, user := range insertedUsers {
		user.Email = "updated+" + user.Email
	}

	if err := tsq.ChunkedUpdate(ctx, dbmap, insertedUsers, &tsq.ChunkedOptions{ChunkSize: 2}); err != nil {
		return nil, errors.Trace(err)
	}

	if err := tsq.ChunkedDelete(ctx, dbmap, insertedUsers[:1], &tsq.ChunkedOptions{ChunkSize: 1}); err != nil {
		return nil, errors.Trace(err)
	}

	remainingIDs := make([]any, 0, len(insertedUsers)-1)
	for _, user := range insertedUsers[1:] {
		remainingIDs = append(remainingIDs, user.ID)
	}

	if err := tsq.ChunkedDeleteByIDs(
		ctx,
		dbmap,
		"user",
		"id",
		remainingIDs,
		&tsq.ChunkedOptions{ChunkSize: 2},
	); err != nil {
		return nil, errors.Trace(err)
	}

	after, err := database.CountUser(ctx, dbmap)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &chunkedSummary{
		Inserted: int64(len(insertedUsers)),
		Updated:  int64(len(insertedUsers)),
		Deleted:  int64(len(insertedUsers)),
		Before:   before,
		After:    after,
	}, nil
}
