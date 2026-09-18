package academy

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/tmoeish/tsq/v5"
	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// QuickstartSummary captures the three introductory Academy demo outcomes.
type QuickstartSummary struct {
	TrackCRUD      CRUDSummary    `json:"track_crud"`      // TrackCRUD summarizes generated CRUD helper usage.
	CatalogSearch  SearchSummary  `json:"catalog_search"`  // CatalogSearch summarizes keyword search with paging.
	BackendCatalog CatalogSummary `json:"backend_catalog"` // BackendCatalog summarizes a joined catalog listing.
}

// AdvancedSummary captures the focused feature demos in the advanced example.
type AdvancedSummary struct {
	Alias          AliasSummary          `json:"alias_prerequisite"` // Alias summarizes table aliasing over prerequisite joins.
	Aggregate      []AggregateSummary    `json:"track_metrics"`      // Aggregate summarizes grouped track metrics.
	ListParam      ListParamSummary      `json:"list_param"`         // ListParam summarizes list parameter binding.
	Subquery       SubquerySummary       `json:"subquery"`           // Subquery summarizes scalar and membership subquery usage.
	Case           CaseSummary           `json:"case_labels"`        // Case summarizes CASE-expression labeling.
	CTE            CTESummary            `json:"cte"`                // CTE summarizes common-table-expression queries.
	SetOps         SetOpsSummary         `json:"set_ops"`            // SetOps summarizes UNION and INTERSECT style queries.
	Batch          BatchSummary          `json:"batch"`              // Batch summarizes batch write helpers.
	SoftDelete     SoftDeleteSummary     `json:"soft_delete"`        // SoftDelete summarizes soft deletes, restores and hard deletes.
	OptimisticLock OptimisticLockSummary `json:"optimistic_lock"`    // OptimisticLock summarizes version-guarded writes.
	DatabaseFilled DatabaseFilledSummary `json:"database_filled"`    // DatabaseFilled summarizes columns the database provides.
	FullText       FullTextSummary       `json:"full_text"`          // FullText summarizes full-text search over the catalog.
}

// FullTextSummary captures the full-text search demo.
type FullTextSummary struct {
	Term   string   `json:"term"`   // Term is the searched term.
	Titles []string `json:"titles"` // Titles are the matching course titles.
	Native bool     `json:"native"` // Native reports whether the dialect searched a full-text index.
}

// DatabaseFilledSummary captures the demo of columns the database fills.
type DatabaseFilledSummary struct {
	Currency string `json:"currency"` // Currency comes from the column's DEFAULT.
	Slug     string `json:"slug"`     // Slug is computed by the database from the title.
}

// FullSuiteSummary aggregates the quickstart, advanced, and comprehensive demos.
type FullSuiteSummary struct {
	Quickstart    QuickstartSummary    `json:"quickstart"`    // Quickstart contains the introductory demos.
	Advanced      AdvancedSummary      `json:"advanced"`      // Advanced contains the focused feature demos.
	Comprehensive ComprehensiveSummary `json:"comprehensive"` // Comprehensive contains the end-to-end reporting demo.
}

// ComprehensiveSummary captures the final paginated LearningJourney demo result.
type ComprehensiveSummary struct {
	LearnerIDs []int64          `json:"learner_ids"`     // LearnerIDs lists the learner filters passed into the query.
	Tracks     []string         `json:"tracks"`          // Tracks lists the selected track filters.
	Total      int64            `json:"total"`           // Total is the total number of matching rows.
	First      *LearningJourney `json:"first,omitempty"` // First holds the first matching row when present.
}

// CRUDSummary captures the create, update, and delete demo outputs.
type CRUDSummary struct {
	InsertedID          int64           `json:"inserted_id"`          // InsertedID is the generated ID returned by the insert demo.
	UpdatedDescription  string          `json:"updated_description"`  // UpdatedDescription is the description after the update demo.
	UpdatedSkillItems   json.RawMessage `json:"updated_skill_items"`  // UpdatedSkillItems shows explicit JSON db-type override data round-tripping through generated CRUD helpers.
	UpsertedSameRow     bool            `json:"upserted_same_row"`    // UpsertedSameRow reports whether an upsert by name updated the inserted row.
	DeletedSuccessfully bool            `json:"deleted_successfully"` // DeletedSuccessfully reports whether the delete demo removed the row.
}

// SearchSummary captures the keyword-search demo result.
type SearchSummary struct {
	Keyword string   `json:"keyword"` // Keyword is the search term used in the demo.
	Total   int64    `json:"total"`   // Total is the total number of matching rows.
	Titles  []string `json:"titles"`  // Titles lists the returned course titles.
}

// CatalogSummary captures the joined catalog listing demo result.
type CatalogSummary struct {
	Track  string   `json:"track"`  // Track is the track filter used for the listing.
	Titles []string `json:"titles"` // Titles lists the returned course titles.
}

// AliasSummary captures the self-join alias demo result.
type AliasSummary struct {
	CourseTitle       string `json:"course_title"`       // CourseTitle is the course title.
	PrerequisiteTitle string `json:"prerequisite_title"` // PrerequisiteTitle is the aliased prerequisite title.
}

// AggregateSummary captures one grouped metric row.
type AggregateSummary struct {
	Track           string  `json:"track"`            // Track is the grouped track name.
	EnrollmentCount int64   `json:"enrollment_count"` // EnrollmentCount is the number of enrollments in the group.
	AverageScore    float64 `json:"average_score"`    // AverageScore is the average score within the group.
}

// ListParamSummary captures the list parameter demo result.
type ListParamSummary struct {
	CourseIDs []int64  `json:"course_ids"` // CourseIDs is the input ID slice bound at execution time.
	Titles    []string `json:"titles"`     // Titles lists the matched course titles.
}

// SubquerySummary captures the subquery demo outputs.
type SubquerySummary struct {
	LearnersInDataTrack      []string `json:"learners_in_data_track"`      // LearnersInDataTrack lists learners returned by an IN subquery.
	CoursesCheaperThanAnchor []string `json:"courses_cheaper_than_anchor"` // CoursesCheaperThanAnchor lists courses filtered by a scalar subquery.
}

// CaseSummary captures labels produced by a CASE expression.
type CaseSummary struct {
	Labels []string `json:"labels"` // Labels lists the derived labels returned by the CASE demo.
}

// CTESummary captures the common-table-expression demo result.
type CTESummary struct {
	Track  string   `json:"track"`  // Track is the track summarized by the CTE query.
	Total  int64    `json:"total"`  // Total is the number of returned titles.
	Titles []string `json:"titles"` // Titles lists the titles returned from the CTE query.
}

// SetOpsSummary captures the set-operation demo result.
type SetOpsSummary struct {
	UnionTitles   []string `json:"union_titles"`   // UnionTitles lists titles returned by the UNION query.
	StarterTitles []string `json:"starter_titles"` // StarterTitles lists titles returned by the INTERSECT query.
}

// BatchSummary captures the batch write demo result.
type BatchSummary struct {
	Inserted int64 `json:"inserted"` // Inserted is the number of rows inserted by the batch demo.
	Updated  int64 `json:"updated"`  // Updated is the number of rows updated by the batch demo.
	Deleted  int64 `json:"deleted"`  // Deleted is the number of rows deleted by the batch demo.
	Before   int64 `json:"before"`   // Before is the number of rows loaded before the update step.
	After    int64 `json:"after"`    // After is the number of rows remaining after cleanup.
}

// SoftDeleteSummary captures the soft-delete demo result.
type SoftDeleteSummary struct {
	EnrollmentUID  int64 `json:"enrollment_uid"`   // EnrollmentUID is the row used for the demo.
	ActiveBefore   bool  `json:"active_before"`    // ActiveBefore reports whether the row was active before the delete.
	VisibleBefore  bool  `json:"visible_before"`   // VisibleBefore reports whether generated queries returned the row before the delete.
	ActiveAfter    bool  `json:"active_after"`     // ActiveAfter reports whether the row was still active after the soft delete.
	VisibleAfter   bool  `json:"visible_after"`    // VisibleAfter reports whether generated queries still returned the row.
	StoredAfter    bool  `json:"stored_after"`     // StoredAfter reports whether the row was still stored in the table.
	VisibleRestore bool  `json:"visible_restored"` // VisibleRestore reports whether clearing the tombstone brought the row back.
	StoredFinal    bool  `json:"stored_final"`     // StoredFinal reports whether the row survived the closing hard delete.
}

// OptimisticLockSummary captures the optimistic-lock demo result.
type OptimisticLockSummary struct {
	EnrollmentUID       int64 `json:"enrollment_uid"`       // EnrollmentUID is the row used for the optimistic-lock demo.
	InitialVersion      int64 `json:"initial_version"`      // InitialVersion is the stale snapshot version captured before another writer commits.
	ConcurrentVersion   int64 `json:"concurrent_version"`   // ConcurrentVersion is the version after the competing update commits.
	FinalVersion        int64 `json:"final_version"`        // FinalVersion is the version after the retried transaction succeeds.
	Attempts            int   `json:"attempts"`             // Attempts is the number of transaction attempts needed before success.
	DeletedSuccessfully bool  `json:"deleted_successfully"` // DeletedSuccessfully reports whether cleanup removed the demo row.
}

type prerequisiteRow struct {
	CourseTitle string
	// PrerequisiteTitle comes from a LEFT JOIN, so it can be NULL.
	PrerequisiteTitle sql.NullString
}

type trackMetricRow struct {
	Track           string
	EnrollmentCount int64
	AverageScore    float64
}

type namedRow struct {
	Name string
}

// RunQuickstart bundles the three smallest day-to-day Academy demos:
// generated CRUD helpers, keyword search with paging, and a basic joined list query.
func RunQuickstart(ctx context.Context, runtime *tsq.Runtime) (*QuickstartSummary, error) {
	crud, err := runTrackCRUDDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "track crud", err)
	}

	search, err := runCatalogSearchDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "catalog search", err)
	}

	catalog, err := runBackendCatalogDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "backend catalog", err)
	}

	return &QuickstartSummary{
		TrackCRUD:      *crud,
		CatalogSearch:  *search,
		BackendCatalog: *catalog,
	}, nil
}

// RunAdvanced collects focused feature demos that each highlight one TSQ capability
// in a realistic Academy reporting or batch-processing scenario.
func RunAdvanced(ctx context.Context, runtime *tsq.Runtime) (*AdvancedSummary, error) {
	alias, err := runAliasDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "alias demo", err)
	}

	aggregate, err := runAggregateDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "aggregate demo", err)
	}

	listParam, err := runListParamDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "dynamic in demo", err)
	}

	subquery, err := runSubqueryDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "subquery demo", err)
	}

	caseExpr, err := runCaseDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "case demo", err)
	}

	cte, err := runCTEDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "cte demo", err)
	}

	setOps, err := runSetOpsDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "set operations demo", err)
	}

	batch, err := runBatchDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "batch demo", err)
	}

	softDelete, err := runSoftDeleteDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "soft delete demo", err)
	}

	optimisticLock, err := runOptimisticLockDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "optimistic lock demo", err)
	}

	databaseFilled, err := runDatabaseFilledDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "database filled demo", err)
	}

	fullText, err := runFullTextDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "full text demo", err)
	}

	return &AdvancedSummary{
		Alias:          *alias,
		Aggregate:      aggregate,
		ListParam:      *listParam,
		Subquery:       *subquery,
		Case:           *caseExpr,
		CTE:            *cte,
		SetOps:         *setOps,
		Batch:          *batch,
		SoftDelete:     *softDelete,
		OptimisticLock: *optimisticLock,
		DatabaseFilled: *databaseFilled,
		FullText:       *fullText,
	}, nil
}

// runFullTextDemo searches the course catalog through the declared full-text index.
// MySQL runs MATCH ... AGAINST and PostgreSQL to_tsvector @@ plainto_tsquery; SQLite
// has no index TSQ manages, so the same predicate matches the term as a substring.
func runFullTextDemo(ctx context.Context, runtime *tsq.Runtime) (*FullTextSummary, error) {
	exec := runtime
	term := "sqlite"

	query, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		Where(tsq.Matches(TableCourse.FullText(), tsq.Val(term))).
		OrderBy(Course_Title.Asc()).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build full text query", err)
	}

	courses, err := query.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(courses))
	for _, course := range courses {
		titles = append(titles, course.Title)
	}

	return &FullTextSummary{
		Term:   term,
		Titles: titles,
		Native: runtime.Dialect().SupportsCapability(tsqdialect.CapabilityFullTextSearch),
	}, nil
}

// runDatabaseFilledDemo inserts a course without its currency or slug: the first
// has a DEFAULT, the second is generated by the database from the title. Insert
// leaves both out of the statement and reads them back.
func runDatabaseFilledDemo(ctx context.Context, runtime *tsq.Runtime) (*DatabaseFilledSummary, error) {
	exec := runtime

	course := &Course{
		TrackID:        1,
		InstructorID:   1,
		Title:          "Database Filled Columns",
		Summary:        "Temporary course used to demonstrate DEFAULT and generated columns.",
		Level:          CourseLevelFoundations,
		ListPriceCents: 1000,
	}
	if err := course.Insert(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "insert course", err)
	}

	// A generated column is never written, not even by Update.
	course.Title = "Database Filled Columns v2"
	if err := course.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "update course", err)
	}

	stored, err := QueryCourseByID.Get(ctx, exec, Course_ID.Bind(course.ID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "reload course", err)
	}

	if err := course.HardDelete(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "delete course", err)
	}

	return &DatabaseFilledSummary{Currency: course.Currency, Slug: stored.Slug}, nil
}

// RunFullSuite executes the whole teaching path and ends with the LearningJourney
// result projection, which is the most complete query in the examples suite.
func RunFullSuite(ctx context.Context, runtime *tsq.Runtime) (*FullSuiteSummary, error) {
	quickstart, err := RunQuickstart(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "quickstart", err)
	}

	advanced, err := RunAdvanced(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "advanced", err)
	}

	comprehensive, err := runComprehensive(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "comprehensive", err)
	}

	return &FullSuiteSummary{
		Quickstart:    *quickstart,
		Advanced:      *advanced,
		Comprehensive: *comprehensive,
	}, nil
}

// runComprehensive builds the final Academy "learning journey" board:
// filter a learner set and track set, then page through the joined Result rows.
func runComprehensive(ctx context.Context, runtime *tsq.Runtime) (*ComprehensiveSummary, error) {
	exec := runtime
	learnerIDs := []int64{1, 3, 5}
	tracks := []string{"Backend Engineering", "Data & AI"}

	pageReq := &tsq.PageRequest{
		Page:    1,
		Size:    4,
		OrderBy: "learner_id,enrollment_id",
		Order:   "asc,asc",
	}
	if err := pageReq.Validate(runtime.MaxPageSize()); err != nil {
		return nil, err
	}

	// A handler turns the request into a Paging, naming the columns clients may sort by.
	paging, err := pageReq.Paging(LearningJourney_LearnerID, LearningJourney_EnrollmentID)
	if err != nil {
		return nil, err
	}

	resp, err := PageLearningJourney(ctx, exec, paging, learnerIDs, tracks...)
	if err != nil {
		return nil, err
	}

	var first *LearningJourney
	if len(resp.Data) > 0 {
		first = resp.Data[0]
	}

	return &ComprehensiveSummary{
		LearnerIDs: learnerIDs,
		Tracks:     tracks,
		Total:      resp.Total,
		First:      first,
	}, nil
}

// runTrackCRUDDemo shows the smallest generated-helper loop on a business entity:
// create a track, update its description, then delete it again.
func runTrackCRUDDemo(ctx context.Context, runtime *tsq.Runtime) (*CRUDSummary, error) {
	exec := runtime
	// Create a track.
	inserted := &Track{
		Name:        "Edge Delivery Systems",
		Description: "Temporary track used to demonstrate Insert, Update, and Delete.",
		SkillItems:  json.RawMessage(`[{"name":"Global Cache Invalidation","focus":"consistency"},{"name":"Request Hedging","focus":"latency"}]`),
	}
	if err := inserted.Insert(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "insert track", err)
	}

	// Update the track.
	inserted.Description = "Updated through the generated Track helpers."

	inserted.SkillItems = json.RawMessage(`[{"name":"Global Cache Invalidation","focus":"consistency"},{"name":"Request Hedging","focus":"latency"},{"name":"Regional Traffic Steering","focus":"resilience"}]`)
	if err := inserted.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "update track", err)
	}

	// Upsert by the unique name: the existing track is updated, and its ID is
	// read back into the new value.
	upserted := &Track{
		Name:        inserted.Name,
		Description: "Updated through an upsert by name.",
		SkillItems:  inserted.SkillItems,
	}
	if err := TableTrack.Upsert(ctx, exec, upserted, Track_Name); err != nil {
		return nil, fmt.Errorf("%s: %w", "upsert track", err)
	}

	// Look up the track to verify the update.
	// Generated Query values can also load a single record by primary key:
	//   updated, err := QueryTrackByID.Get(ctx, exec, Track_ID.Bind(inserted.ID))

	query, err := tsq.
		Select(Track__Cols...).
		From(TableTrack).
		Where(Track_ID.EQ(tsq.Val(inserted.ID))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build track lookup", err)
	}

	updated, err := query.Get(ctx, exec)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "get updated track", err)
	}

	// Delete the track.
	if err := inserted.Delete(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "delete track", err)
	}

	// Look up the track to verify the delete.
	deleted, err := query.Find(ctx, exec)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "verify deleted track", err)
	}

	return &CRUDSummary{
		InsertedID:          inserted.ID,
		UpsertedSameRow:     upserted.ID == inserted.ID && updated.Description == upserted.Description,
		UpdatedDescription:  updated.Description,
		UpdatedSkillItems:   updated.SkillItems,
		DeletedSuccessfully: deleted == nil,
	}, nil
}

// runCatalogSearchDemo demonstrates keyword search and paging over the public
// course catalog by searching for SQLite-themed classes.
func runCatalogSearchDemo(ctx context.Context, runtime *tsq.Runtime) (*SearchSummary, error) {
	exec := runtime

	pageReq := &tsq.PageRequest{
		Page:    1,
		Size:    5,
		OrderBy: "id", // OrderBy uses the JSON tag because callers (e.g. the frontend) only see JSON tags, not column names or db tags.
		Order:   "asc",
		Keyword: "SQLite",
	}
	if err := pageReq.Validate(runtime.MaxPageSize()); err != nil {
		return nil, err
	}

	paging, err := pageReq.Paging(Course_ID, Course_Title)
	if err != nil {
		return nil, err
	}

	// The search term is an argument like any other; an empty one searches nothing.
	resp, err := QueryCourse.Page(ctx, exec, paging, tsq.Keyword(pageReq.Keyword))
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(resp.Data))
	for _, course := range resp.Data {
		titles = append(titles, course.Title)
	}

	// Keyset paging reads the same rows by position instead of offset; the order
	// ends with the primary key so every position is unique.
	keyset, err := QueryCourse.PageKeyset(ctx, exec, tsq.Keyset{
		Size:    pageReq.Size,
		OrderBy: []tsq.OrderBy{Course_ID.Asc()},
	}, tsq.Keyword(pageReq.Keyword))
	if err != nil {
		return nil, err
	}

	if len(keyset.Data) != len(resp.Data) {
		return nil, fmt.Errorf("keyset page has %d rows, offset page %d", len(keyset.Data), len(resp.Data))
	}

	return &SearchSummary{
		Keyword: "SQLite",
		Total:   resp.Total,
		Titles:  titles,
	}, nil
}

// runBackendCatalogDemo is the simplest hand-written query builder example:
// list the published courses for the Backend Engineering track.
func runBackendCatalogDemo(ctx context.Context, runtime *tsq.Runtime) (*CatalogSummary, error) {
	exec := runtime

	query, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		LeftJoin(TableTrack, Course_TrackID.EQ(Track_ID)).
		Where(
			Track_Name.EQ(tsq.Val("Backend Engineering")),
			Course_Published.EQ(tsq.Val(true)),
		).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build backend catalog query", err)
	}

	courses, err := query.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(courses))
	for _, course := range courses {
		titles = append(titles, course.Title)
	}

	sort.Strings(titles)

	return &CatalogSummary{
		Track:  "Backend Engineering",
		Titles: titles,
	}, nil
}

// runAliasDemo shows how to rebind the course table so one query can read both a
// course and its prerequisite title.
func runAliasDemo(ctx context.Context, runtime *tsq.Runtime) (*AliasSummary, error) {
	exec := runtime
	prerequisiteAlias := "prerequisite_course"
	prerequisiteID := Course_ID.As(prerequisiteAlias)

	courseTitle := tsq.MapInto(Course_Title, func(holder *prerequisiteRow) *string {
		return &holder.CourseTitle
	}, "course_title")
	prerequisiteTitle := tsq.MapIntoNull(Course_Title.As(prerequisiteAlias), func(holder *prerequisiteRow) *sql.NullString {
		return &holder.PrerequisiteTitle
	}, "prerequisite_title")

	query, err := tsq.
		Select(courseTitle, prerequisiteTitle).
		From(TableCourse).
		LeftJoin(prerequisiteID.Table(), Course_PrerequisiteID.EQ(prerequisiteID)).
		Where(Course_Title.EQ(tsq.Val("API Design Workshop"))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build alias query", err)
	}

	row, err := query.Get(ctx, exec)
	if err != nil {
		return nil, err
	}

	return &AliasSummary{
		CourseTitle:       row.CourseTitle,
		PrerequisiteTitle: row.PrerequisiteTitle.String,
	}, nil
}

// runAggregateDemo turns enrollment rows into track-level metrics by combining
// aggregate functions, GroupBy, and Having.
func runAggregateDemo(ctx context.Context, runtime *tsq.Runtime) ([]AggregateSummary, error) {
	exec := runtime
	trackName := tsq.MapInto(Track_Name, func(holder *trackMetricRow) *string {
		return &holder.Track
	}, "track")
	enrollmentCount := tsq.MapInto(tsq.Count(Enrollment_UID), func(holder *trackMetricRow) *int64 {
		return &holder.EnrollmentCount
	}, "enrollment_count")
	averageScore := tsq.MapInto(tsq.Avg(Enrollment_Score), func(holder *trackMetricRow) *float64 {
		return &holder.AverageScore
	}, "average_score")

	query, err := tsq.
		Select(trackName, enrollmentCount, averageScore).
		From(TableTrack).
		// The WHERE on enrollment status drops the rows a LEFT JOIN would add, and
		// the averaged score would read them as NULL; say INNER JOIN outright.
		Join(TableCourse, Track_ID.EQ(Course_TrackID)).
		Join(TableEnrollment, Course_ID.EQ(Enrollment_CourseID)).
		Where(tsq.Or(
			Enrollment_Status.EQ(tsq.Val(EnrollmentStatusActive)),
			Enrollment_Status.EQ(tsq.Val(EnrollmentStatusCompleted)),
		)).
		GroupBy(Track_Name).
		Having(tsq.Count(Enrollment_UID).GT(tsq.Val(int64(0)))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build aggregate query", err)
	}

	// Iter scans one row at a time instead of loading the whole result first.
	var summaries []AggregateSummary

	for row, err := range query.Iter(ctx, exec) {
		if err != nil {
			return nil, err
		}

		summaries = append(summaries, AggregateSummary{
			Track:           row.Track,
			EnrollmentCount: row.EnrollmentCount,
			AverageScore:    row.AverageScore,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Track < summaries[j].Track
	})

	return summaries, nil
}

// runListParamDemo demonstrates an IN list bound at execution used when callers
// provide a runtime-sized list of course IDs.
func runListParamDemo(ctx context.Context, runtime *tsq.Runtime) (*ListParamSummary, error) {
	exec := runtime

	query, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		Where(Course_ID.In(Course_ID.ListParam())).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build dynamic in query", err)
	}

	courseIDs := []int64{1, 4, 6}

	courses, err := query.List(ctx, exec, Course_ID.BindList(courseIDs...))
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(courses))
	for _, course := range courses {
		titles = append(titles, course.Title)
	}

	sort.Strings(titles)

	return &ListParamSummary{
		CourseIDs: courseIDs,
		Titles:    titles,
	}, nil
}

// runSubqueryDemo stacks two business filters on top of subqueries:
// "learners enrolled in Data & AI" and "courses cheaper than a reference course".
func runSubqueryDemo(ctx context.Context, runtime *tsq.Runtime) (*SubquerySummary, error) {
	exec := runtime

	dataTrackIDSubquery, err := tsq.BuildSubquery(
		tsq.
			Select(Track_ID).
			From(TableTrack).
			Where(Track_Name.EQ(tsq.Val("Data & AI"))),
		Track_ID,
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build data track id subquery", err)
	}

	dataTrackLearnerIDs, err := tsq.BuildSubquery(
		tsq.
			Select(Enrollment_LearnerID).
			From(TableEnrollment).
			LeftJoin(TableCourse, Enrollment_CourseID.EQ(Course_ID)).
			Where(Course_TrackID.In(dataTrackIDSubquery)),
		Enrollment_LearnerID,
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build learner ids in data track subquery", err)
	}

	learnersInDataTrackQuery, err := tsq.
		Select(Learner__Cols...).
		From(TableLearner).
		Where(Learner_ID.In(dataTrackLearnerIDs)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build learners in data track query", err)
	}

	learnersInDataTrack, err := learnersInDataTrackQuery.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	anchorPriceSubquery, err := tsq.BuildSubquery(
		tsq.
			Select(Course_ListPriceCents).
			From(TableCourse).
			Where(Course_Title.EQ(tsq.Val("Retrieval Systems with SQLite"))),
		Course_ListPriceCents,
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build anchor price subquery", err)
	}

	coursesCheaperThanAnchorQuery, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		Where(Course_ListPriceCents.LT(anchorPriceSubquery)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build cheaper courses query", err)
	}

	coursesCheaperThanAnchor, err := coursesCheaperThanAnchorQuery.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	learnerNames := make([]string, 0, len(learnersInDataTrack))
	for _, learner := range learnersInDataTrack {
		learnerNames = append(learnerNames, learner.Name)
	}

	sort.Strings(learnerNames)

	courseTitles := make([]string, 0, len(coursesCheaperThanAnchor))
	for _, course := range coursesCheaperThanAnchor {
		courseTitles = append(courseTitles, course.Title)
	}

	sort.Strings(courseTitles)

	return &SubquerySummary{
		LearnersInDataTrack:      learnerNames,
		CoursesCheaperThanAnchor: courseTitles,
	}, nil
}

// runCaseDemo maps raw enrollment states into user-facing labels with CASE WHEN.
func runCaseDemo(ctx context.Context, runtime *tsq.Runtime) (*CaseSummary, error) {
	exec := runtime
	labelExpr := tsq.
		Case[string]().
		When(tsq.And(
			Enrollment_Status.EQ(tsq.Val(EnrollmentStatusCompleted)),
			Enrollment_Score.GTE(tsq.Val(int64(90))),
		), tsq.Val("excellent")).
		When(tsq.And(
			Enrollment_Status.EQ(tsq.Val(EnrollmentStatusActive)),
			Enrollment_Score.GTE(tsq.Val(int64(80))),
		), tsq.Val("on_track")).
		When(Enrollment_Status.EQ(tsq.Val(EnrollmentStatusWaitlisted)), tsq.Val("waitlist")).
		Else(tsq.Val("watchlist")).
		End()

	label := tsq.MapInto(labelExpr, func(holder *namedRow) *string {
		return &holder.Name
	}, "label")

	query, err := tsq.
		Select(label).
		From(TableEnrollment).
		Where(Enrollment_LearnerID.EQ(tsq.Val(int64(1)))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build case query", err)
	}

	rows, err := query.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	labels := make([]string, 0, len(rows))
	for _, row := range rows {
		labels = append(labels, row.Name)
	}

	sort.Strings(labels)

	return &CaseSummary{Labels: labels}, nil
}

// runCTEDemo shows a non-recursive CTE that first names a filtered course subset
// and then queries it like a normal table.
func runCTEDemo(ctx context.Context, runtime *tsq.Runtime) (*CTESummary, error) {
	exec := runtime
	platformCatalog := tsq.CTE(
		"platform_catalog",
		tsq.Select(Course_ID, Course_Title).
			From(TableCourse).
			LeftJoin(TableTrack, Course_TrackID.EQ(Track_ID)).
			Where(
				Track_Name.EQ(tsq.Val("Platform Reliability")),
				Course_Published.EQ(tsq.Val(true)),
			),
	)

	platformCourseID := Course_ID.WithTable(platformCatalog)
	platformCourseTitle := tsq.MapInto(Course_Title.WithTable(platformCatalog), func(holder *namedRow) *string {
		return &holder.Name
	}, "name")

	query, err := tsq.
		Select(platformCourseTitle).
		From(platformCatalog).
		Where(platformCourseID.GT(tsq.Val(int64(0)))).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build cte query", err)
	}

	rows, err := query.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(rows))
	for _, row := range rows {
		titles = append(titles, row.Name)
	}

	sort.Strings(titles)

	total, err := query.Count(ctx, exec)
	if err != nil {
		return nil, err
	}

	return &CTESummary{
		Track:  "Platform Reliability",
		Total:  total,
		Titles: titles,
	}, nil
}

// runSetOpsDemo demonstrates set composition for course catalogs:
// union two tracks, then exclude courses that require prerequisites.
func runSetOpsDemo(ctx context.Context, runtime *tsq.Runtime) (*SetOpsSummary, error) {
	exec := runtime
	courseTitle := tsq.MapInto(Course_Title, func(holder *namedRow) *string {
		return &holder.Name
	}, "name")

	unionQuery, err := tsq.
		Select(courseTitle).
		From(TableCourse).
		LeftJoin(TableTrack, Course_TrackID.EQ(Track_ID)).
		Where(Track_Name.EQ(tsq.Val("Backend Engineering"))).
		Union(
			tsq.Select(courseTitle).
				From(TableCourse).
				LeftJoin(TableTrack, Course_TrackID.EQ(Track_ID)).
				Where(Track_Name.EQ(tsq.Val("Platform Reliability"))),
		).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build union query", err)
	}

	unionRows, err := unionQuery.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	exceptQuery, err := tsq.
		Select(courseTitle).
		From(TableCourse).
		Except(
			tsq.Select(courseTitle).
				From(TableCourse).
				Where(Course_PrerequisiteID.GT(tsq.Val(int64(0)))),
		).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build except query", err)
	}

	exceptRows, err := exceptQuery.List(ctx, exec)
	if err != nil {
		return nil, err
	}

	unionTitles := make([]string, 0, len(unionRows))
	for _, row := range unionRows {
		unionTitles = append(unionTitles, row.Name)
	}

	sort.Strings(unionTitles)

	starterTitles := make([]string, 0, len(exceptRows))
	for _, row := range exceptRows {
		starterTitles = append(starterTitles, row.Name)
	}

	sort.Strings(starterTitles)

	return &SetOpsSummary{
		UnionTitles:   unionTitles,
		StarterTitles: starterTitles,
	}, nil
}

// runBatchDemo simulates a batch enrollment workflow where records are inserted,
// updated, and removed in bounded batches inside one explicit transaction helper.
func runBatchDemo(ctx context.Context, runtime *tsq.Runtime) (*BatchSummary, error) {
	exec := runtime

	before, err := QueryEnrollment.Count(ctx, exec)
	if err != nil {
		return nil, err
	}

	enrollments := []*Enrollment{
		{
			LearnerID: 5,
			CourseID:  1,
			Status:    EnrollmentStatusActive,
			Score:     76,
			FeeCents:  120000,
		},
		{
			LearnerID: 5,
			CourseID:  5,
			Status:    EnrollmentStatusActive,
			Score:     81,
			FeeCents:  90000,
		},
		{
			LearnerID: 4,
			CourseID:  3,
			Status:    EnrollmentStatusWaitlisted,
			Score:     0,
			FeeCents:  150000,
		},
	}

	if err := runtime.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.Executor) error {
		if err := TableEnrollment.BatchInsert(ctx, txExec, enrollments, tsq.WithBatchSize(2)); err != nil {
			return err
		}

		for _, enrollment := range enrollments {
			if enrollment.Status == EnrollmentStatusWaitlisted {
				enrollment.Status = EnrollmentStatusActive
				enrollment.Score = 72

				continue
			}

			enrollment.Score += 3
		}

		if err := TableEnrollment.BatchUpdate(ctx, txExec, enrollments, tsq.WithBatchSize(2)); err != nil {
			return err
		}

		if err := TableEnrollment.BatchDelete(ctx, txExec, enrollments[:1], tsq.WithBatchSize(1)); err != nil {
			return err
		}

		remainingIDs := make([]int64, 0, len(enrollments)-1)
		for _, enrollment := range enrollments[1:] {
			remainingIDs = append(remainingIDs, enrollment.UID)
		}

		if err := TableEnrollment.BatchDeleteByPK(
			ctx,
			txExec,
			Enrollment_UID.BindList(remainingIDs...),
			tsq.WithBatchSize(2),
		); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	after, err := QueryEnrollment.Count(ctx, exec)
	if err != nil {
		return nil, err
	}

	return &BatchSummary{
		Inserted: int64(len(enrollments)),
		Updated:  int64(len(enrollments)),
		Deleted:  int64(len(enrollments)),
		Before:   before,
		After:    after,
	}, nil
}

// runSoftDeleteDemo walks a row through the whole soft-delete lifecycle.
//
// Enrollment declares deleted_at, so Delete stamps a tombstone instead of
// removing the row: it leaves every query while staying in the table,
// clearing the tombstone brings it back, and HardDelete is what actually
// removes it.
func runSoftDeleteDemo(ctx context.Context, runtime *tsq.Runtime) (*SoftDeleteSummary, error) {
	exec := runtime

	// Every row of the table, tombstoned or not. A soft-delete table leaves deleted
	// rows out of every query unless the query says WithDeleted.
	storedByUID := tsq.
		Select(Enrollment__Cols...).
		From(TableEnrollment.WithDeleted()).
		Where(Enrollment_UID.EQ(Enrollment_UID.Param())).
		MustBuild()

	row := &Enrollment{
		LearnerID: 5,
		CourseID:  1,
		Status:    EnrollmentStatusActive,
		Score:     70,
		FeeCents:  90000,
	}
	if err := row.Insert(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "insert enrollment", err)
	}

	visible, err := QueryEnrollmentByUID.Find(ctx, exec, Enrollment_UID.Bind(row.UID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load enrollment before delete", err)
	}

	summary := &SoftDeleteSummary{
		EnrollmentUID: row.UID,
		ActiveBefore:  row.Active(),
		VisibleBefore: visible != nil,
	}

	if err := row.Delete(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "soft-delete enrollment", err)
	}

	summary.ActiveAfter = row.Active()

	visible, err = QueryEnrollmentByUID.Find(ctx, exec, Enrollment_UID.Bind(row.UID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load enrollment after delete", err)
	}

	summary.VisibleAfter = visible != nil

	stored, err := storedByUID.Find(ctx, exec, Enrollment_UID.Bind(row.UID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load stored enrollment after delete", err)
	}

	summary.StoredAfter = stored != nil

	// Update never writes deleted_at, so a deleted row comes back only through
	// Restore.
	if err := stored.Restore(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "restore enrollment", err)
	}

	visible, err = QueryEnrollmentByUID.Find(ctx, exec, Enrollment_UID.Bind(row.UID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load enrollment after restore", err)
	}

	summary.VisibleRestore = visible != nil

	if err := stored.HardDelete(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "hard-delete enrollment", err)
	}

	stored, err = storedByUID.Find(ctx, exec, Enrollment_UID.Bind(row.UID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load stored enrollment after hard delete", err)
	}

	summary.StoredFinal = stored != nil

	return summary, nil
}

// runOptimisticLockDemo shows a stale snapshot failing with OptimisticLockError,
// then Runtime.WithTxResult retrying and succeeding after reloading the row. Row-lock
// reads are not run because the examples use SQLite, which has no FOR UPDATE.
func runOptimisticLockDemo(ctx context.Context, runtime *tsq.Runtime) (*OptimisticLockSummary, error) {
	exec := runtime

	inserted := &Enrollment{
		LearnerID: 4,
		CourseID:  2,
		Status:    EnrollmentStatusActive,
		Score:     88,
		FeeCents:  110000,
	}
	if err := inserted.Insert(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "insert enrollment", err)
	}

	stale, err := QueryEnrollmentByUID.Get(ctx, exec, Enrollment_UID.Bind(inserted.UID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load stale enrollment snapshot", err)
	}

	concurrent, err := QueryEnrollmentByUID.Get(ctx, exec, Enrollment_UID.Bind(inserted.UID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load competing enrollment snapshot", err)
	}

	concurrent.Score = 91
	if err := concurrent.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "commit competing enrollment update", err)
	}

	attempts := 0

	summary, err := runtime.WithTxResult(ctx, &tsq.TxOptions{RetryIf: tsq.IsOptimisticLockError}, func(ctx context.Context, txExec tsq.Executor) (*OptimisticLockSummary, error) {
		attempts++

		if attempts == 1 {
			staleAttempt := *stale
			staleAttempt.Score = 95

			staleAttempt.Status = EnrollmentStatusCompleted
			if err := staleAttempt.Update(ctx, txExec); err != nil {
				return nil, err
			}

			return nil, fmt.Errorf("%s", "expected stale snapshot to trigger optimistic lock retry")
		}

		loaded, err := QueryEnrollmentByUID.Get(ctx, txExec, Enrollment_UID.Bind(inserted.UID))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", "reload enrollment after retry", err)
		}

		loaded.Score = 95

		loaded.Status = EnrollmentStatusCompleted
		if err := loaded.Update(ctx, txExec); err != nil {
			return nil, fmt.Errorf("%s: %w", "update fresh enrollment after retry", err)
		}
		finalVersion := loaded.Version

		// HardDelete, not Delete: this is cleanup, and Enrollment declares
		// deleted_at, so Delete would leave a tombstoned row behind.
		if err := loaded.HardDelete(ctx, txExec); err != nil {
			return nil, fmt.Errorf("%s: %w", "hard-delete fresh enrollment", err)
		}

		deleted, err := QueryEnrollmentByUID.Find(ctx, txExec, Enrollment_UID.Bind(loaded.UID))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", "verify deleted enrollment", err)
		}

		return &OptimisticLockSummary{
			EnrollmentUID:       loaded.UID,
			InitialVersion:      stale.Version,
			ConcurrentVersion:   concurrent.Version,
			FinalVersion:        finalVersion,
			Attempts:            attempts,
			DeletedSuccessfully: deleted == nil,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return summary, nil
}
