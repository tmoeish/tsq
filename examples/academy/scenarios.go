package academy

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/tmoeish/tsq/v4"
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
	InVar          InVarSummary          `json:"dynamic_in"`         // InVar summarizes runtime slice binding.
	Subquery       SubquerySummary       `json:"subquery"`           // Subquery summarizes scalar and membership subquery usage.
	Case           CaseSummary           `json:"case_labels"`        // Case summarizes CASE-expression labeling.
	CTE            CTESummary            `json:"cte"`                // CTE summarizes common-table-expression queries.
	SetOps         SetOpsSummary         `json:"set_ops"`            // SetOps summarizes UNION and INTERSECT style queries.
	Chunked        ChunkedSummary        `json:"chunked"`            // Chunked summarizes chunked write helpers.
	OptimisticLock OptimisticLockSummary `json:"optimistic_lock"`    // OptimisticLock summarizes version-guarded writes.
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
	InsertedID          int64  `json:"inserted_id"`          // InsertedID is the generated ID returned by the insert demo.
	UpdatedDescription  string `json:"updated_description"`  // UpdatedDescription is the description after the update demo.
	DeletedSuccessfully bool   `json:"deleted_successfully"` // DeletedSuccessfully reports whether the delete demo removed the row.
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

// InVarSummary captures the runtime slice binding demo result.
type InVarSummary struct {
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
	Total  int      `json:"total"`  // Total is the number of returned titles.
	Titles []string `json:"titles"` // Titles lists the titles returned from the CTE query.
}

// SetOpsSummary captures the set-operation demo result.
type SetOpsSummary struct {
	UnionTitles   []string `json:"union_titles"`   // UnionTitles lists titles returned by the UNION query.
	StarterTitles []string `json:"starter_titles"` // StarterTitles lists titles returned by the INTERSECT query.
}

// ChunkedSummary captures the chunked write demo result.
type ChunkedSummary struct {
	Inserted int64 `json:"inserted"` // Inserted is the number of rows inserted by the chunked demo.
	Updated  int64 `json:"updated"`  // Updated is the number of rows updated by the chunked demo.
	Deleted  int64 `json:"deleted"`  // Deleted is the number of rows deleted by the chunked demo.
	Before   int   `json:"before"`   // Before is the number of rows loaded before the update step.
	After    int   `json:"after"`    // After is the number of rows remaining after cleanup.
}

// OptimisticLockSummary captures the optimistic-lock demo result.
type OptimisticLockSummary struct {
	EnrollmentUID       int64 `json:"enrollment_uid"`       // EnrollmentUID is the row used for the optimistic-lock demo.
	InitialVersion      int64 `json:"initial_version"`      // InitialVersion is the row version before the update.
	UpdatedVersion      int64 `json:"updated_version"`      // UpdatedVersion is the row version after the successful update.
	ConflictDetected    bool  `json:"conflict_detected"`    // ConflictDetected reports whether the stale write failed as expected.
	DeletedSuccessfully bool  `json:"deleted_successfully"` // DeletedSuccessfully reports whether cleanup removed the demo row.
}

type prerequisiteRow struct {
	CourseTitle       string
	PrerequisiteTitle string
}

// TSQOwner marks prerequisiteRow as an internal projection owner.
func (prerequisiteRow) TSQOwner() {}

type trackMetricRow struct {
	Track           string
	EnrollmentCount int64
	AverageScore    float64
}

// TSQOwner marks trackMetricRow as an internal projection owner.
func (trackMetricRow) TSQOwner() {}

type namedRow struct {
	Name string
}

// TSQOwner marks namedRow as an internal projection owner.
func (namedRow) TSQOwner() {}

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

	inVar, err := runInVarDemo(ctx, runtime)
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

	chunked, err := runChunkedDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "chunked demo", err)
	}

	optimisticLock, err := runOptimisticLockDemo(ctx, runtime)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "optimistic lock demo", err)
	}

	return &AdvancedSummary{
		Alias:          *alias,
		Aggregate:      aggregate,
		InVar:          *inVar,
		Subquery:       *subquery,
		Case:           *caseExpr,
		CTE:            *cte,
		SetOps:         *setOps,
		Chunked:        *chunked,
		OptimisticLock: *optimisticLock,
	}, nil
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
	exec := runtime.Executor()
	learnerIDs := []int64{1, 3, 5}
	tracks := []string{"Backend Engineering", "Data & AI"}

	pageReq := &tsq.PageRequest{
		Page:    1,
		Size:    4,
		OrderBy: "learner_id,enrollment_id",
		Order:   "asc,asc",
	}
	if err := pageReq.Validate(); err != nil {
		return nil, err
	}

	resp, err := PageLearningJourney(ctx, exec, pageReq, learnerIDs, tracks...)
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
	exec := runtime.Executor()
	// Create a track.
	inserted := &Track{
		Name:        "Edge Delivery Systems",
		Description: "Temporary track used to demonstrate Insert, Update, and Delete.",
	}
	if err := inserted.Insert(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "insert track", err)
	}

	// Update the track.
	inserted.Description = "Updated through the generated Track helpers."
	if err := inserted.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "update track", err)
	}

	// Look up the track to verify the update.
	// You can also use GetXXByPK to get a single record by its primary key:
	//   updated, err := GetTrackByID(ctx, exec, inserted.ID)

	query, err := tsq.
		Select(Track__Cols...).
		From(TableTrack).
		Where(Track_ID.EQ(inserted.ID)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build track lookup", err)
	}

	updated, err := tsq.GetOrErr(ctx, exec, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "get updated track", err)
	}

	// Delete the track.
	if err := inserted.Delete(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "delete track", err)
	}

	// Look up the track to verify the delete.
	deleted, err := tsq.Get(ctx, exec, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "verify deleted track", err)
	}

	return &CRUDSummary{
		InsertedID:          inserted.ID,
		UpdatedDescription:  updated.Description,
		DeletedSuccessfully: deleted == nil,
	}, nil
}

// runCatalogSearchDemo demonstrates keyword search and paging over the public
// course catalog by searching for SQLite-themed classes.
func runCatalogSearchDemo(ctx context.Context, runtime *tsq.Runtime) (*SearchSummary, error) {
	exec := runtime.Executor()

	pageReq := &tsq.PageRequest{
		Page:    1,
		Size:    5,
		OrderBy: "id", // OrderBy uses the JSON tag because callers (e.g. the frontend) only see JSON tags, not column names or db tags.
		Order:   "asc",
		Keyword: tsq.EscapeKeywordSearch("SQLite"),
	}
	if err := pageReq.Validate(); err != nil {
		return nil, err
	}

	resp, err := PageCourse(ctx, exec, pageReq)
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(resp.Data))
	for _, course := range resp.Data {
		titles = append(titles, course.Title)
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
	exec := runtime.Executor()

	query, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		LeftJoin(TableTrack, Course_TrackID.EQCol(Track_ID)).
		Where(
			Track_Name.EQ("Backend Engineering"),
			Course_Published.EQ(true),
		).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build backend catalog query", err)
	}

	courses, err := tsq.List(ctx, exec, query)
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
	exec := runtime.Executor()
	prerequisiteAlias := "prerequisite_course"
	prerequisiteID := Course_ID.As(prerequisiteAlias)

	courseTitle := tsq.MapInto(Course_Title, func(holder *prerequisiteRow) *string {
		return &holder.CourseTitle
	}, "course_title")
	prerequisiteTitle := tsq.MapInto(Course_Title.As(prerequisiteAlias), func(holder *prerequisiteRow) *string {
		return &holder.PrerequisiteTitle
	}, "prerequisite_title")

	query, err := tsq.
		Select(courseTitle, prerequisiteTitle).
		From(TableCourse).
		LeftJoin(prerequisiteID.Table(), Course_PrerequisiteID.EQCol(prerequisiteID)).
		Where(Course_Title.EQ("API Design Workshop")).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build alias query", err)
	}

	row, err := tsq.GetOrErr(ctx, exec, query)
	if err != nil {
		return nil, err
	}

	return &AliasSummary{
		CourseTitle:       row.CourseTitle,
		PrerequisiteTitle: row.PrerequisiteTitle,
	}, nil
}

// runAggregateDemo turns enrollment rows into track-level metrics by combining
// aggregate functions, GroupBy, and Having.
func runAggregateDemo(ctx context.Context, runtime *tsq.Runtime) ([]AggregateSummary, error) {
	exec := runtime.Executor()
	trackName := tsq.MapInto(Track_Name, func(holder *trackMetricRow) *string {
		return &holder.Track
	}, "track")
	enrollmentCount := tsq.MapInto(Enrollment_UID.Count(), func(holder *trackMetricRow) *int64 {
		return &holder.EnrollmentCount
	}, "enrollment_count")
	averageScore := tsq.MapInto(Enrollment_Score.Avg(), func(holder *trackMetricRow) *float64 {
		return &holder.AverageScore
	}, "average_score")

	query, err := tsq.
		Select(trackName, enrollmentCount, averageScore).
		From(TableTrack).
		LeftJoin(TableCourse, Track_ID.EQCol(Course_TrackID)).
		LeftJoin(TableEnrollment, Course_ID.EQCol(Enrollment_CourseID)).
		Where(tsq.Or(
			Enrollment_Status.EQ(EnrollmentStatusActive),
			Enrollment_Status.EQ(EnrollmentStatusCompleted),
		)).
		GroupBy(Track_Name).
		Having(Enrollment_UID.Count().GT(0)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build aggregate query", err)
	}

	rows, err := tsq.List(ctx, exec, query)
	if err != nil {
		return nil, err
	}

	summaries := make([]AggregateSummary, 0, len(rows))
	for _, row := range rows {
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

// runInVarDemo demonstrates the dynamic IN placeholder flow used when callers
// provide a runtime-sized list of course IDs.
func runInVarDemo(ctx context.Context, runtime *tsq.Runtime) (*InVarSummary, error) {
	exec := runtime.Executor()

	query, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		Where(Course_ID.InVar()).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build dynamic in query", err)
	}

	courseIDs := []int64{1, 4, 6}

	courses, err := tsq.List(ctx, exec, query, courseIDs)
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(courses))
	for _, course := range courses {
		titles = append(titles, course.Title)
	}

	sort.Strings(titles)

	return &InVarSummary{
		CourseIDs: courseIDs,
		Titles:    titles,
	}, nil
}

// runSubqueryDemo stacks two business filters on top of subqueries:
// "learners enrolled in Data & AI" and "courses cheaper than a reference course".
func runSubqueryDemo(ctx context.Context, runtime *tsq.Runtime) (*SubquerySummary, error) {
	exec := runtime.Executor()

	dataTrackIDSubquery, err := tsq.
		Select(Track_ID).
		From(TableTrack).
		Where(Track_Name.EQ("Data & AI")).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build data track id subquery", err)
	}

	dataTrackLearnerIDs, err := tsq.
		Select(Enrollment_LearnerID).
		From(TableEnrollment).
		LeftJoin(TableCourse, Enrollment_CourseID.EQCol(Course_ID)).
		Where(Course_TrackID.InSub(dataTrackIDSubquery)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build learner ids in data track subquery", err)
	}

	learnersInDataTrackQuery, err := tsq.
		Select(Learner__Cols...).
		From(TableLearner).
		Where(Learner_ID.InSub(dataTrackLearnerIDs)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build learners in data track query", err)
	}

	learnersInDataTrack, err := tsq.List(ctx, exec, learnersInDataTrackQuery)
	if err != nil {
		return nil, err
	}

	anchorPriceSubquery, err := tsq.
		Select(Course_ListPriceCents).
		From(TableCourse).
		Where(Course_Title.EQ("Retrieval Systems with SQLite")).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build anchor price subquery", err)
	}

	coursesCheaperThanAnchorQuery, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		Where(Course_ListPriceCents.LTSub(anchorPriceSubquery)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build cheaper courses query", err)
	}

	coursesCheaperThanAnchor, err := tsq.List(ctx, exec, coursesCheaperThanAnchorQuery)
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
	exec := runtime.Executor()
	labelExpr := tsq.
		Case[string]().
		When(
			tsq.And(
				Enrollment_Status.EQ(EnrollmentStatusCompleted),
				Enrollment_Score.GTE(90),
			),
			"excellent",
		).
		When(
			tsq.And(
				Enrollment_Status.EQ(EnrollmentStatusActive),
				Enrollment_Score.GTE(80),
			),
			"on_track",
		).
		When(Enrollment_Status.EQ(EnrollmentStatusWaitlisted), "waitlist").
		Else("watchlist").
		End()

	label := tsq.MapInto(labelExpr, func(holder *namedRow) *string {
		return &holder.Name
	}, "label")

	query, err := tsq.
		Select(label).
		From(TableEnrollment).
		Where(Enrollment_LearnerID.EQ(1)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build case query", err)
	}

	rows, err := tsq.List(ctx, exec, query)
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
	exec := runtime.Executor()
	platformCatalog := tsq.CTE(
		"platform_catalog",
		tsq.Select(Course_ID, Course_Title).
			From(TableCourse).
			LeftJoin(TableTrack, Course_TrackID.EQCol(Track_ID)).
			Where(
				Track_Name.EQ("Platform Reliability"),
				Course_Published.EQ(true),
			),
	)

	platformCourseID := Course_ID.WithTable(platformCatalog)
	platformCourseTitle := tsq.MapInto(Course_Title.WithTable(platformCatalog), func(holder *namedRow) *string {
		return &holder.Name
	}, "name")

	query, err := tsq.
		Select(platformCourseTitle).
		From(platformCatalog).
		Where(platformCourseID.GT(0)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build cte query", err)
	}

	rows, err := tsq.List(ctx, exec, query)
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
	exec := runtime.Executor()
	courseTitle := tsq.MapInto(Course_Title, func(holder *namedRow) *string {
		return &holder.Name
	}, "name")

	unionQuery, err := tsq.
		Select(courseTitle).
		From(TableCourse).
		LeftJoin(TableTrack, Course_TrackID.EQCol(Track_ID)).
		Where(Track_Name.EQ("Backend Engineering")).
		Union(
			tsq.Select(courseTitle).
				From(TableCourse).
				LeftJoin(TableTrack, Course_TrackID.EQCol(Track_ID)).
				Where(Track_Name.EQ("Platform Reliability")),
		).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build union query", err)
	}

	unionRows, err := tsq.List(ctx, exec, unionQuery)
	if err != nil {
		return nil, err
	}

	exceptQuery, err := tsq.
		Select(courseTitle).
		From(TableCourse).
		Except(
			tsq.Select(courseTitle).
				From(TableCourse).
				Where(Course_PrerequisiteID.GT(0)),
		).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build except query", err)
	}

	exceptRows, err := tsq.List(ctx, exec, exceptQuery)
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

// runChunkedDemo simulates a batch enrollment workflow where records are inserted,
// updated, and removed in bounded chunks instead of one huge statement.
func runChunkedDemo(ctx context.Context, runtime *tsq.Runtime) (*ChunkedSummary, error) {
	exec := runtime.Executor()

	before, err := CountEnrollment(ctx, exec)
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

	if err := tsq.ChunkedInsert(ctx, exec, enrollments, &tsq.ChunkedInsertOptions{ChunkSize: 2}); err != nil {
		return nil, err
	}

	for _, enrollment := range enrollments {
		if enrollment.Status == EnrollmentStatusWaitlisted {
			enrollment.Status = EnrollmentStatusActive
			enrollment.Score = 72

			continue
		}

		enrollment.Score += 3
	}

	if err := tsq.ChunkedUpdate(ctx, exec, enrollments, &tsq.ChunkedOptions{ChunkSize: 2}); err != nil {
		return nil, err
	}

	if err := tsq.ChunkedDelete(ctx, exec, enrollments[:1], &tsq.ChunkedOptions{ChunkSize: 1}); err != nil {
		return nil, err
	}

	remainingIDs := make([]any, 0, len(enrollments)-1)
	for _, enrollment := range enrollments[1:] {
		remainingIDs = append(remainingIDs, enrollment.UID)
	}

	if err := tsq.ChunkedDeleteByIDs(
		ctx,
		exec,
		"enrollment",
		"uid",
		remainingIDs,
		&tsq.ChunkedOptions{ChunkSize: 2},
	); err != nil {
		return nil, err
	}

	after, err := CountEnrollment(ctx, exec)
	if err != nil {
		return nil, err
	}

	return &ChunkedSummary{
		Inserted: int64(len(enrollments)),
		Updated:  int64(len(enrollments)),
		Deleted:  int64(len(enrollments)),
		Before:   before,
		After:    after,
	}, nil
}

// runOptimisticLockDemo demonstrates the SQLite-safe part of the new locking model:
// automatic version guarding on Update/Delete. Row-lock reads are intentionally not
// executed here because the examples runtime uses SQLite, which rejects FOR UPDATE /
// FOR SHARE at execution time.
func runOptimisticLockDemo(ctx context.Context, runtime *tsq.Runtime) (*OptimisticLockSummary, error) {
	exec := runtime.Executor()

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

	loaded, err := GetEnrollmentByUIDOrErr(ctx, exec, inserted.UID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "load enrollment", err)
	}
	stale := *loaded

	loaded.Score = 92

	loaded.Status = EnrollmentStatusCompleted
	if err := loaded.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "update fresh enrollment", err)
	}

	stale.Score = 61
	conflictDetected := false

	if err := stale.Update(ctx, exec); err != nil {
		if !errors.Is(err, &tsq.ErrOptimisticLockConflict{}) {
			return nil, fmt.Errorf("%s: %w", "update stale enrollment", err)
		}
		conflictDetected = true
	}

	if err := loaded.Delete(ctx, exec); err != nil {
		return nil, fmt.Errorf("%s: %w", "delete fresh enrollment", err)
	}

	deleted, err := GetEnrollmentByUID(ctx, exec, loaded.UID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "verify deleted enrollment", err)
	}

	return &OptimisticLockSummary{
		EnrollmentUID:       loaded.UID,
		InitialVersion:      stale.Version,
		UpdatedVersion:      loaded.Version,
		ConflictDetected:    conflictDetected,
		DeletedSuccessfully: deleted == nil,
	}, nil
}
