package academy

import (
	"context"
	"fmt"
	"sort"

	"github.com/tmoeish/tsq"
)

type QuickstartSummary struct {
	TrackCRUD      CRUDSummary    `json:"track_crud"`
	CatalogSearch  SearchSummary  `json:"catalog_search"`
	BackendCatalog CatalogSummary `json:"backend_catalog"`
}

type AdvancedSummary struct {
	Alias     AliasSummary       `json:"alias_prerequisite"`
	Aggregate []AggregateSummary `json:"track_metrics"`
	InVar     InVarSummary       `json:"dynamic_in"`
	Subquery  SubquerySummary    `json:"subquery"`
	Case      CaseSummary        `json:"case_labels"`
	CTE       CTESummary         `json:"cte"`
	SetOps    SetOpsSummary      `json:"set_ops"`
	Chunked   ChunkedSummary     `json:"chunked"`
}

type FullSuiteSummary struct {
	Quickstart    QuickstartSummary    `json:"quickstart"`
	Advanced      AdvancedSummary      `json:"advanced"`
	Comprehensive ComprehensiveSummary `json:"comprehensive"`
}

type ComprehensiveSummary struct {
	LearnerIDs []int64          `json:"learner_ids"`
	Tracks     []string         `json:"tracks"`
	Total      int64            `json:"total"`
	First      *LearningJourney `json:"first,omitempty"`
}

type CRUDSummary struct {
	InsertedID          int64  `json:"inserted_id"`
	UpdatedDescription  string `json:"updated_description"`
	DeletedSuccessfully bool   `json:"deleted_successfully"`
}

type SearchSummary struct {
	Keyword string   `json:"keyword"`
	Total   int64    `json:"total"`
	Titles  []string `json:"titles"`
}

type CatalogSummary struct {
	Track  string   `json:"track"`
	Titles []string `json:"titles"`
}

type AliasSummary struct {
	CourseTitle       string `json:"course_title"`
	PrerequisiteTitle string `json:"prerequisite_title"`
}

type AggregateSummary struct {
	Track           string  `json:"track"`
	EnrollmentCount int64   `json:"enrollment_count"`
	AverageScore    float64 `json:"average_score"`
}

type InVarSummary struct {
	CourseIDs []int64  `json:"course_ids"`
	Titles    []string `json:"titles"`
}

type SubquerySummary struct {
	LearnersInDataTrack      []string `json:"learners_in_data_track"`
	CoursesCheaperThanAnchor []string `json:"courses_cheaper_than_anchor"`
}

type CaseSummary struct {
	Labels []string `json:"labels"`
}

type CTESummary struct {
	Track  string   `json:"track"`
	Total  int      `json:"total"`
	Titles []string `json:"titles"`
}

type SetOpsSummary struct {
	UnionTitles   []string `json:"union_titles"`
	StarterTitles []string `json:"starter_titles"`
}

type ChunkedSummary struct {
	Inserted int64 `json:"inserted"`
	Updated  int64 `json:"updated"`
	Deleted  int64 `json:"deleted"`
	Before   int   `json:"before"`
	After    int   `json:"after"`
}

type prerequisiteRow struct {
	CourseTitle       string
	PrerequisiteTitle string
}

func (prerequisiteRow) TSQOwner() {}

type trackMetricRow struct {
	Track           string
	EnrollmentCount int64
	AverageScore    float64
}

func (trackMetricRow) TSQOwner() {}

type namedRow struct {
	Name string
}

func (namedRow) TSQOwner() {}

// RunQuickstart bundles the three smallest day-to-day Academy demos:
// generated CRUD helpers, keyword search with paging, and a basic joined list query.
func RunQuickstart(ctx context.Context, engine *tsq.Engine) (*QuickstartSummary, error) {
	crud, err := runTrackCRUDDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "track crud", err)
	}

	search, err := runCatalogSearchDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "catalog search", err)
	}

	catalog, err := runBackendCatalogDemo(ctx, engine)
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
func RunAdvanced(ctx context.Context, engine *tsq.Engine) (*AdvancedSummary, error) {
	alias, err := runAliasDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "alias demo", err)
	}

	aggregate, err := runAggregateDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "aggregate demo", err)
	}

	inVar, err := runInVarDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "dynamic in demo", err)
	}

	subquery, err := runSubqueryDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "subquery demo", err)
	}

	caseExpr, err := runCaseDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "case demo", err)
	}

	cte, err := runCTEDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "cte demo", err)
	}

	setOps, err := runSetOpsDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "set operations demo", err)
	}

	chunked, err := runChunkedDemo(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "chunked demo", err)
	}

	return &AdvancedSummary{
		Alias:     *alias,
		Aggregate: aggregate,
		InVar:     *inVar,
		Subquery:  *subquery,
		Case:      *caseExpr,
		CTE:       *cte,
		SetOps:    *setOps,
		Chunked:   *chunked,
	}, nil
}

// RunFullSuite executes the whole teaching path and ends with the LearningJourney
// result projection, which is the most complete query in the examples suite.
func RunFullSuite(ctx context.Context, engine *tsq.Engine) (*FullSuiteSummary, error) {
	quickstart, err := RunQuickstart(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "quickstart", err)
	}

	advanced, err := RunAdvanced(ctx, engine)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "advanced", err)
	}

	comprehensive, err := runComprehensive(ctx, engine)
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
func runComprehensive(ctx context.Context, engine *tsq.Engine) (*ComprehensiveSummary, error) {
	learnerIDs := []int64{1, 3, 5}
	tracks := []string{"Backend Engineering", "Data & AI"}

	pageReq := &tsq.PageReq{
		Page:    1,
		Size:    4,
		OrderBy: "learner_id,enrollment_id",
		Order:   "asc,asc",
	}
	if err := pageReq.Validate(); err != nil {
		return nil, err
	}

	resp, err := PageLearningJourney(ctx, engine, pageReq, learnerIDs, tracks...)
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
func runTrackCRUDDemo(ctx context.Context, engine *tsq.Engine) (*CRUDSummary, error) {
	// Create a track.
	inserted := &Track{
		Name:        "Edge Delivery Systems",
		Description: "Temporary track used to demonstrate Insert, Update, and Delete.",
	}
	if err := inserted.Insert(ctx, engine); err != nil {
		return nil, fmt.Errorf("%s: %w", "insert track", err)
	}

	// Update the track.
	inserted.Description = "Updated through the generated Track helpers."
	if err := inserted.Update(ctx, engine); err != nil {
		return nil, fmt.Errorf("%s: %w", "update track", err)
	}

	// Look up the track to verify the update.
	// You can also use GetXXByPK to get a single record by its primary key:
	//   updated, err := GetTrackByID(ctx, engine, inserted.ID)

	query, err := tsq.
		Select(Track__Cols...).
		From(TableTrack).
		Where(Track_ID.EQ(inserted.ID)).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build track lookup", err)
	}

	updated, err := tsq.GetOrErr(ctx, engine, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "get updated track", err)
	}

	// Delete the track.
	if err := inserted.Delete(ctx, engine); err != nil {
		return nil, fmt.Errorf("%s: %w", "delete track", err)
	}

	// Look up the track to verify the delete.
	deleted, err := tsq.Get(ctx, engine, query)
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
func runCatalogSearchDemo(ctx context.Context, engine *tsq.Engine) (*SearchSummary, error) {
	pageReq := &tsq.PageReq{
		Page:    1,
		Size:    5,
		OrderBy: "id", // OrderBy uses the JSON tag because callers (e.g. the frontend) only see JSON tags, not column names or db tags.
		Order:   "asc",
		Keyword: tsq.EscapeKeywordSearch("SQLite"),
	}
	if err := pageReq.Validate(); err != nil {
		return nil, err
	}

	resp, err := PageCourse(ctx, engine, pageReq)
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

// runBackendCatalogDemo is the simplest hand-written QueryBuilder example:
// list the published courses for the Backend Engineering track.
func runBackendCatalogDemo(ctx context.Context, engine *tsq.Engine) (*CatalogSummary, error) {
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

	courses, err := tsq.List(ctx, engine, query)
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
func runAliasDemo(ctx context.Context, engine *tsq.Engine) (*AliasSummary, error) {
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

	row, err := tsq.GetOrErr(ctx, engine, query)
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
func runAggregateDemo(ctx context.Context, engine *tsq.Engine) ([]AggregateSummary, error) {
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

	rows, err := tsq.List(ctx, engine, query)
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
func runInVarDemo(ctx context.Context, engine *tsq.Engine) (*InVarSummary, error) {
	query, err := tsq.
		Select(Course__Cols...).
		From(TableCourse).
		Where(Course_ID.InVar()).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "build dynamic in query", err)
	}

	courseIDs := []int64{1, 4, 6}

	courses, err := tsq.List(ctx, engine, query, courseIDs)
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
func runSubqueryDemo(ctx context.Context, engine *tsq.Engine) (*SubquerySummary, error) {
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

	learnersInDataTrack, err := tsq.List(ctx, engine, learnersInDataTrackQuery)
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

	coursesCheaperThanAnchor, err := tsq.List(ctx, engine, coursesCheaperThanAnchorQuery)
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
func runCaseDemo(ctx context.Context, engine *tsq.Engine) (*CaseSummary, error) {
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

	rows, err := tsq.List(ctx, engine, query)
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
func runCTEDemo(ctx context.Context, engine *tsq.Engine) (*CTESummary, error) {
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

	rows, err := tsq.List(ctx, engine, query)
	if err != nil {
		return nil, err
	}

	titles := make([]string, 0, len(rows))
	for _, row := range rows {
		titles = append(titles, row.Name)
	}

	sort.Strings(titles)

	total, err := query.Count(ctx, engine)
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
func runSetOpsDemo(ctx context.Context, engine *tsq.Engine) (*SetOpsSummary, error) {
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

	unionRows, err := tsq.List(ctx, engine, unionQuery)
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

	exceptRows, err := tsq.List(ctx, engine, exceptQuery)
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
func runChunkedDemo(ctx context.Context, engine *tsq.Engine) (*ChunkedSummary, error) {
	before, err := CountEnrollment(ctx, engine)
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

	if err := tsq.ChunkedInsert(ctx, engine, enrollments, &tsq.ChunkedInsertOptions{ChunkSize: 2}); err != nil {
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

	if err := tsq.ChunkedUpdate(ctx, engine, enrollments, &tsq.ChunkedOptions{ChunkSize: 2}); err != nil {
		return nil, err
	}

	if err := tsq.ChunkedDelete(ctx, engine, enrollments[:1], &tsq.ChunkedOptions{ChunkSize: 1}); err != nil {
		return nil, err
	}

	remainingIDs := make([]any, 0, len(enrollments)-1)
	for _, enrollment := range enrollments[1:] {
		remainingIDs = append(remainingIDs, enrollment.UID)
	}

	if err := tsq.ChunkedDeleteByIDs(
		ctx,
		engine,
		"enrollment",
		"uid",
		remainingIDs,
		&tsq.ChunkedOptions{ChunkSize: 2},
	); err != nil {
		return nil, err
	}

	after, err := CountEnrollment(ctx, engine)
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
