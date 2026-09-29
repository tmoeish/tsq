package academy

// Course is the main catalog entity learners enroll into.
//
//tsq:table name=course pk=ID
//tsq:managed created_at
//tsq:unique Title
//tsq:index TrackID
//tsq:index InstructorID
//tsq:index PrerequisiteID
//tsq:search Title,Summary
//tsq:fulltext Title,Summary
type Course struct {
	ImmutableTable

	// TrackID is the track the course belongs to.
	TrackID int64 `db:"track_id" json:"track_id"`
	// InstructorID is the instructor teaching it.
	InstructorID int64 `db:"instructor_id" json:"instructor_id"`
	// PrerequisiteID is the course to take first; 0 when there is none.
	PrerequisiteID int64 `db:"prerequisite_id" json:"prerequisite_id"`

	// Title is the course title.
	Title string `db:"title,size:160" json:"title"`
	// Summary describes the course.
	Summary string `db:"summary,size:4096" json:"summary"`
	// Level is how advanced the course is.
	Level CourseLevel `db:"level" json:"level"`
	// ListPriceCents is the list price, in cents.
	ListPriceCents int64 `db:"list_price_cents" json:"list_price_cents"`
	// Published reports whether the course is in the catalog.
	Published bool `db:"published" json:"published"`

	// Currency is the price currency; nil leaves it to the database default, read back after an insert.
	Currency *string `db:"currency,size:3,default:'USD'" json:"currency"`
	// Slug is computed by the database from the title; TSQ never writes it.
	Slug string `db:"slug,size:160,generated:LOWER(title)" json:"slug"`
}

// CourseLevel classifies how advanced a course is within the catalog.
type CourseLevel int

const (
	// CourseLevelFoundations marks entry-level courses.
	CourseLevelFoundations CourseLevel = iota
	// CourseLevelApplied marks practice-oriented intermediate courses.
	CourseLevelApplied
	// CourseLevelAdvanced marks advanced specialist courses.
	CourseLevelAdvanced
)
