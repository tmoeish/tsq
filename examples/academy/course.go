package academy

import "database/sql/driver"

// Course is the main catalog entity learners enroll into.
// @TABLE(
//
//	name="course",
//	pk="ID",
//	created_at,
//	ux=[
//		{fields=["Title"]},
//	],
//	idx=[
//		{fields=["TrackID"]},
//		{fields=["InstructorID"]},
//		{fields=["PrerequisiteID"]},
//	],
//	kw=["Title", "Summary"],
//
// )
type Course struct {
	ImmutableTable

	// TrackID 关联所属学习路径。
	TrackID int64 `db:"track_id" json:"track_id"`
	// InstructorID 关联授课讲师。
	InstructorID int64 `db:"instructor_id" json:"instructor_id"`
	// PrerequisiteID 关联前置课程，0 表示没有前置课。
	PrerequisiteID int64 `db:"prerequisite_id" json:"prerequisite_id"`

	// Title 是课程标题。
	Title string `db:"title,size:160" json:"title"`
	// Summary 是课程简介。
	Summary string `db:"summary,size:4096" json:"summary"`
	// Level 表示课程难度等级。
	Level CourseLevel `db:"level" json:"level"`
	// ListPriceCents 是课程标价，单位为分。
	ListPriceCents int64 `db:"list_price_cents" json:"list_price_cents"`
	// Published 表示课程是否已发布到目录。
	Published bool `db:"published" json:"published"`
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

var _ driver.Valuer = CourseLevel(0)

// Value stores the enum as an integer in the database.
func (l CourseLevel) Value() (driver.Value, error) {
	return int64(l), nil
}
