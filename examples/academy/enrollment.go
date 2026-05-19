package academy

import "database/sql/driver"

// Enrollment records the learner's progress in a course.
// @TABLE(
//
//	name="enrollment",
//	pk="UID,true",
//	version,
//	created_at,
//	updated_at,
//	deleted_at,
//	idx=[
//		{fields=["LearnerID", "CourseID"]},
//		{fields=["CourseID"]},
//		{fields=["Status"]},
//	],
//
// )
type Enrollment struct {
	MutableTable

	// LearnerID 关联报名学员。
	LearnerID int64 `db:"learner_id" json:"learner_id"`
	// CourseID 关联被报名的课程。
	CourseID int64 `db:"course_id" json:"course_id"`
	// Status 表示报名状态。
	Status EnrollmentStatus `db:"status" json:"status"`
	// Score 是课程成绩。
	Score int64 `db:"score" json:"score"`
	// FeeCents 是实际支付金额，单位为分。
	FeeCents int64 `db:"fee_cents" json:"fee_cents"`
}

// EnrollmentStatus classifies a learner's lifecycle state within a course.
type EnrollmentStatus int

const (
	// EnrollmentStatusActive marks an in-progress enrollment.
	EnrollmentStatusActive EnrollmentStatus = iota
	// EnrollmentStatusCompleted marks a finished enrollment.
	EnrollmentStatusCompleted
	// EnrollmentStatusWaitlisted marks a learner waiting for a seat.
	EnrollmentStatusWaitlisted
	// EnrollmentStatusCancelled marks an enrollment that was cancelled.
	EnrollmentStatusCancelled
)

var _ driver.Valuer = EnrollmentStatus(0)

// Value stores the enum as an integer in the database.
func (s EnrollmentStatus) Value() (driver.Value, error) {
	return int64(s), nil
}
