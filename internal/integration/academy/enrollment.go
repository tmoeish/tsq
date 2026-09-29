package academy

// Enrollment records the learner's progress in a course.
//
//tsq:table name=enrollment pk=UID
//tsq:managed version created_at updated_at deleted_at
//tsq:index LearnerID,CourseID
//tsq:index CourseID
//tsq:index Status
type Enrollment struct {
	MutableTable

	// LearnerID is the enrolled learner.
	LearnerID int64 `db:"learner_id" json:"learner_id"`
	// CourseID is the course enrolled into.
	CourseID int64 `db:"course_id" json:"course_id"`
	// Status is where the enrollment stands.
	Status EnrollmentStatus `db:"status" json:"status"`
	// Score is the learner's score.
	Score int64 `db:"score" json:"score"`
	// FeeCents is the amount paid, in cents.
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
