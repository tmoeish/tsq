package academy

import (
	"context"
	"fmt"
	"time"

	"github.com/tmoeish/tsq/v5"
)

// LearningJourney is the end-to-end Result projection used in the full suite.
//
//tsq:result
type LearningJourney struct {
	// LearnerID identifies the learner row joined into the projection.
	LearnerID int64 `json:"learner_id" tsq:"Learner.ID"`
	// LearnerName carries the learner display name.
	LearnerName string `json:"learner_name" tsq:"Learner.Name"`
	// LearnerCompany carries the learner's company affiliation.
	LearnerCompany string `json:"learner_company" tsq:"Learner.Company"`

	// TrackName carries the course track name.
	TrackName string `json:"track_name" tsq:"Track.Name"`
	// CourseID identifies the enrolled course.
	CourseID int64 `json:"course_id" tsq:"Course.ID"`
	// CourseTitle carries the course title.
	CourseTitle string `json:"course_title" tsq:"Course.Title"`
	// CourseLevel carries the course difficulty level.
	CourseLevel CourseLevel `json:"course_level" tsq:"Course.Level"`
	// InstructorName carries the assigned instructor name.
	InstructorName string `json:"instructor_name" tsq:"Instructor.Name"`

	// EnrollmentID identifies the enrollment row.
	EnrollmentID int64 `json:"enrollment_id" tsq:"Enrollment.UID"`
	// EnrollmentStatus carries the learner's enrollment status.
	EnrollmentStatus EnrollmentStatus `json:"enrollment_status" tsq:"Enrollment.Status"`
	// EnrollmentScore carries the learner's score.
	EnrollmentScore int64 `json:"enrollment_score" tsq:"Enrollment.Score"`
	// EnrollmentFee carries the enrollment fee in cents.
	EnrollmentFee int64 `json:"enrollment_fee" tsq:"Enrollment.FeeCents"`
	// EnrolledAt carries the enrollment creation time.
	EnrolledAt time.Time `json:"enrolled_at" tsq:"Enrollment.CreatedAt"`
}

var pageLearningJourneyQuery *tsq.Query[LearningJourney]

func init() {
	// A stage from SelectValue is a subquery of its values; its errors surface in
	// the Build below.
	engagedCourseIDs := tsq.
		SelectValue(TableEnrollment.CourseID).
		From(TableEnrollment).
		Where(TableEnrollment.Status.NE(tsq.Val(EnrollmentStatusCancelled))).
		GroupBy(TableEnrollment.CourseID).
		Having(tsq.Count(TableEnrollment.UID).GTE(tsq.Val(int64(2))))

	var err error

	pageLearningJourneyQuery, err = tsq.
		Select(ResultLearningJourney.Columns()...).
		From(TableEnrollment).
		// Every enrollment has a learner and a course, and every course a track and
		// an instructor, so these are inner joins; a LEFT JOIN would make the
		// projected fields nullable.
		Join(TableLearner, TableEnrollment.LearnerID.EQ(TableLearner.ID)).
		Join(TableCourse, TableEnrollment.CourseID.EQ(TableCourse.ID)).
		Join(TableTrack, TableCourse.TrackID.EQ(TableTrack.ID)).
		Join(TableInstructor, TableCourse.InstructorID.EQ(TableInstructor.ID)).
		Where(
			TableLearner.ID.In(TableLearner.ID.ListParam()),
			TableTrack.Name.In(TableTrack.Name.ListParam()),
			TableEnrollment.CourseID.In(engagedCourseIDs),
		).
		Build()
	if err != nil {
		panic(fmt.Errorf("%s: %w", "initialize pageLearningJourneyQuery", err))
	}
}

// PageLearningJourney pages the full-suite LearningJourney projection for selected learners and tracks.
func PageLearningJourney(
	ctx context.Context,
	tx tsq.Executor,
	page tsq.Paging,
	learnerIDs []int64,
	tracks ...string,
) (*tsq.Page[LearningJourney], error) {
	return pageLearningJourneyQuery.Page(ctx, tx, page,
		TableLearner.ID.BindList(learnerIDs...),
		TableTrack.Name.BindList(tracks...),
	)
}
