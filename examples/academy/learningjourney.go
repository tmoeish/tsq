package academy

import (
	"context"
	"fmt"
	"time"

	"github.com/tmoeish/tsq/v4"
)

// LearningJourney is the end-to-end Result projection used in the full suite.
// @RESULT(name="LearningJourney")
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

type engagedCourseRow struct {
	CourseID int64
}

// TSQOwner marks engagedCourseRow as an internal projection owner.
func (engagedCourseRow) TSQOwner() {}

var pageLearningJourneyQuery *tsq.Query[LearningJourney]

func init() {
	var err error

	courseID := tsq.MapInto(Enrollment_CourseID, func(holder *engagedCourseRow) *int64 {
		return &holder.CourseID
	}, "course_id")

	engagedCourseIDs, err := tsq.
		Select(courseID).
		From(TableEnrollment).
		Where(Enrollment_Status.NE(EnrollmentStatusCancelled)).
		GroupBy(Enrollment_CourseID).
		Having(Enrollment_UID.Count().GTE(2)).
		Build()
	if err != nil {
		panic(fmt.Errorf("%s: %w", "initialize engagedCourseIDs", err))
	}

	pageLearningJourneyQuery, err = tsq.
		Select(ResultLearningJourney.Cols()...).
		From(TableEnrollment).
		LeftJoin(TableLearner, Enrollment_LearnerID.EQCol(Learner_ID)).
		LeftJoin(TableCourse, Enrollment_CourseID.EQCol(Course_ID)).
		LeftJoin(TableTrack, Course_TrackID.EQCol(Track_ID)).
		LeftJoin(TableInstructor, Course_InstructorID.EQCol(Instructor_ID)).
		Where(
			Learner_ID.InVar(),
			Track_Name.InVar(),
			Enrollment_CourseID.InSub(engagedCourseIDs),
		).
		Build()
	if err != nil {
		panic(fmt.Errorf("%s: %w", "initialize pageLearningJourneyQuery", err))
	}
}

// PageLearningJourney pages the full-suite LearningJourney projection for selected learners and tracks.
func PageLearningJourney(
	ctx context.Context,
	tx tsq.SQLExecutor,
	page *tsq.PageRequest,
	learnerIDs []int64,
	tracks ...string,
) (*tsq.PageResponse[LearningJourney], error) {
	return tsq.Page(ctx, tx, page, pageLearningJourneyQuery, learnerIDs, tracks)
}
