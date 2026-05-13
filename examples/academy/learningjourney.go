package academy

import (
	"context"
	"time"

	"github.com/juju/errors"

	"github.com/tmoeish/tsq"
)

// LearningJourney is the end-to-end Result projection used in the full suite.
// @RESULT(name="LearningJourney")
type LearningJourney struct {
	LearnerID      int64  `json:"learner_id"      tsq:"Learner.ID"`
	LearnerName    string `json:"learner_name"    tsq:"Learner.Name"`
	LearnerCompany string `json:"learner_company" tsq:"Learner.Company"`

	TrackName      string      `json:"track_name"      tsq:"Track.Name"`
	CourseID       int64       `json:"course_id"       tsq:"Course.ID"`
	CourseTitle    string      `json:"course_title"    tsq:"Course.Title"`
	CourseLevel    CourseLevel `json:"course_level"    tsq:"Course.Level"`
	InstructorName string      `json:"instructor_name" tsq:"Instructor.Name"`

	EnrollmentID     int64            `json:"enrollment_id"     tsq:"Enrollment.UID"`
	EnrollmentStatus EnrollmentStatus `json:"enrollment_status" tsq:"Enrollment.Status"`
	EnrollmentScore  int64            `json:"enrollment_score"  tsq:"Enrollment.Score"`
	EnrollmentFee    int64            `json:"enrollment_fee"    tsq:"Enrollment.FeeCents"`
	EnrolledAt       time.Time        `json:"enrolled_at"       tsq:"Enrollment.CreatedAt"`
}

type engagedCourseRow struct {
	CourseID int64
}

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
		panic(errors.Annotate(err, "initialize engagedCourseIDs"))
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
		panic(errors.Annotate(err, "initialize pageLearningJourneyQuery"))
	}
}

func PageLearningJourney(
	ctx context.Context,
	tx tsq.SQLExecutor,
	page *tsq.PageReq,
	learnerIDs []int64,
	tracks ...string,
) (*tsq.PageResp[LearningJourney], error) {
	return tsq.Page(ctx, tx, page, pageLearningJourneyQuery, learnerIDs, tracks)
}
