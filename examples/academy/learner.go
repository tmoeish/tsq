package academy

// Learner is the student profile shown across reports.
// @TABLE(
//
//	name="learner",
//	pk="ID",
//	created_at,
//	ux=[
//		{fields=["Email"]},
//	],
//	idx=[
//		{fields=["Company"]},
//	],
//	search=["Name", "Email", "Company"],
//
// )
type Learner struct {
	ImmutableTable

	// Name 是学员姓名。
	Name string `db:"name,size:120" json:"name"`
	// Email 是学员邮箱，要求唯一。
	Email string `db:"email,size:160" json:"email"`
	// Company 是学员所在公司。
	Company string `db:"company,size:160" json:"company"`
}
