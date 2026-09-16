package academy

// Learner is the student profile shown across reports.
//
//tsq:table name=learner pk=ID
//tsq:managed created_at
//tsq:unique Email
//tsq:index Company
//tsq:search Name,Email,Company
type Learner struct {
	ImmutableTable

	// Name 是学员姓名。
	Name string `db:"name,size:120" json:"name"`
	// Email 是学员邮箱，要求唯一。
	Email string `db:"email,size:160" json:"email"`
	// Company 是学员所在公司。
	Company string `db:"company,size:160" json:"company"`
}
