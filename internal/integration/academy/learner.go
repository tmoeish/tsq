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

	// Name is the learner's name.
	Name string `db:"name,size:120" json:"name"`
	// Email is the learner's unique email.
	Email string `db:"email,size:160" json:"email"`
	// Company is where the learner works.
	Company string `db:"company,size:160" json:"company"`
}
