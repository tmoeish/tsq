package academy

// Instructor stores the people teaching courses.
// @TABLE(
//
//	name="instructor",
//	pk="ID",
//	created_at,
//	ux=[
//		{fields=["Email"]},
//	],
//	search=["Name", "Specialty", "Bio"],
//
// )
type Instructor struct {
	ImmutableTable

	// Name 是讲师姓名。
	Name string `db:"name,size:120" json:"name"`
	// Email 是讲师邮箱，要求唯一。
	Email string `db:"email,size:160" json:"email"`
	// Specialty 是讲师擅长的教学方向。
	Specialty string `db:"specialty,size:160" json:"specialty"`
	// Bio 是讲师简介。
	Bio string `db:"bio,size:2048" json:"bio"`
}
