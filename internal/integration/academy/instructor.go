package academy

// Instructor stores the people teaching courses.
//
//tsq:table name=instructor pk=ID
//tsq:managed created_at
//tsq:unique Email
//tsq:search Name,Specialty,Bio
type Instructor struct {
	ImmutableTable

	// Name is the instructor's name.
	Name string `db:"name,size:120" json:"name"`
	// Email is the instructor's unique email.
	Email string `db:"email,size:160" json:"email"`
	// Specialty is what the instructor teaches best.
	Specialty string `db:"specialty,size:160" json:"specialty"`
	// Bio introduces the instructor.
	Bio string `db:"bio,size:2048" json:"bio"`
}
