package academy

import "encoding/json"

// Track groups courses into a learning path.
//
//tsq:table name=track pk=ID
//tsq:managed created_at
//tsq:unique Name
//tsq:search Name,Description
type Track struct {
	ImmutableTable

	// Name is the track name.
	Name string `db:"name,size:120" json:"name"`
	// Description introduces the track.
	Description string `db:"description,size:1024" json:"description"`
	// SkillItems stores structured JSON as is, through an explicit DDL type override.
	SkillItems json.RawMessage `db:"skill_items,type:JSON" json:"skill_items"`
}
