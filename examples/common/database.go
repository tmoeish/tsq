package common

import (
	"time"

	"gopkg.in/nullbio/null.v6"
)

// MutableTable is a table which its entities are mutable.
type MutableTable struct {
	UID           int64     `db:"uid"           json:"uid"`           // UID is the unique id.
	CT           time.Time `db:"ct"            json:"ct"`            // CT is the create time.
	ModifiedTime null.Time `db:"modified_time" json:"modified_time"` // ModifiedTime is the modified time.
	DT           int64     `db:"dt"            json:"dt"`            // DT is the delete time.
	V            int64     `db:"v"             json:"v"`             // V is the version.
}

// ImmutableTable is a table which its entities are immutable.
type ImmutableTable struct {
	ID int64     `db:"id" json:"id"` // ID is the primary key.
	CT null.Time `db:"ct" json:"ct"` // CT is the create time.
}
