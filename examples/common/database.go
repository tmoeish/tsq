package common

import (
	"time"

	"gopkg.in/nullbio/null.v6"
)

// MutableTable is a table which its entities are mutable.
type MutableTable struct {
	UID       int64     `db:"uid"        json:"uid"`        // UID is the unique id.
	CreatedAt time.Time `db:"created_at" json:"created_at"` // CreatedAt is the create time.
	UpdatedAt null.Time `db:"updated_at" json:"updated_at"` // UpdatedAt is the update time.
	DeletedAt int64     `db:"deleted_at" json:"deleted_at"` // DeletedAt is the delete time.
	Version   int64     `db:"version"    json:"version"`    // Version is the row version.
}

// ImmutableTable is a table which its entities are immutable.
type ImmutableTable struct {
	ID        int64     `db:"id"         json:"id"`         // ID is the primary key.
	CreatedAt null.Time `db:"created_at" json:"created_at"` // CreatedAt is the create time.
}
