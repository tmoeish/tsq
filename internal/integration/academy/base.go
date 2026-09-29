package academy

import (
	"database/sql"
	"time"
)

// MutableTable provides shared lifecycle fields for mutable Academy tables.
type MutableTable struct {
	// UID is the auto-increment key of tables with a managed lifecycle.
	UID int64 `db:"uid" json:"uid"`
	// CreatedAt is when the row was created.
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	// UpdatedAt is the last update; NULL until the first one.
	UpdatedAt sql.Null[time.Time] `db:"updated_at" json:"updated_at"`
	// DeletedAt is the soft-delete tombstone; 0 means live.
	DeletedAt int64 `db:"deleted_at" json:"deleted_at"`
	// Version is the optimistic-lock version.
	Version int64 `db:"version" json:"version"`
}

// ImmutableTable provides shared identity fields for append-only Academy tables.
type ImmutableTable struct {
	// ID is the auto-increment key.
	ID int64 `db:"id" json:"id"`
	// CreatedAt is when the row was created.
	CreatedAt sql.Null[time.Time] `db:"created_at" json:"created_at"`
}
