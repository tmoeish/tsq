package common

import (
	"time"
)

// AppBasic is a struct that contains basic app information.
type AppBasic struct {
	AppID   int64  `db:"app_id" json:"app_id"`
	AppCode string `db:"app_code" json:"app_code"`
	AppName string `db:"app_name" json:"app_name"`
}

// MutableTable is a table which its entities are mutable.
type MutableTable struct {
	ID int64     `db:"id" json:"id"` // ID is the primary key.
	CT time.Time `db:"ct" json:"ct"` // CT is the create time.
	MT time.Time `db:"mt" json:"mt"` // MT is the modify time.
	DT int64     `db:"dt" json:"dt"` // DT is the delete time.
	V  int64     `db:"v" json:"v"`   // V is the version.
}

// ImmutableTable is a table which its entities are immutable.
type ImmutableTable struct {
	ID int64     `db:"id" json:"id"` // ID is the primary key.
	CT time.Time `db:"ct" json:"ct"` // CT is the create time.
}
