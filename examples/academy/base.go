package academy

import (
	"time"

	"gopkg.in/nullbio/null.v6"
)

// MutableTable provides shared lifecycle fields for mutable Academy tables.
type MutableTable struct {
	// UID 是带生命周期管理表的自增主键。
	UID int64 `db:"uid" json:"uid"`
	// CreatedAt 是记录创建时间。
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	// UpdatedAt 是最近一次更新时间，空值表示尚未更新。
	UpdatedAt null.Time `db:"updated_at" json:"updated_at"`
	// DeletedAt 是软删除标记，0 表示未删除。
	DeletedAt int64 `db:"deleted_at" json:"deleted_at"`
	// Version 是乐观锁版本号。
	Version int64 `db:"version" json:"version"`
}

// ImmutableTable provides shared identity fields for append-only Academy tables.
type ImmutableTable struct {
	// ID 是业务主表的自增主键。
	ID int64 `db:"id" json:"id"`
	// CreatedAt 是记录创建时间。
	CreatedAt null.Time `db:"created_at" json:"created_at"`
}
