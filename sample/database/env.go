package database

import (
	"database/sql/driver"

	"github.com/tmoeish/tsq/sample/common"
)

// Env represents Env.
//
//	@TABLE
//	@UX(UxAppIDEnvCode=AppID,EnvCode)
//	@IDX(IdxAppEnvLevel=AppID,EnvLevel)
//	@IDX(IdxEnvLevel=EnvLevel)
//	@KW(EnvName)
//	@V
//	@CT
//	@MT(ModifiedTime)
//	@DT
type Env struct {
	common.MutableTable
	common.AppBasic

	EnvCode  string   `db:"env_code"           json:"env_code"`
	EnvName  string   `db:"env_name,size:4096" json:"env_name"`
	EnvLevel EnvLevel `db:"env_level"          json:"env_level"`
}

var _ driver.Valuer = EnvLevel(0)

// EnvLevel represents Env level.
type EnvLevel uint8

// Value returns a driver Value.
// Value must not panic.
func (e EnvLevel) Value() (driver.Value, error) {
	return uint8(e), nil
}

const (
	EnvLevelDev  EnvLevel = iota // Development level
	EnvLevelStg                  // Staging level
	EnvLevelGray                 // Gray level
	EnvLevelProd                 // Production level
)
