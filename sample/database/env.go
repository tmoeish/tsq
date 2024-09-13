package database

import (
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

	EnvCode  string   `db:"env_code" json:"env_code"`
	EnvName  string   `db:"env_name" json:"env_name"`
	EnvLevel EnvLevel `db:"env_level" json:"env_level"`
}

// EnvLevel represents Env level.
type EnvLevel uint8

const (
	EnvLevelDev  EnvLevel = iota // Development level
	EnvLevelStg                  // Staging level
	EnvLevelGray                 // Gray level
	EnvLevelProd                 // Production level
)
