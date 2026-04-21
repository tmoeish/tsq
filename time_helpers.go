package tsq

import "time"

// TimePtr returns a pointer to the provided time value.
func TimePtr(t time.Time) *time.Time {
	return &t
}
