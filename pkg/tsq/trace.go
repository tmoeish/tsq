package tsq

import (
	"context"
	"encoding/json"
)

type Fn func(ctx context.Context) error

type Tracer func(next Fn) Fn

var tracers []Tracer

func Trace(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	for i := len(tracers) - 1; i >= 0; i-- {
		fn = tracers[i](fn)
	}
	return fn(ctx)
}

// PrettyJSON returns indented json string of obj.
func PrettyJSON(obj any) string {
	bs, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return ""
	}

	return string(bs)
}
