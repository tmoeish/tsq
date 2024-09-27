package tsq

import (
	"context"
	"encoding/json"
)

func TraceDB(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	return fn(ctx)
}

// PrettyJSON returns indented json string of obj.
func PrettyJSON(obj interface{}) string {
	bs, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return ""
	}

	return string(bs)
}
