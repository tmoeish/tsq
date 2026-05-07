package tsq

// Owner marks a Go struct that can own selected or scanned fields.
type Owner interface {
	TSQOwner()
}

// Result marks a projection-only owner generated from a @RESULT spec.
type Result interface {
	Owner
	TSQResult()
}
