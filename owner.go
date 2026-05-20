package tsq

// Owner marks any Go type that tsq can scan selected fields into.
// Table and Result are the two specialized owner kinds built on top of it.
type Owner interface {
	// TSQOwner marks a type as a valid tsq scan owner.
	TSQOwner()
}

// Result marks a projection-only owner.
// It participates in typed SELECT flows but is not a physical mutation target.
type Result interface {
	Owner
	// TSQResult marks a scan owner as projection-only.
	TSQResult()
}
