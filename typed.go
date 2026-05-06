package tsq

// JoinOn is a type-level join edge between two table owner types.
// It still renders as a normal Condition, but callers that accept JoinOn[L, R]
// can require the ON clause to connect exactly those two table owners.
type JoinOn[Left, Right any] struct {
	Cond
}

// JoinCond is an additional ON condition constrained to a join's left/right
// table owner types.
type JoinCond[Left, Right any] struct {
	Cond
}

// Pred is a type-level predicate owned by a table owner type.
type Pred[Owner any] struct {
	Cond
}

// PredConditions converts typed predicates to generic runtime conditions.
func PredConditions[Owner any](preds ...Pred[Owner]) []Condition {
	result := make([]Condition, 0, len(preds))
	for _, pred := range preds {
		result = append(result, pred)
	}

	return result
}

// OnExtra converts an additional typed join edge into a typed ON condition.
func OnExtra[Left, Right any](on JoinOn[Left, Right]) JoinCond[Left, Right] {
	return JoinCond[Left, Right](on)
}

// OnLeft converts a left-table predicate into a typed ON condition.
func OnLeft[Left, Right any](pred Pred[Left]) JoinCond[Left, Right] {
	return JoinCond[Left, Right](pred)
}

// OnRight converts a right-table predicate into a typed ON condition.
func OnRight[Left, Right any](pred Pred[Right]) JoinCond[Left, Right] {
	return JoinCond[Left, Right](pred)
}

// On creates an equality join edge between two typed columns.
func On[Left, Right, T any](left Col[Left, T], right Col[Right, T]) JoinOn[Left, Right] {
	return JoinOn[Left, Right]{Cond: left.EQCol(right).Cond}
}

// OnNE creates a non-equality join edge between two typed columns.
func OnNE[Left, Right, T any](left Col[Left, T], right Col[Right, T]) JoinOn[Left, Right] {
	return JoinOn[Left, Right]{Cond: left.NECol(right).Cond}
}

// OnGT creates a greater-than join edge between two typed columns.
func OnGT[Left, Right, T any](left Col[Left, T], right Col[Right, T]) JoinOn[Left, Right] {
	return JoinOn[Left, Right]{Cond: left.GTCol(right).Cond}
}

// OnGTE creates a greater-than-or-equal join edge between two typed columns.
func OnGTE[Left, Right, T any](left Col[Left, T], right Col[Right, T]) JoinOn[Left, Right] {
	return JoinOn[Left, Right]{Cond: left.GTECol(right).Cond}
}

// OnLT creates a less-than join edge between two typed columns.
func OnLT[Left, Right, T any](left Col[Left, T], right Col[Right, T]) JoinOn[Left, Right] {
	return JoinOn[Left, Right]{Cond: left.LTCol(right).Cond}
}

// OnLTE creates a less-than-or-equal join edge between two typed columns.
func OnLTE[Left, Right, T any](left Col[Left, T], right Col[Right, T]) JoinOn[Left, Right] {
	return JoinOn[Left, Right]{Cond: left.LTECol(right).Cond}
}
