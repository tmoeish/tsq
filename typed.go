package tsq

// Pred is a type-level predicate owned by a table owner type.
type Pred[O Owner] struct {
	Cond
}

// PredConditions converts typed predicates to generic runtime conditions.
func PredConditions[O Owner](preds ...Pred[O]) []Condition {
	result := make([]Condition, 0, len(preds))
	for _, pred := range preds {
		result = append(result, pred)
	}

	return result
}
