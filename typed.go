package tsq

// Predicate carries a runtime condition together with its owner type.
type Predicate[O Owner] struct {
	Cond
}

// PredConditions converts typed predicates to generic runtime conditions.
func PredConditions[O Owner](preds ...Predicate[O]) []Condition {
	result := make([]Condition, 0, len(preds))
	for _, pred := range preds {
		result = append(result, pred)
	}

	return result
}
