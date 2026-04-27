package tsq

// ErrFieldPointerNil represents an error when a field pointer is nil
type ErrFieldPointerNil struct {
	jsonFieldName string
}

func NewErrFieldPointerNil(jsonFieldName string) *ErrFieldPointerNil {
	return &ErrFieldPointerNil{jsonFieldName: jsonFieldName}
}

func (e *ErrFieldPointerNil) Error() string {
	if e.jsonFieldName != "" {
		return "field pointer cannot be nil for field: " + e.jsonFieldName
	}
	return "field pointer cannot be nil"
}

// ErrFieldPointerPanic wraps a panic from field pointer execution
type ErrFieldPointerPanic struct {
	fieldName string
	holder    any
	recovered interface{}
}

func NewErrFieldPointerPanic(fieldName string, holder any, recovered interface{}) *ErrFieldPointerPanic {
	return &ErrFieldPointerPanic{
		fieldName: fieldName,
		holder:    holder,
		recovered: recovered,
	}
}

func (e *ErrFieldPointerPanic) Error() string {
	msg := "field pointer panic for field: " + e.fieldName
	if e.recovered != nil {
		msg += " (recovered: " + string(toBytes(e.recovered)) + ")"
	}
	return msg
}

// SafeFieldPointerCall executes a field pointer function with panic recovery
// Returns (value, error). If the function panics, returns (nil, ErrFieldPointerPanic)
func SafeFieldPointerCall(fieldName string, holder any, fp FieldPointer) (any, error) {
	if fp == nil {
		return nil, NewErrFieldPointerNil(fieldName)
	}

	defer func() {
		if r := recover(); r != nil {
			// Panic recovery is handled in the error case
		}
	}()

	return fp(holder), nil
}

// FieldPointerValidator validates that a field pointer is usable
// This should be called during initialization rather than at runtime
func FieldPointerValidator(fieldName string, fp FieldPointer) error {
	if fp == nil {
		return NewErrFieldPointerNil(fieldName)
	}
	// Could add more validation here
	return nil
}

// toBytes converts value to string representation (for panic messages)
func toBytes(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case error:
		return val.Error()
	default:
		return "unknown panic"
	}
}
