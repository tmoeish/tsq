package tsq

import (
	"fmt"
	"reflect"
)

func buildScanDest[O Owner](cols []BoundColumn[O], holder *O) ([]any, error) {
	if isNilValue(holder) {
		return nil, fmt.Errorf("scan holder cannot be nil")
	}

	erased := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		erased = append(erased, col)
	}

	return buildScanDestWith(erased, func(pointerFunc scanPointer) (any, error) {
		return invokeFieldPointer(pointerFunc, holder)
	})
}

func buildScanDestWith(cols []SQLColumn, invoke func(scanPointer) (any, error)) ([]any, error) {
	dest := make([]any, len(cols))

	for i, col := range cols {
		pointerFunc := col.scanPointer()
		if pointerFunc == nil {
			return nil, fmt.Errorf("select column %s cannot be scanned: field pointer is nil", col.SQLExpr())
		}

		ptr, err := invoke(pointerFunc)
		if err != nil {
			return nil, fmt.Errorf("select column %s cannot be scanned"+": %w", col.SQLExpr(), err)
		}

		if ptr == nil {
			return nil, fmt.Errorf("select column %s cannot be scanned: field pointer returned nil", col.SQLExpr())
		}

		value := reflect.ValueOf(ptr)
		if value.IsValid() && value.Kind() == reflect.Pointer && value.IsNil() {
			return nil, fmt.Errorf("select column %s cannot be scanned: field pointer returned nil", col.SQLExpr())
		}

		dest[i] = ptr
	}

	return dest, nil
}

func validateScanDestForType[O Owner](cols []BoundColumn[O], sqlText string, args []any) error {
	holder := new(O)
	if _, err := buildScanDest(cols, holder); err != nil {
		return fmt.Errorf("build scan dest\n%s\n%v"+": %w",
			sqlText, compactJSON(args), err)
	}

	return nil
}

func invokeFieldPointer[O Owner](pointerFunc scanPointer, holder *O) (ptr any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("field pointer panicked: %v", recovered)
		}
	}()

	return pointerFunc(holder), nil
}
