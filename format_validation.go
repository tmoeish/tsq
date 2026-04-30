package tsq

import (
	"github.com/juju/errors"
)

func countStringFormatPlaceholders(format string) (int, error) {
	count := 0

	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}

		if i+1 >= len(format) {
			return 0, errors.Errorf("unterminated format verb")
		}

		switch format[i+1] {
		case '%':
			i++
		case 's':
			count++
			i++
		default:
			return 0, errors.Errorf("unsupported format verb %%%c", format[i+1])
		}
	}

	return count, nil
}
