package tool

import (
	"encoding/json"
	"math"
)

func boundedInt(value any, fallback, max int) int {
	var number int64
	switch v := value.(type) {
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return fallback
		}
		number = n
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) || v < 1 || v > float64(max) {
			return fallback
		}
		number = int64(v)
	case int:
		number = int64(v)
	case int64:
		number = v
	default:
		return fallback
	}
	if number < 1 || number > int64(max) {
		return fallback
	}
	return int(number)
}
