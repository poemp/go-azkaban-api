package utils

import "strconv"

// Itoa is equivalent to FormatInt(int64(i), 10).
func Itoa32(i int32) string {
	return strconv.FormatInt(int64(i), 10)
}

// Itoa is equivalent to FormatInt(int64(i), 10).
func Itoa64(i int64) string {
	return strconv.FormatInt(i, 10)
}