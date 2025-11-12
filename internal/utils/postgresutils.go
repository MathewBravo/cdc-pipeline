package utils

import "time"

func PGTimeMicros(t time.Time) int64 {
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return t.Sub(pgEpoch).Microseconds()
}
