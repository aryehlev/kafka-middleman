package utils

import (
	"fmt"
	"time"
)

func ParseTimestampMultiFormats(timestampStr string, timeFormats []string) (time.Time, error) {
	for _, format := range timeFormats {
		timestamp, err := time.Parse(format, timestampStr)
		if err == nil {
			return timestamp, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %s", timestampStr)
}
