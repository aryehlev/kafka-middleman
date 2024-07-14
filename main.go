package main

import (
	"context"
	"github.com/aryehlev/kafka-middleman/processor"
	"os"
	"strings"
	"time"
)

const (
	groupIdEnv   = "GROUP_ID"
	addressesEnv = "ADRESSES"
	srcTpicEnv   = "TOPIC"
	destTopicEnv = "TOPIC"
	loopTimeEnv  = "LOOP_TIME"
)

func main() {
	pro := processor.Validator{
		SupportedTimeFormats: []string{time.DateOnly},
	}

	mid, _ := New(os.Getenv(groupIdEnv), os.Getenv(srcTpicEnv),
		os.Getenv(destTopicEnv), strings.Split(os.Getenv(addressesEnv), ","), pro.Process)

	mid.Run(context.Background())
}
