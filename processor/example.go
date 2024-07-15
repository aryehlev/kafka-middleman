package processor

import (
	"fmt"
	"time"

	"github.com/aryehlev/kafka-middleman/utils"
)

const (
	ValidStatus   = "valid"
	InValidStatus = "invalid"
)

type Message struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
	Status    string `json:"status"`
}

type Validator struct {
	SupportedTimeFormats []string
}

func (v *Validator) Process(msg Message) (*Message, error) {
	timeStamp, err := utils.ParseTimestampMultiFormats(msg.Timestamp, v.SupportedTimeFormats)
	if err != nil {
		return nil, err
	}
	if time.Since(timeStamp) > 24*time.Hour {
		return nil, fmt.Errorf("message older than 24 hours")
	}

	if len(msg.Data) > 10 {
		msg.Status = InValidStatus
	} else { // will allow empty messages.
		msg.Status = ValidStatus
	}

	return &msg, nil
}
