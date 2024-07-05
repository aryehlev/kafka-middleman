package processor

import (
	"fmt"
	"time"

	"github.com/aryehlev/kafka-middleman/models"
	"github.com/aryehlev/kafka-middleman/utils"
)

const (
	ValidStatus   = "valid"
	InValidStatus = "invalid"
)

type validator struct {
	supportedTimeFormats []string
}

func (v *validator) Process(msg models.Message) (*models.Message, error) {
	timeStamp, err := utils.ParseTimestampMultiFormats(msg.Timestamp, v.supportedTimeFormats)
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
