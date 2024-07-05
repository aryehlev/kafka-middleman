package models

type Message struct {
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
	Status    string `json:"status"`
}
