package serde

import "encoding/json"

// T Must be pointer type, TODO: add validation on startup
type JsonParser[T any] struct {
}

func (jp JsonParser[T]) Decode(data []byte) (T, error) {
	var t T
	err := json.Unmarshal(data, t)
	return t, err
}

type JsonEncoder[T any] struct {
}

func (jp JsonEncoder[T]) Encode(data T) ([]byte, error) {
	return json.Marshal(data)
}
