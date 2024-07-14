package processor

type Decoder[T any] interface {
	Decode([]byte) (T, error)
}

type Encoder[T any] interface {
	Encode(T) ([]byte, error)
}
