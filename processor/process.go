package processor

type Decoder[T any] interface {
	Decode([]byte) (T, error)
}

type Processor[In, Out any] interface {
	Process(In) (Out, error)
}

type Encoder[T any] interface {
	Encode(T) ([]byte, error)
}
