package processor

type Decoder[T any] interface {
	Decode([]byte) (T, error)
}

type Processor[In, Out any] interface {
	Process(In) (Out, error)
}

type FuncProcessor[In, Out any] struct {
	f func(In) (Out, error)
}

func (r FuncProcessor[In, Out]) Process(in In) (Out, error) {
	return r.f(in)
}

func ProcessorFromFunc[In, Out any](f func(In) (Out, error)) FuncProcessor[In, Out] {
	return FuncProcessor[In, Out]{
		f: f,
	}
}

type Encoder[T any] interface {
	Encode(T) ([]byte, error)
}
