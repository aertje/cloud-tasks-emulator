package maybe

type M[T any] struct {
	val T
	has bool
}

func (m M[T]) Get() (T, bool) {
	return m.val, m.has
}

func Just[T any](val T) M[T] {
	return M[T]{val, true}
}

func (m M[T]) Has() bool {
	return m.has
}
