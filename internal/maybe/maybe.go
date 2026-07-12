// Package maybe provides Maybe, a generic optional value that makes the
// presence or absence of a value explicit rather than overloading a nil
// pointer to mean "absent".
package maybe

// Maybe holds a value that may or may not be present. The zero value is an
// absent Maybe, equivalent to None. When T is comparable, Maybe[T] is
// comparable too.
type Maybe[T any] struct {
	value   T
	present bool
}

// Some returns a Maybe containing v.
func Some[T any](v T) Maybe[T] {
	return Maybe[T]{value: v, present: true}
}

// None returns an absent Maybe of type T.
func None[T any]() Maybe[T] {
	return Maybe[T]{}
}

// OfPtr returns Some(*p) when p is non-nil and None otherwise. It bridges
// pointer-based APIs (such as generated protobuf types) into a Maybe.
func OfPtr[T any](p *T) Maybe[T] {
	if p == nil {
		return None[T]()
	}
	return Some(*p)
}

// OfNonZero returns Some(v) unless v is the zero value of T, in which case it
// returns None. It bridges APIs that overload a zero value to mean "absent"
// (such as proto3 scalar fields) into a Maybe.
func OfNonZero[T comparable](v T) Maybe[T] {
	var zero T
	if v == zero {
		return None[T]()
	}
	return Some(v)
}

// IsPresent reports whether a value is present.
func (m Maybe[T]) IsPresent() bool {
	return m.present
}

// Get returns the contained value and whether it was present. When absent the
// returned value is the zero value of T.
func (m Maybe[T]) Get() (T, bool) {
	return m.value, m.present
}

// OrElse returns the contained value if present, otherwise def.
func (m Maybe[T]) OrElse(def T) T {
	if m.present {
		return m.value
	}
	return def
}

// Or returns m if it is present, otherwise Some(def). Unlike OrElse it keeps the
// result wrapped, so it reads as "fill in a default when absent" (e.g. applying
// a server-assigned default to an optional field).
func (m Maybe[T]) Or(def T) Maybe[T] {
	if m.present {
		return m
	}
	return Some(def)
}

// OrZero returns the contained value if present, otherwise the zero value of T.
func (m Maybe[T]) OrZero() T {
	return m.value
}

// Ptr returns a pointer to a copy of the contained value, or nil when absent.
// It bridges a Maybe back into pointer-based APIs.
func (m Maybe[T]) Ptr() *T {
	if !m.present {
		return nil
	}
	v := m.value
	return &v
}

// Map applies f to the value inside m when present and returns a Maybe of the
// result; when m is absent it returns None.
func Map[T, U any](m Maybe[T], f func(T) U) Maybe[U] {
	v, ok := m.Get()
	if !ok {
		return None[U]()
	}
	return Some(f(v))
}
