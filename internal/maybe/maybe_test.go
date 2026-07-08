package maybe

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSomePresent(t *testing.T) {
	m := Some(42)

	assert.True(t, m.IsPresent())

	v, ok := m.Get()
	assert.True(t, ok)
	assert.Equal(t, 42, v)
}

func TestNoneAbsent(t *testing.T) {
	m := None[int]()

	assert.False(t, m.IsPresent())

	v, ok := m.Get()
	assert.False(t, ok)
	assert.Equal(t, 0, v)
}

func TestZeroValueIsNone(t *testing.T) {
	var m Maybe[string]

	assert.False(t, m.IsPresent())
	assert.Equal(t, None[string](), m)
}

func TestSomeZeroValueStillPresent(t *testing.T) {
	m := Some("")

	assert.True(t, m.IsPresent())

	v, ok := m.Get()
	assert.True(t, ok)
	assert.Equal(t, "", v)
}

func TestOfPtr(t *testing.T) {
	v := 7
	assert.Equal(t, Some(7), OfPtr(&v))
	assert.Equal(t, None[int](), OfPtr[int](nil))
}

func TestPtrRoundTrip(t *testing.T) {
	assert.Nil(t, None[int]().Ptr())

	p := Some(9).Ptr()
	assert.NotNil(t, p)
	assert.Equal(t, 9, *p)
}

func TestPtrReturnsCopy(t *testing.T) {
	m := Some(3)
	p := m.Ptr()
	*p = 100

	// Mutating the returned pointer must not affect the Maybe.
	assert.Equal(t, 3, m.OrZero())
}

func TestOrElse(t *testing.T) {
	assert.Equal(t, 5, Some(5).OrElse(99))
	assert.Equal(t, 99, None[int]().OrElse(99))
}

func TestOrZero(t *testing.T) {
	assert.Equal(t, 5, Some(5).OrZero())
	assert.Equal(t, 0, None[int]().OrZero())
}

func TestMap(t *testing.T) {
	double := func(n int) int { return n * 2 }
	assert.Equal(t, Some(10), Map(Some(5), double))
	assert.Equal(t, None[int](), Map(None[int](), double))

	toLen := func(s string) int { return len(s) }
	assert.Equal(t, Some(3), Map(Some("abc"), toLen))
}

func TestComparable(t *testing.T) {
	one, alsoOne, two := Some(1), Some(1), Some(2)
	absent, alsoAbsent := None[int](), None[int]()

	assert.True(t, one == alsoOne)
	assert.False(t, one == two)
	assert.False(t, one == absent)
	assert.True(t, absent == alsoAbsent)
}
