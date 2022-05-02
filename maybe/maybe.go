package maybe

type Maybe[T any] interface {
	Unfold() T
	IsAny() bool
	IsEmpty() bool
}

type empty[T any] struct{}

func (e empty[T]) Unfold() T {
	panic("there is no value in empty")
}

func (e empty[T]) IsAny() bool {
	return false
}

func (e empty[T]) IsEmpty() bool {
	return true
}

func None[T any]() Maybe[T] {
	return empty[T]{}
}

func From[T any](v T) Maybe[T] {
	return some[T]{value: v}
}

type some[T any] struct {
	value T
}

func (a some[T]) Unfold() T {
	return a.value
}

func (a some[T]) IsAny() bool {
	return true
}

func (a some[T]) IsEmpty() bool {
	return false
}
