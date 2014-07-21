package transformer

type TransformError struct {
	What string
}

type NotTrackedError struct {
	What string
}

type SkippedColumnError struct {
	What string
}

type EmptyRequestError struct {
	What string
}

func (t TransformError) Error() string {
	return t.What
}

func (t NotTrackedError) Error() string {
	return t.What
}

func (t EmptyRequestError) Error() string {
	return t.What
}

func (t SkippedColumnError) Error() string {
	return t.What
}
