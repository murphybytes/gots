package service

type KeyNotFound interface {
	NotFound()
}

type ErrorNotFound struct {
	Key string
}

func (n *ErrorNotFound) NotFound() {}

func (n *ErrorNotFound) Error() string {
	return "key '" + n.Key + "' does not exist"
}

type InvalidSearch interface {
	InvalidSearch()
}

type ErrorInvalidSearch struct{}

func (e *ErrorInvalidSearch) InvalidSearch() {}

func (e *ErrorInvalidSearch) Error() string {
	return "search arguments are not valid"
}
