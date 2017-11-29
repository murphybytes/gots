package service

type ErrorNotFound struct {
	Key string
}

func (n *ErrorNotFound) Error() string {
	return "key '" + n.Key + "' does not exist"
}

type ErrorInvalidSearch struct {}
func(e *ErrorInvalidSearch) Error() string {
	return "search arguments are not valid"
}
