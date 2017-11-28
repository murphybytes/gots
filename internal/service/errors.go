package service

type NotFoundError struct {
	Key string
}

func (n *NotFoundError) Error() string {
	return "key '" + n.Key + "' does not exist"
}
