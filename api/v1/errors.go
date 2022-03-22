package record_v1

import "fmt"

type ErrRecordNotFound struct {
	Key string
}

func (rnf ErrRecordNotFound) Error() string {
	return fmt.Sprintf("record with key %q not found", rnf.Key)
}
