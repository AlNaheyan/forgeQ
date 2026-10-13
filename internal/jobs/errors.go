package jobs

import "errors"

var (
	ErrNotFound          = errors.New("job not found")
	ErrInvalidTransition = errors.New("invalid job state transition")
	ErrInvalidJob        = errors.New("invalid job")
)
