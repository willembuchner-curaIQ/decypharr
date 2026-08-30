package arr

import (
	"errors"
	"fmt"
	"time"
)

// ErrMutationOutcomeUnknown marks a mutation whose result the Arr never
// confirmed. The caller must reconcile against the Arr before retrying, or it
// risks blocklisting or searching the same release twice.
var ErrMutationOutcomeUnknown = errors.New("Arr mutation outcome is unknown")

type mutationOutcomeUnknownError struct {
	cause      error
	retryAfter time.Duration
}

func (err *mutationOutcomeUnknownError) Error() string {
	return fmt.Sprintf("%s: %v", ErrMutationOutcomeUnknown, err.cause)
}

func (err *mutationOutcomeUnknownError) Unwrap() error {
	return errors.Join(ErrMutationOutcomeUnknown, err.cause)
}

// UnknownMutationOutcome wraps cause as an unconfirmed mutation. retryAfter is
// how long the caller should wait before the Arr is expected to show it.
func UnknownMutationOutcome(cause error, retryAfter time.Duration) error {
	if cause == nil {
		cause = errors.New("remote mutation was not visible during reconciliation")
	}
	return &mutationOutcomeUnknownError{cause: cause, retryAfter: retryAfter}
}

// MutationRetryAfter returns the wait an unconfirmed mutation asked for.
func MutationRetryAfter(err error) time.Duration {
	unknown, ok := errors.AsType[*mutationOutcomeUnknownError](err)
	if !ok {
		return 0
	}
	return unknown.retryAfter
}
