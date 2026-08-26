package recovery

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrRecoveryDisabled  = errors.New("PAR2 recovery is disabled")
	ErrBudgetExceeded    = errors.New("PAR2 recovery download budget exceeded")
	ErrUnboundedTraffic  = errors.New("PAR2 recovery article has no bounded traffic estimate")
	ErrNoRecoverySet     = errors.New("no usable PAR2 recovery set")
	ErrLayoutUnavailable = errors.New("exact raw article layout is unavailable")
	ErrAmbiguousMapping  = errors.New("ambiguous PAR2 source-file mapping")
	ErrStorageBudget     = errors.New("PAR2 recovery storage budget exceeded")
)

// BudgetExceededError is returned before NNTP BODY traffic when the next
// atomic reservation would cross the per-repair cap. Requested may describe
// one article or an aggregate multi-article plan.
type BudgetExceededError struct {
	Limit     int64
	Used      int64
	Requested int64
}

func (e *BudgetExceededError) Error() string {
	return fmt.Sprintf("%v: limit %d bytes, already reserved %d bytes, next reservation %d bytes", ErrBudgetExceeded, e.Limit, e.Used, e.Requested)
}

func (e *BudgetExceededError) Is(target error) bool { return target == ErrBudgetExceeded }

// UnboundedTrafficError identifies an article for which the NZB supplied no
// positive posted-byte estimate. Such an article is never fetched as part of
// automatic repair.
type UnboundedTrafficError struct {
	RawFile   RawFileKey
	MessageID string
}

func (e *UnboundedTrafficError) Error() string {
	return fmt.Sprintf("%v: raw file %d article %q", ErrUnboundedTraffic, e.RawFile, e.MessageID)
}

func (e *UnboundedTrafficError) Is(target error) bool { return target == ErrUnboundedTraffic }

type NoRecoverySetError struct {
	NZBID   string
	RawFile RawFileKey
}

func (e *NoRecoverySetError) Error() string {
	return fmt.Sprintf("%v for raw file %d in NZB %q", ErrNoRecoverySet, e.RawFile, e.NZBID)
}

func (e *NoRecoverySetError) Is(target error) bool { return target == ErrNoRecoverySet }

type LayoutError struct {
	RawFile RawFileKey
	Offset  int64
	Length  int64
	Reason  string
}

func (e *LayoutError) Error() string {
	detail := e.Reason
	if detail == "" {
		detail = "no exact article covers the requested range"
	}
	return fmt.Sprintf("%v for raw file %d at [%d,%d): %s", ErrLayoutUnavailable, e.RawFile, e.Offset, e.Offset+e.Length, detail)
}

func (e *LayoutError) Is(target error) bool { return target == ErrLayoutUnavailable }

type AmbiguousMappingError struct {
	Filename   string
	FileID     FileID
	Candidates []RawFileKey
}

func (e *AmbiguousMappingError) Error() string {
	keys := make([]string, len(e.Candidates))
	for i, key := range e.Candidates {
		keys[i] = fmt.Sprint(key)
	}
	return fmt.Sprintf("%v for %q (%x): raw files [%s]", ErrAmbiguousMapping, e.Filename, e.FileID, strings.Join(keys, ", "))
}

func (e *AmbiguousMappingError) Is(target error) bool { return target == ErrAmbiguousMapping }

type StorageBudgetError struct {
	Limit     int64
	Used      int64
	Requested int64
}

func (e *StorageBudgetError) Error() string {
	return fmt.Sprintf("%v: limit %d bytes, store uses %d, write needs %d", ErrStorageBudget, e.Limit, e.Used, e.Requested)
}

func (e *StorageBudgetError) Is(target error) bool { return target == ErrStorageBudget }
