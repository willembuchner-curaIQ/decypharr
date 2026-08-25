package recovery

import (
	"errors"
	"fmt"
)

var (
	// ErrStoreClosed identifies an operation attempted after Close.
	ErrStoreClosed = errors.New("recovery store is closed")
	// ErrNotFound identifies a requested recovery record that is not present.
	ErrNotFound = errors.New("recovery record not found")
	// ErrLegacyManifestUnsupported identifies an NZB that predates raw recovery
	// manifests. Re-importing the NZB is required before it can be repaired.
	ErrLegacyManifestUnsupported = errors.New("legacy NZB has no recovery manifest")
	// ErrUnsupported identifies a recovery record version this build cannot read.
	ErrUnsupported = errors.New("unsupported recovery record")
	// ErrCorrupt identifies a record whose framing, checksum, or contents failed
	// validation.
	ErrCorrupt = errors.New("corrupt recovery record")
	// ErrInvalid identifies invalid caller input.
	ErrInvalid = errors.New("invalid recovery data")
	// ErrChecksumMismatch identifies a payload that does not match its trusted
	// descriptor.
	ErrChecksumMismatch = errors.New("recovery checksum mismatch")
	// ErrRangeNotCovered identifies a repaired range with at least one missing
	// byte. Range reads never return partial data.
	ErrRangeNotCovered = errors.New("repaired range is not fully covered")
)

// ManifestUnavailableError is returned for legacy NZBs that have no raw-file
// manifest. It deliberately matches both ErrNotFound and
// ErrLegacyManifestUnsupported so callers can either present a generic cache
// miss or request a re-import.
type ManifestUnavailableError struct {
	NZBID string
}

func (e *ManifestUnavailableError) Error() string {
	return fmt.Sprintf("recovery manifest for NZB %q is unavailable: %v", e.NZBID, ErrLegacyManifestUnsupported)
}

func (e *ManifestUnavailableError) Is(target error) bool {
	return target == ErrNotFound || target == ErrLegacyManifestUnsupported
}

// NotFoundError identifies a missing non-manifest recovery record.
type NotFoundError struct {
	Kind  string
	NZBID string
	ID    string
}

func (e *NotFoundError) Error() string {
	if e.ID == "" {
		return fmt.Sprintf("%s for NZB %q: %v", e.Kind, e.NZBID, ErrNotFound)
	}
	return fmt.Sprintf("%s %q for NZB %q: %v", e.Kind, e.ID, e.NZBID, ErrNotFound)
}

func (e *NotFoundError) Is(target error) bool { return target == ErrNotFound }

// UnsupportedVersionError identifies a well-framed record with a newer or
// otherwise unsupported application format.
type UnsupportedVersionError struct {
	Kind    string
	Version uint64
}

func (e *UnsupportedVersionError) Error() string {
	return fmt.Sprintf("%s version %d: %v", e.Kind, e.Version, ErrUnsupported)
}

func (e *UnsupportedVersionError) Is(target error) bool { return target == ErrUnsupported }

// CorruptionError adds record context to ErrCorrupt.
type CorruptionError struct {
	Kind  string
	Key   string
	Cause error
}

func (e *CorruptionError) Error() string {
	if e.Key == "" {
		return fmt.Sprintf("corrupt %s: %v", e.Kind, e.Cause)
	}
	return fmt.Sprintf("corrupt %s record %q: %v", e.Kind, e.Key, e.Cause)
}

func (e *CorruptionError) Unwrap() error { return e.Cause }

func (e *CorruptionError) Is(target error) bool { return target == ErrCorrupt }

// ValidationError adds a field name to ErrInvalid.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

func (e *ValidationError) Is(target error) bool { return target == ErrInvalid }

// ChecksumMismatchError reports an untrusted payload without retaining it.
type ChecksumMismatchError struct {
	Kind     string
	Expected [16]byte
	Actual   [16]byte
}

func (e *ChecksumMismatchError) Error() string {
	return fmt.Sprintf("%s trusted MD5: expected %x, got %x: %v", e.Kind, e.Expected, e.Actual, ErrChecksumMismatch)
}

func (e *ChecksumMismatchError) Is(target error) bool { return target == ErrChecksumMismatch }

// RangeCoverageError reports the first uncovered byte in a requested range.
type RangeCoverageError struct {
	Start        uint64
	End          uint64
	FirstMissing uint64
}

func (e *RangeCoverageError) Error() string {
	return fmt.Sprintf("repaired range [%d,%d) is missing byte %d: %v", e.Start, e.End, e.FirstMissing, ErrRangeNotCovered)
}

func (e *RangeCoverageError) Is(target error) bool { return target == ErrRangeNotCovered }
