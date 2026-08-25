package par2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/sirrobot01/decypharr/internal/par2gf16"
)

var (
	ErrInvalidPlan       = errors.New("par2: invalid recovery plan")
	ErrNotEnoughRecovery = errors.New("par2: not enough recovery slices")
	ErrSingularSelection = errors.New("par2: singular recovery row selection")
	ErrInvalidRange      = errors.New("par2: invalid recovery range")
)

const (
	DefaultSelectionAttempts = 4096
	DefaultMaxMatrixBytes    = int64(512 << 20)
	DefaultStripeSize        = int64(4 << 20)
	maxPAR2DataShards        = 32768
)

// ByteRange is a half-open byte interval within every source or recovery
// slice. Length zero is accepted and ignored by Recover.
type ByteRange struct {
	Offset int64
	Length int64
}

func (r ByteRange) end() (int64, bool) {
	if r.Offset < 0 || r.Length < 0 || r.Offset > math.MaxInt64-r.Length {
		return 0, false
	}
	return r.Offset + r.Length, true
}

// RangeReader fills dst with bytes starting at offset within one PAR2 slice.
// It must fill all of dst or return an error. Calls are sequential, but the
// callback must not retain dst. Odd requested ranges are expanded by at most
// one byte at either edge because PAR2 arithmetic operates on 16-bit symbols.
type RangeReader func(ctx context.Context, offset int64, dst []byte) error

// RangeSink receives reconstructed bytes for one missing source shard. Calls
// are sequential and never include bytes outside the requested ranges. data is
// reused after the callback returns and must be copied if it is retained.
type RangeSink func(ctx context.Context, shard int, offset int64, data []byte) error

// DataSource supplies one surviving source shard.
type DataSource struct {
	Shard int
	Read  RangeReader
}

// RecoverySource supplies the recovery slice for an arbitrary PAR2 exponent.
type RecoverySource struct {
	Exponent uint16
	Read     RangeReader
}

// PlanRequest describes source availability. Data must contain every shard
// not listed in Missing exactly once. Extra Recovery entries let the planner
// retry a different row combination when the PAR2 Vandermonde submatrix is
// singular.
type PlanRequest struct {
	DataShards int
	SliceSize  int64
	Missing    []int
	Data       []DataSource
	Recovery   []RecoverySource

	// MaxSelectionAttempts bounds combination search. Zero selects
	// DefaultSelectionAttempts.
	MaxSelectionAttempts int
	// MaxMatrixBytes bounds the reconstruction coefficient matrix. Zero
	// selects DefaultMaxMatrixBytes.
	MaxMatrixBytes int64
}

// RecoverOptions controls interval reconstruction.
type RecoverOptions struct {
	// StripeSize bounds one range pass. Zero selects DefaultStripeSize. It is
	// rounded down to an even byte count for GF(2^16) symbols.
	StripeSize int64
	// NumGoroutines controls FoldInputs. Zero selects a CPU-aware default.
	NumGoroutines int
}

// NotEnoughRecoveryError reports that fewer independent exponent rows were
// supplied than there are missing source shards.
type NotEnoughRecoveryError struct {
	Missing   int
	Available int
}

func (e *NotEnoughRecoveryError) Error() string {
	return fmt.Sprintf("%v: need %d, have %d", ErrNotEnoughRecovery, e.Missing, e.Available)
}

func (e *NotEnoughRecoveryError) Unwrap() error { return ErrNotEnoughRecovery }

// SingularSelectionError reports that the planner could not find an
// invertible selection. Exhausted is true when the configured attempt limit,
// rather than all combinations, stopped the search.
type SingularSelectionError struct {
	Missing    int
	Candidates int
	Attempts   int
	Exponents  []uint16
	Exhausted  bool
}

func (e *SingularSelectionError) Error() string {
	reason := "all candidate combinations were singular"
	if e.Exhausted {
		reason = "selection attempt limit reached"
	}
	return fmt.Sprintf("%v after %d attempts (%s; exponents %v)",
		ErrSingularSelection, e.Attempts, reason, e.Exponents)
}

func (e *SingularSelectionError) Unwrap() error { return ErrSingularSelection }

// Plan is an immutable recovery plan. It can recover multiple disjoint range
// sets; callbacks themselves determine whether concurrent calls are safe.
type Plan struct {
	sliceSize         int64
	missing           []int
	available         []DataSource
	recovery          []RecoverySource
	matrix            par2gf16.Matrix
	selectionAttempts int
}

// NewPlan validates source coverage, retries recovery-row combinations when
// needed, and constructs the matrix consumed by the ranged fold engine.
func NewPlan(request PlanRequest) (*Plan, error) {
	if request.DataShards <= 0 || request.DataShards > maxPAR2DataShards {
		return nil, fmt.Errorf("%w: data shard count %d is outside 1..%d",
			ErrInvalidPlan, request.DataShards, maxPAR2DataShards)
	}
	if request.SliceSize <= 0 || request.SliceSize%2 != 0 {
		return nil, fmt.Errorf("%w: slice size %d must be a positive even number", ErrInvalidPlan, request.SliceSize)
	}

	missing, missingSet, err := validateMissing(request.DataShards, request.Missing)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		return nil, fmt.Errorf("%w: no missing shards", ErrInvalidPlan)
	}
	available, err := validateDataSources(request.DataShards, missingSet, request.Data)
	if err != nil {
		return nil, err
	}
	recovery, err := uniqueRecoverySources(request.Recovery)
	if err != nil {
		return nil, err
	}
	if len(recovery) < len(missing) {
		return nil, &NotEnoughRecoveryError{Missing: len(missing), Available: len(recovery)}
	}

	maxMatrixBytes := request.MaxMatrixBytes
	if maxMatrixBytes == 0 {
		maxMatrixBytes = DefaultMaxMatrixBytes
	}
	if maxMatrixBytes < 0 {
		return nil, fmt.Errorf("%w: negative matrix limit", ErrInvalidPlan)
	}
	if err := validateMatrixBudget(len(missing), request.DataShards, maxMatrixBytes); err != nil {
		return nil, err
	}

	generators := par2Generators(request.DataShards)
	maxAttempts := request.MaxSelectionAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultSelectionAttempts
	}
	if maxAttempts < 0 {
		return nil, fmt.Errorf("%w: negative selection attempt limit", ErrInvalidPlan)
	}

	selected, inverse, attempts, err := selectRecoveryRows(recovery, missing, generators, maxAttempts)
	if err != nil {
		return nil, err
	}
	matrix := buildReconstructionMatrix(inverse, available, selected, generators)
	return &Plan{
		sliceSize: request.SliceSize, missing: missing, available: available,
		recovery: selected, matrix: matrix, selectionAttempts: attempts,
	}, nil
}

func validateMissing(dataShards int, requested []int) ([]int, map[int]struct{}, error) {
	missing := append([]int(nil), requested...)
	sort.Ints(missing)
	set := make(map[int]struct{}, len(missing))
	for _, shard := range missing {
		if shard < 0 || shard >= dataShards {
			return nil, nil, fmt.Errorf("%w: missing shard %d is out of range", ErrInvalidPlan, shard)
		}
		if _, duplicate := set[shard]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate missing shard %d", ErrInvalidPlan, shard)
		}
		set[shard] = struct{}{}
	}
	return missing, set, nil
}

func validateDataSources(dataShards int, missing map[int]struct{}, sources []DataSource) ([]DataSource, error) {
	byShard := make(map[int]DataSource, len(sources))
	for _, source := range sources {
		if source.Shard < 0 || source.Shard >= dataShards {
			return nil, fmt.Errorf("%w: data shard %d is out of range", ErrInvalidPlan, source.Shard)
		}
		if _, absent := missing[source.Shard]; absent {
			return nil, fmt.Errorf("%w: shard %d is both missing and available", ErrInvalidPlan, source.Shard)
		}
		if source.Read == nil {
			return nil, fmt.Errorf("%w: shard %d has a nil reader", ErrInvalidPlan, source.Shard)
		}
		if _, duplicate := byShard[source.Shard]; duplicate {
			return nil, fmt.Errorf("%w: duplicate data shard %d", ErrInvalidPlan, source.Shard)
		}
		byShard[source.Shard] = source
	}

	available := make([]DataSource, 0, dataShards-len(missing))
	for shard := range dataShards {
		if _, absent := missing[shard]; absent {
			continue
		}
		source, ok := byShard[shard]
		if !ok {
			return nil, fmt.Errorf("%w: no reader for available shard %d", ErrInvalidPlan, shard)
		}
		available = append(available, source)
	}
	if len(byShard) != len(available) {
		return nil, fmt.Errorf("%w: unexpected data source", ErrInvalidPlan)
	}
	return available, nil
}

func uniqueRecoverySources(sources []RecoverySource) ([]RecoverySource, error) {
	seen := make(map[uint16]struct{}, len(sources))
	unique := make([]RecoverySource, 0, len(sources))
	for _, source := range sources {
		if source.Read == nil {
			return nil, fmt.Errorf("%w: exponent %d has a nil reader", ErrInvalidPlan, source.Exponent)
		}
		if _, duplicate := seen[source.Exponent]; duplicate {
			continue // another copy of the same recovery row adds no rank
		}
		seen[source.Exponent] = struct{}{}
		unique = append(unique, source)
	}
	return unique, nil
}

func validateMatrixBudget(missing, dataShards int, maxBytes int64) error {
	// Reserve two final-sized matrices and three square inversion workspaces.
	// Each GF element is two bytes.
	elements := 2*uint64(missing)*uint64(dataShards) + 3*uint64(missing)*uint64(missing)
	if elements > math.MaxUint64/2 || elements*2 > uint64(maxBytes) {
		return fmt.Errorf("%w: coefficient matrices need approximately %d bytes (limit %d)",
			ErrInvalidPlan, saturatingDouble(elements), maxBytes)
	}
	if elements > uint64(math.MaxInt) {
		return fmt.Errorf("%w: coefficient matrix exceeds addressable memory", ErrInvalidPlan)
	}
	return nil
}

func saturatingDouble(v uint64) uint64 {
	if v > math.MaxUint64/2 {
		return math.MaxUint64
	}
	return v * 2
}

func par2Generators(count int) []par2gf16.Element {
	generators := make([]par2gf16.Element, 0, count)
	for power := 0; len(generators) < count && power < 1<<16; power++ {
		if power%3 == 0 || power%5 == 0 || power%17 == 0 || power%257 == 0 {
			continue
		}
		generators = append(generators, par2gf16.Element(2).Pow(uint32(power)))
	}
	return generators
}

func recoveryCoefficient(generator par2gf16.Element, exponent uint16) par2gf16.Element {
	return generator.Pow(uint32(exponent))
}

func selectRecoveryRows(
	candidates []RecoverySource,
	missing []int,
	generators []par2gf16.Element,
	maxAttempts int,
) ([]RecoverySource, par2gf16.Matrix, int, error) {
	needed := len(missing)
	choice := make([]int, needed)
	for i := range choice {
		choice[i] = i
	}
	attempts := 0
	for {
		attempts++
		selected := make([]RecoverySource, needed)
		for i, candidate := range choice {
			selected[i] = candidates[candidate]
		}
		matrix := par2gf16.NewMatrix(needed, needed, func(i, j int) par2gf16.Element {
			return recoveryCoefficient(generators[missing[j]], selected[i].Exponent)
		})
		if inverse, err := matrix.Inverse(); err == nil {
			return selected, inverse, attempts, nil
		}

		more := nextCombination(choice, len(candidates))
		if !more || attempts >= maxAttempts {
			exponents := make([]uint16, len(candidates))
			for i := range candidates {
				exponents[i] = candidates[i].Exponent
			}
			return nil, par2gf16.Matrix{}, attempts, &SingularSelectionError{
				Missing: needed, Candidates: len(candidates), Attempts: attempts,
				Exponents: exponents, Exhausted: more,
			}
		}
	}
}

func nextCombination(choice []int, candidates int) bool {
	for i := len(choice) - 1; i >= 0; i-- {
		limit := candidates - len(choice) + i
		if choice[i] >= limit {
			continue
		}
		choice[i]++
		for j := i + 1; j < len(choice); j++ {
			choice[j] = choice[j-1] + 1
		}
		return true
	}
	return false
}

func buildReconstructionMatrix(
	inverse par2gf16.Matrix,
	available []DataSource,
	selected []RecoverySource,
	generators []par2gf16.Element,
) par2gf16.Matrix {
	outputs := len(selected)
	inputs := len(available) + len(selected)
	return par2gf16.NewMatrix(outputs, inputs, func(out, input int) par2gf16.Element {
		if input >= len(available) {
			return inverse.At(out, input-len(available))
		}
		generator := generators[available[input].Shard]
		var coefficient par2gf16.Element
		for equation, recovery := range selected {
			coefficient ^= inverse.At(out, equation).Mul(recoveryCoefficient(generator, recovery.Exponent))
		}
		return coefficient
	})
}

// MissingShards returns the source rows reconstructed by this plan.
func (p *Plan) MissingShards() []int {
	if p == nil {
		return nil
	}
	return append([]int(nil), p.missing...)
}

// SliceSize returns the byte size of each PAR2 source/recovery slice.
func (p *Plan) SliceSize() int64 {
	if p == nil {
		return 0
	}
	return p.sliceSize
}

// RecoveryExponents returns the recovery rows selected by the planner.
func (p *Plan) RecoveryExponents() []uint16 {
	if p == nil {
		return nil
	}
	exponents := make([]uint16, len(p.recovery))
	for i := range p.recovery {
		exponents[i] = p.recovery[i].Exponent
	}
	return exponents
}

// SelectionAttempts reports how many exponent combinations were tested.
func (p *Plan) SelectionAttempts() int {
	if p == nil {
		return 0
	}
	return p.selectionAttempts
}

// Recover reconstructs only ranges and streams them to sink. Internally each
// interval is processed in bounded stripes through the reusable fold engine.
func (p *Plan) Recover(ctx context.Context, ranges []ByteRange, sink RangeSink, options RecoverOptions) error {
	if p == nil {
		return fmt.Errorf("%w: nil plan", ErrInvalidPlan)
	}
	if sink == nil {
		return fmt.Errorf("%w: nil recovery sink", ErrInvalidPlan)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	normalized, err := normalizeRanges(ranges, p.sliceSize)
	if err != nil {
		return err
	}
	if len(normalized) == 0 {
		return nil
	}

	stripeSize := options.StripeSize
	if stripeSize == 0 {
		stripeSize = DefaultStripeSize
	}
	if stripeSize < 2 {
		return fmt.Errorf("%w: stripe size %d is too small", ErrInvalidRange, stripeSize)
	}
	stripeSize -= stripeSize % 2
	if stripeSize > int64(math.MaxInt) {
		stripeSize = int64(math.MaxInt)
		stripeSize -= stripeSize % 2
	}
	workers := options.NumGoroutines
	if workers == 0 {
		workers = par2gf16.DefaultWorkers()
	}
	if workers < 1 {
		return fmt.Errorf("%w: invalid goroutine count %d", ErrInvalidPlan, workers)
	}

	capacity := 0
	for _, requested := range normalized {
		requestedEnd, _ := requested.end()
		alignedStart := requested.Offset &^ 1
		alignedEnd := (requestedEnd + 1) &^ 1
		capacity = max(capacity, int(min(stripeSize, alignedEnd-alignedStart)))
	}
	folder, err := par2gf16.NewFolder(p.matrix, capacity, workers)
	if err != nil {
		return err
	}
	defer folder.Close()

	for _, requested := range normalized {
		requestedEnd, _ := requested.end()
		alignedStart := requested.Offset &^ 1
		alignedEnd := (requestedEnd + 1) &^ 1
		for stripeStart := alignedStart; stripeStart < alignedEnd; {
			stripeLength64 := min(stripeSize, alignedEnd-stripeStart)
			stripeEnd := stripeStart + stripeLength64
			stripeLength := int(stripeEnd - stripeStart)
			err := folder.FoldSize(
				stripeLength,
				func(input int, dst []byte) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if input < len(p.available) {
						source := p.available[input]
						if err := source.Read(ctx, stripeStart, dst); err != nil {
							return fmt.Errorf("read source shard %d at %d+%d: %w", source.Shard, stripeStart, len(dst), err)
						}
						return nil
					}
					recovery := p.recovery[input-len(p.available)]
					if err := recovery.Read(ctx, stripeStart, dst); err != nil {
						return fmt.Errorf("read recovery exponent %d at %d+%d: %w", recovery.Exponent, stripeStart, len(dst), err)
					}
					return nil
				},
			)
			if err != nil {
				return err
			}

			emitStart := max(stripeStart, requested.Offset)
			emitEnd := min(stripeEnd, requestedEnd)
			if emitStart >= emitEnd {
				stripeStart = stripeEnd
				continue
			}
			for i, shard := range p.missing {
				data := folder.Output(i)[emitStart-stripeStart : emitEnd-stripeStart]
				if err := sink(ctx, shard, emitStart, data); err != nil {
					return fmt.Errorf("write recovered shard %d at %d+%d: %w", shard, emitStart, len(data), err)
				}
			}
			stripeStart = stripeEnd
		}
	}
	return nil
}

func normalizeRanges(ranges []ByteRange, sliceSize int64) ([]ByteRange, error) {
	normalized := make([]ByteRange, 0, len(ranges))
	for _, r := range ranges {
		end, ok := r.end()
		if !ok || end > sliceSize {
			return nil, fmt.Errorf("%w: %d+%d outside slice size %d", ErrInvalidRange, r.Offset, r.Length, sliceSize)
		}
		if r.Length == 0 {
			continue
		}
		normalized = append(normalized, r)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Offset < normalized[j].Offset })
	merged := normalized[:0]
	for _, r := range normalized {
		if len(merged) == 0 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		lastEnd, _ := last.end()
		rEnd, _ := r.end()
		if r.Offset > lastEnd {
			merged = append(merged, r)
			continue
		}
		if rEnd > lastEnd {
			last.Length = rEnd - last.Offset
		}
	}
	return merged, nil
}
