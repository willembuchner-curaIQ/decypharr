package par2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/sirrobot01/decypharr/internal/par2test"
)

func TestRecoverSelectedRangesAgainstPolynomialParity(t *testing.T) {
	const (
		dataShards = 6
		sliceSize  = 256
	)
	rng := rand.New(rand.NewSource(73))
	data := make([][]byte, dataShards)
	for i := range data {
		data[i] = make([]byte, sliceSize)
		if _, err := rng.Read(data[i]); err != nil {
			t.Fatal(err)
		}
	}
	parity := par2test.RecoverySet(data, 10)

	missing := []int{1, 4}
	missingSet := map[int]bool{1: true, 4: true}
	var reads []readerCall
	var sources []DataSource
	for shard := range dataShards {
		if missingSet[shard] {
			continue
		}
		sources = append(sources, DataSource{
			Shard: shard,
			Read:  trackingReader(fmt.Sprintf("data-%d", shard), data[shard], &reads),
		})
	}
	recoveries := []RecoverySource{
		{Exponent: 9, Read: trackingReader("recovery-9", parity[9], &reads)},
		{Exponent: 2, Read: trackingReader("recovery-2", parity[2], &reads)},
		{Exponent: 6, Read: trackingReader("recovery-6", parity[6], &reads)},
	}
	plan, err := NewPlan(PlanRequest{
		DataShards: dataShards,
		SliceSize:  sliceSize,
		Missing:    missing,
		Data:       sources,
		Recovery:   recoveries,
	})
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	if len(plan.RecoveryExponents()) != len(missing) {
		t.Fatalf("selected exponents=%v", plan.RecoveryExponents())
	}

	requested := []ByteRange{{Offset: 3, Length: 19}, {Offset: 101, Length: 37}}
	got := make(map[int][]byte)
	for _, shard := range missing {
		got[shard] = slices.Repeat([]byte{0xcc}, sliceSize)
	}
	var emitted []readerCall
	err = plan.Recover(context.Background(), requested, func(_ context.Context, shard int, off int64, recovered []byte) error {
		copy(got[shard][off:], recovered)
		emitted = append(emitted, readerCall{name: fmt.Sprintf("shard-%d", shard), off: off, length: len(recovered)})
		return nil
	}, RecoverOptions{StripeSize: 10, NumGoroutines: 2})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	for _, shard := range missing {
		for _, interval := range requested {
			end, _ := interval.end()
			if !slices.Equal(got[shard][interval.Offset:end], data[shard][interval.Offset:end]) {
				t.Fatalf("shard %d range %d+%d mismatch", shard, interval.Offset, interval.Length)
			}
		}
		for i, b := range got[shard] {
			if inRanges(int64(i), requested) {
				continue
			}
			if b != 0xcc {
				t.Fatalf("sink emitted unrequested byte for shard %d at %d", shard, i)
			}
		}
	}
	if len(emitted) == 0 {
		t.Fatal("recovery sink was not called")
	}
	for _, call := range emitted {
		if !rangeCovered(call.off, int64(call.length), requested) {
			t.Fatalf("sink emitted outside requested ranges: %+v", call)
		}
	}
	if len(reads) == 0 {
		t.Fatal("source readers were not called")
	}
	for _, call := range reads {
		if call.off%2 != 0 || call.length%2 != 0 || call.length > 10 {
			t.Fatalf("reader call is not a bounded GF stripe: %+v", call)
		}
		if !rangeCovered(call.off, int64(call.length), []ByteRange{{Offset: 2, Length: 20}, {Offset: 100, Length: 38}}) {
			t.Fatalf("reader fetched outside minimally aligned ranges: %+v", call)
		}
		if call.length == sliceSize {
			t.Fatalf("reader fetched the full slice: %+v", call)
		}
	}
}

func TestRecoverRandomizedAgainstPolynomialParity(t *testing.T) {
	rng := rand.New(rand.NewSource(0x5eed))
	for iteration := range 40 {
		dataShards := 2 + rng.Intn(11)
		sliceSize := 2 * (1 + rng.Intn(257))
		data := make([][]byte, dataShards)
		for shard := range data {
			data[shard] = make([]byte, sliceSize)
			if _, err := rng.Read(data[shard]); err != nil {
				t.Fatal(err)
			}
		}

		missingCount := 1 + rng.Intn(min(dataShards, 4))
		shards := rng.Perm(dataShards)
		missing := append([]int(nil), shards[:missingCount]...)
		missingSet := make(map[int]struct{}, missingCount)
		for _, shard := range missing {
			missingSet[shard] = struct{}{}
		}
		available := make([]DataSource, 0, dataShards-missingCount)
		for shard := range dataShards {
			if _, absent := missingSet[shard]; absent {
				continue
			}
			available = append(available, DataSource{Shard: shard, Read: trackingReader("data", data[shard], nil)})
		}

		exponentOrder := rng.Perm(missingCount + 3)
		recovery := make([]RecoverySource, len(exponentOrder))
		for index, exponent := range exponentOrder {
			parity := par2test.Recovery(data, uint16(exponent))
			recovery[index] = RecoverySource{
				Exponent: uint16(exponent),
				Read:     trackingReader("recovery", parity, nil),
			}
		}
		plan, err := NewPlan(PlanRequest{
			DataShards: dataShards,
			SliceSize:  int64(sliceSize),
			Missing:    missing,
			Data:       available,
			Recovery:   recovery,
		})
		if err != nil {
			t.Fatalf("iteration %d NewPlan: %v", iteration, err)
		}

		recovered := make(map[int][]byte, missingCount)
		for _, shard := range missing {
			recovered[shard] = make([]byte, sliceSize)
		}
		err = plan.Recover(
			context.Background(),
			[]ByteRange{{Length: int64(sliceSize)}},
			func(_ context.Context, shard int, offset int64, value []byte) error {
				copy(recovered[shard][offset:], value)
				return nil
			},
			RecoverOptions{StripeSize: int64(2 + rng.Intn(63)), NumGoroutines: 1 + rng.Intn(4)},
		)
		if err != nil {
			t.Fatalf("iteration %d Recover: %v", iteration, err)
		}
		for _, shard := range missing {
			if !slices.Equal(recovered[shard], data[shard]) {
				t.Fatalf("iteration %d shard %d mismatch", iteration, shard)
			}
		}
	}
}

func TestPlanContractAndCopies(t *testing.T) {
	reader := func(context.Context, int64, []byte) error { return nil }
	plan, err := NewPlan(PlanRequest{
		DataShards: 2,
		SliceSize:  64,
		Missing:    []int{1},
		Data:       []DataSource{{Shard: 0, Read: reader}},
		Recovery:   []RecoverySource{{Exponent: 7, Read: reader}, {Exponent: 7, Read: reader}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.SliceSize() != 64 || plan.SelectionAttempts() != 1 {
		t.Fatalf("unexpected plan metadata: size=%d attempts=%d", plan.SliceSize(), plan.SelectionAttempts())
	}
	missing := plan.MissingShards()
	exponents := plan.RecoveryExponents()
	if !slices.Equal(missing, []int{1}) || !slices.Equal(exponents, []uint16{7}) {
		t.Fatalf("unexpected plan selection: missing=%v exponents=%v", missing, exponents)
	}
	missing[0] = 0
	exponents[0] = 0
	if !slices.Equal(plan.MissingShards(), []int{1}) || !slices.Equal(plan.RecoveryExponents(), []uint16{7}) {
		t.Fatal("plan accessors exposed mutable storage")
	}

	var nilPlan *Plan
	if nilPlan.MissingShards() != nil || nilPlan.SliceSize() != 0 ||
		nilPlan.RecoveryExponents() != nil || nilPlan.SelectionAttempts() != 0 {
		t.Fatal("nil plan accessors returned nonzero values")
	}
	sink := func(context.Context, int, int64, []byte) error { return nil }
	if err := nilPlan.Recover(context.Background(), nil, sink, RecoverOptions{}); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("nil Plan.Recover error=%v, want ErrInvalidPlan", err)
	}
}

func TestNewPlanRejectsInvalidContracts(t *testing.T) {
	reader := func(context.Context, int64, []byte) error { return nil }
	valid := func() PlanRequest {
		return PlanRequest{
			DataShards: 2,
			SliceSize:  64,
			Missing:    []int{1},
			Data:       []DataSource{{Shard: 0, Read: reader}},
			Recovery:   []RecoverySource{{Exponent: 0, Read: reader}},
		}
	}
	tests := []struct {
		name   string
		change func(*PlanRequest)
	}{
		{name: "zero shard count", change: func(r *PlanRequest) { r.DataShards = 0 }},
		{name: "excessive shard count", change: func(r *PlanRequest) { r.DataShards = maxPAR2DataShards + 1 }},
		{name: "zero slice", change: func(r *PlanRequest) { r.SliceSize = 0 }},
		{name: "odd slice", change: func(r *PlanRequest) { r.SliceSize = 63 }},
		{name: "no missing shards", change: func(r *PlanRequest) { r.Missing = nil }},
		{name: "missing shard out of range", change: func(r *PlanRequest) { r.Missing = []int{2} }},
		{name: "duplicate missing shard", change: func(r *PlanRequest) { r.Missing = []int{1, 1} }},
		{name: "data shard out of range", change: func(r *PlanRequest) { r.Data[0].Shard = 2 }},
		{name: "missing shard supplied", change: func(r *PlanRequest) { r.Data[0].Shard = 1 }},
		{name: "nil data reader", change: func(r *PlanRequest) { r.Data[0].Read = nil }},
		{name: "duplicate data shard", change: func(r *PlanRequest) { r.Data = append(r.Data, r.Data[0]) }},
		{name: "available shard absent", change: func(r *PlanRequest) { r.Data = nil }},
		{name: "nil recovery reader", change: func(r *PlanRequest) { r.Recovery[0].Read = nil }},
		{name: "negative matrix limit", change: func(r *PlanRequest) { r.MaxMatrixBytes = -1 }},
		{name: "matrix limit exceeded", change: func(r *PlanRequest) { r.MaxMatrixBytes = 1 }},
		{name: "negative selection limit", change: func(r *PlanRequest) { r.MaxSelectionAttempts = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid()
			test.change(&request)
			if _, err := NewPlan(request); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("NewPlan error=%v, want ErrInvalidPlan", err)
			}
		})
	}
}

func TestRecoverContractErrors(t *testing.T) {
	errRecovery := errors.New("recovery read failed")
	errSink := errors.New("sink failed")
	reader := func(context.Context, int64, []byte) error { return nil }
	request := PlanRequest{
		DataShards: 2,
		SliceSize:  64,
		Missing:    []int{1},
		Data:       []DataSource{{Shard: 0, Read: reader}},
		Recovery: []RecoverySource{{Exponent: 0, Read: func(context.Context, int64, []byte) error {
			return errRecovery
		}}},
	}
	plan, err := NewPlan(request)
	if err != nil {
		t.Fatal(err)
	}
	sink := func(context.Context, int, int64, []byte) error { return nil }
	if err := plan.Recover(context.Background(), nil, nil, RecoverOptions{}); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("nil sink error=%v, want ErrInvalidPlan", err)
	}
	if err := plan.Recover(context.Background(), []ByteRange{{Length: 0}}, sink, RecoverOptions{}); err != nil {
		t.Fatalf("empty recovery: %v", err)
	}
	if err := plan.Recover(context.Background(), []ByteRange{{Length: 2}}, sink, RecoverOptions{StripeSize: 1}); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("small stripe error=%v, want ErrInvalidRange", err)
	}
	if err := plan.Recover(context.Background(), []ByteRange{{Length: 2}}, sink, RecoverOptions{NumGoroutines: -1}); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("worker error=%v, want ErrInvalidPlan", err)
	}
	if err := plan.Recover(nil, []ByteRange{{Length: 2}}, sink, RecoverOptions{}); !errors.Is(err, errRecovery) {
		t.Fatalf("recovery reader error=%v, want %v", err, errRecovery)
	}

	request.Recovery[0].Read = reader
	plan, err = NewPlan(request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := plan.Recover(ctx, []ByteRange{{Length: 2}}, sink, RecoverOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled recovery error=%v, want context.Canceled", err)
	}
	if err := plan.Recover(context.Background(), []ByteRange{{Length: 2}}, func(context.Context, int, int64, []byte) error {
		return errSink
	}, RecoverOptions{}); !errors.Is(err, errSink) {
		t.Fatalf("sink error=%v, want %v", err, errSink)
	}
}

func TestRecoveryErrorsAndLimits(t *testing.T) {
	notEnough := &NotEnoughRecoveryError{Missing: 3, Available: 2}
	if !errors.Is(notEnough, ErrNotEnoughRecovery) || notEnough.Error() == "" {
		t.Fatalf("invalid not-enough error: %v", notEnough)
	}
	singular := &SingularSelectionError{Attempts: 4, Exponents: []uint16{1, 2}, Exhausted: true}
	if !errors.Is(singular, ErrSingularSelection) || singular.Error() == "" {
		t.Fatalf("invalid singular error: %v", singular)
	}
	if saturatingDouble(math.MaxUint64) != math.MaxUint64 || saturatingDouble(7) != 14 {
		t.Fatal("saturatingDouble returned an invalid result")
	}
}

func TestPlannerRetriesSingularSelection(t *testing.T) {
	zeroReader := func(context.Context, int64, []byte) error { return nil }
	request := PlanRequest{
		DataShards: 3,
		SliceSize:  64,
		Missing:    []int{0, 2},
		Data:       []DataSource{{Shard: 1, Read: zeroReader}},
		// For source generators 2^1 and 2^4, rows 0 and 21845
		// coincide: (2^(1-4))^21845 == 1. Row 1 repairs the rank.
		Recovery: []RecoverySource{
			{Exponent: 0, Read: zeroReader},
			{Exponent: 21845, Read: zeroReader},
			{Exponent: 1, Read: zeroReader},
		},
		MaxSelectionAttempts: 10,
	}
	plan, err := NewPlan(request)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	if plan.SelectionAttempts() != 2 {
		t.Fatalf("selection attempts=%d, want 2", plan.SelectionAttempts())
	}
	if got := plan.RecoveryExponents(); !slices.Equal(got, []uint16{0, 1}) {
		t.Fatalf("selected exponents=%v, want [0 1]", got)
	}
}

func TestPlannerReturnsTypedSingularSelection(t *testing.T) {
	zeroReader := func(context.Context, int64, []byte) error { return nil }
	_, err := NewPlan(PlanRequest{
		DataShards: 3,
		SliceSize:  64,
		Missing:    []int{0, 2},
		Data:       []DataSource{{Shard: 1, Read: zeroReader}},
		Recovery: []RecoverySource{
			{Exponent: 0, Read: zeroReader},
			{Exponent: 21845, Read: zeroReader},
		},
	})
	if !errors.Is(err, ErrSingularSelection) {
		t.Fatalf("NewPlan error=%v, want ErrSingularSelection", err)
	}
	var singular *SingularSelectionError
	if !errors.As(err, &singular) || singular.Attempts != 1 || singular.Exhausted {
		t.Fatalf("unexpected singular error: %#v", err)
	}
}

func TestPlannerReturnsTypedNotEnoughRecovery(t *testing.T) {
	zeroReader := func(context.Context, int64, []byte) error { return nil }
	_, err := NewPlan(PlanRequest{
		DataShards: 3,
		SliceSize:  64,
		Missing:    []int{0, 2},
		Data:       []DataSource{{Shard: 1, Read: zeroReader}},
		Recovery:   []RecoverySource{{Exponent: 0, Read: zeroReader}},
	})
	if !errors.Is(err, ErrNotEnoughRecovery) {
		t.Fatalf("NewPlan error=%v, want ErrNotEnoughRecovery", err)
	}
	var notEnough *NotEnoughRecoveryError
	if !errors.As(err, &notEnough) || notEnough.Missing != 2 || notEnough.Available != 1 {
		t.Fatalf("unexpected not-enough error: %#v", err)
	}
}

func TestRecoverValidatesRangesAndPropagatesReaderErrors(t *testing.T) {
	errRead := errors.New("reader failed")
	plan, err := NewPlan(PlanRequest{
		DataShards: 2,
		SliceSize:  64,
		Missing:    []int{1},
		Data: []DataSource{{Shard: 0, Read: func(context.Context, int64, []byte) error {
			return errRead
		}}},
		Recovery: []RecoverySource{{Exponent: 0, Read: func(context.Context, int64, []byte) error {
			return nil
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := func(context.Context, int, int64, []byte) error { return nil }
	if err := plan.Recover(context.Background(), []ByteRange{{Offset: 63, Length: 2}}, sink, RecoverOptions{}); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("range error=%v, want ErrInvalidRange", err)
	}
	if err := plan.Recover(context.Background(), []ByteRange{{Offset: 0, Length: 2}}, sink, RecoverOptions{}); !errors.Is(err, errRead) {
		t.Fatalf("reader error=%v, want wrapped source error", err)
	}
}

func FuzzNormalizeRanges(f *testing.F) {
	for _, seed := range []struct {
		offset int64
		length int64
	}{
		{0, 0},
		{0, 1024},
		{3, 19},
		{1023, 1},
		{math.MaxInt64, 1},
		{-1, 2},
	} {
		f.Add(seed.offset, seed.length)
	}
	f.Fuzz(func(t *testing.T, offset, length int64) {
		const sliceSize = int64(1024)
		ranges, err := normalizeRanges([]ByteRange{
			{Offset: offset, Length: length},
			{Offset: 11, Length: 29},
			{Offset: 17, Length: 41},
		}, sliceSize)
		validEnd, valid := (ByteRange{Offset: offset, Length: length}).end()
		if !valid || validEnd > sliceSize {
			if !errors.Is(err, ErrInvalidRange) {
				t.Fatalf("normalizeRanges error = %v, want ErrInvalidRange", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		for index, interval := range ranges {
			end, ok := interval.end()
			if !ok || interval.Length <= 0 || end > sliceSize {
				t.Fatalf("invalid normalized interval: %+v", interval)
			}
			if index > 0 {
				previousEnd, _ := ranges[index-1].end()
				if interval.Offset <= previousEnd {
					t.Fatalf("unmerged intervals: %+v", ranges)
				}
			}
		}
	})
}

type readerCall struct {
	name   string
	off    int64
	length int
}

func trackingReader(name string, data []byte, calls *[]readerCall) RangeReader {
	return func(_ context.Context, off int64, dst []byte) error {
		if off < 0 || off > int64(len(data))-int64(len(dst)) {
			return fmt.Errorf("%s read out of range: %d+%d", name, off, len(dst))
		}
		copy(dst, data[off:off+int64(len(dst))])
		if calls != nil {
			*calls = append(*calls, readerCall{name: name, off: off, length: len(dst)})
		}
		return nil
	}
}

func inRanges(offset int64, ranges []ByteRange) bool {
	for _, interval := range ranges {
		end, _ := interval.end()
		if offset >= interval.Offset && offset < end {
			return true
		}
	}
	return false
}

func rangeCovered(offset, length int64, ranges []ByteRange) bool {
	end := offset + length
	for _, interval := range ranges {
		intervalEnd, _ := interval.end()
		if offset >= interval.Offset && end <= intervalEnd {
			return true
		}
	}
	return false
}
