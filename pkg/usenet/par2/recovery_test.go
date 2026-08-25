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
