package par2

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/javi11/gopar-turbo/rsec16"
)

func TestRecoverSelectedRangesAgainstLibraryParity(t *testing.T) {
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
	coder, err := rsec16.NewCoderPAR2Vandermonde(dataShards, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	parity := coder.GenerateParity(data)

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
		*calls = append(*calls, readerCall{name: name, off: off, length: len(dst)})
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
