package main

import (
	"bytes"
	"errors"
	"flag"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestParseCLIBackwardCompatible(t *testing.T) {
	opts, err := parseCLI([]string{"release.nzb"}, new(bytes.Buffer))
	if err != nil {
		t.Fatalf("parseCLI: %v", err)
	}
	if opts.nzbFile != "release.nzb" {
		t.Fatalf("nzbFile=%q, want release.nzb", opts.nzbFile)
	}
	if opts.configPath != "data" || opts.maxConnections != 10 {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	if opts.repairMissing || opts.keepPAR2State || opts.par2DBPath != "" {
		t.Fatalf("unexpected opt-in defaults: %+v", opts)
	}
}

func TestParseCLIRepairOptions(t *testing.T) {
	opts, err := parseCLI([]string{
		"-config", "/config",
		"-par2-db", "/state/test.db",
		"-max-connections", "4",
		"-repair-missing",
		"-keep-par2-state",
		"release.nzb",
	}, new(bytes.Buffer))
	if err != nil {
		t.Fatalf("parseCLI: %v", err)
	}
	if opts.configPath != "/config" || opts.par2DBPath != "/state/test.db" || opts.maxConnections != 4 {
		t.Fatalf("unexpected options: %+v", opts)
	}
	if !opts.repairMissing || !opts.keepPAR2State {
		t.Fatalf("expected repair and retention options: %+v", opts)
	}
}

func TestParseCLIValidationAndHelp(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "missing NZB", args: nil},
		{name: "extra NZB", args: []string{"one.nzb", "two.nzb"}},
		{name: "invalid connections", args: []string{"-max-connections", "0", "one.nzb"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseCLI(test.args, new(bytes.Buffer)); err == nil {
				t.Fatal("parseCLI succeeded, want validation error")
			}
		})
	}

	var output bytes.Buffer
	_, err := parseCLI([]string{"-help"}, &output)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseCLI help error=%v, want flag.ErrHelp", err)
	}
	if !strings.Contains(output.String(), "-repair-missing") {
		t.Fatalf("help does not describe repair mode:\n%s", output.String())
	}
}

func TestCollectRepairTargetsDeduplicatesRanges(t *testing.T) {
	shared := storage.NZBSegment{
		MessageID:        "shared@example",
		RawFileKey:       7,
		RawOffset:        100,
		RawLength:        50,
		SegmentDataStart: 3,
	}
	nzb := &storage.NZB{Files: []storage.NZBFile{
		{Name: "movie.mkv", FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{
			shared,
			{MessageID: "shared@example", RawFileKey: 7, RawOffset: 150, RawLength: 25, SegmentDataStart: 53},
			{MessageID: "unique@example", RawFileKey: 8, RawLength: 100},
			{RawFileKey: 9, RawLength: 100},
		}},
		{Name: "duplicate.mkv", FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{shared}},
		{Name: "ignored.par2", FileType: storage.NZBFileTypePar2, Segments: []storage.NZBSegment{
			{MessageID: "parity@example", RawFileKey: 10, RawLength: 100},
		}},
		{Name: "deleted.mkv", FileType: storage.NZBFileTypeMedia, IsDeleted: true, Segments: []storage.NZBSegment{
			{MessageID: "deleted@example", RawFileKey: 11, RawLength: 100},
		}},
	}}

	targets := collectRepairTargets(nzb)
	if len(targets) != 2 {
		t.Fatalf("message IDs=%d, want 2: %#v", len(targets), targets)
	}
	if got := len(targets["shared@example"]); got != 2 {
		t.Fatalf("shared ranges=%d, want 2", got)
	}
	if got := len(targets["unique@example"]); got != 1 {
		t.Fatalf("unique ranges=%d, want 1", got)
	}
	if _, ok := targets["parity@example"]; ok {
		t.Fatal("PAR2 article should not be treated as a logical repair target")
	}
}

func TestValidateRecoveredRange(t *testing.T) {
	valid := storage.NZBSegment{RawFileKey: 1, RawLength: 4, SegmentDataStart: 2}
	if err := validateRecoveredRange(valid, make([]byte, 6)); err != nil {
		t.Fatalf("valid range: %v", err)
	}

	for _, test := range []struct {
		name    string
		segment storage.NZBSegment
		body    []byte
	}{
		{name: "no provenance", segment: storage.NZBSegment{RawLength: 1}, body: []byte{0}},
		{name: "zero length", segment: storage.NZBSegment{RawFileKey: 1}, body: []byte{0}},
		{name: "negative start", segment: storage.NZBSegment{RawFileKey: 1, RawLength: 1, SegmentDataStart: -1}, body: []byte{0}},
		{name: "short body", segment: storage.NZBSegment{RawFileKey: 1, RawLength: 4, SegmentDataStart: 2}, body: make([]byte, 5)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateRecoveredRange(test.segment, test.body); err == nil {
				t.Fatal("validateRecoveredRange succeeded, want error")
			}
		})
	}
}
