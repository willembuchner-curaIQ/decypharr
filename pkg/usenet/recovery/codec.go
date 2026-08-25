package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const (
	recordFormatVersion = 1
	recordHeaderSize    = 48
	maxMetadataBytes    = 256 << 20
	maxStringBytes      = 16 << 20
	maxNZBIDBytes       = 1024
	maxRawRecordBytes   = math.MaxInt32 - recordHeaderSize
)

var recordMagic = [4]byte{'G', 'B', 'P', '2'}

type recordKind byte

const (
	recordManifest recordKind = iota + 1
	recordParsedSet
	recordRecoverySlice
	recordPatch
)

func (k recordKind) String() string {
	switch k {
	case recordManifest:
		return "manifest"
	case recordParsedSet:
		return "parsed set"
	case recordRecoverySlice:
		return "recovery slice"
	case recordPatch:
		return "repair patch"
	default:
		return fmt.Sprintf("record kind %d", k)
	}
}

type recordCodec byte

const (
	codecRaw recordCodec = iota
	codecZstd
)

func newMetadataCodec() (*zstd.Encoder, *zstd.Decoder, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create PAR2 metadata encoder: %w", err)
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(maxMetadataBytes),
		zstd.WithDecoderMaxWindow(64<<20),
		zstd.WithDecodeAllCapLimit(true),
	)
	if err != nil {
		_ = encoder.Close()
		return nil, nil, fmt.Errorf("create PAR2 metadata decoder: %w", err)
	}
	return encoder, decoder, nil
}

func wrapRecord(kind recordKind, codec recordCodec, logical []byte, encoder *zstd.Encoder) ([]byte, error) {
	limit := maxRawRecordBytes
	if codec == codecZstd {
		limit = maxMetadataBytes
	}
	if len(logical) > limit {
		return nil, &ValidationError{Field: kind.String(), Reason: fmt.Sprintf("record is %d bytes; maximum is %d", len(logical), limit)}
	}

	stored := logical
	switch codec {
	case codecRaw:
		// append below copies caller-owned data into the framed value.
	case codecZstd:
		if encoder == nil {
			return nil, errors.New("metadata encoder is unavailable")
		}
		stored = encoder.EncodeAll(logical, nil)
	default:
		return nil, fmt.Errorf("unknown record codec %d", codec)
	}
	if len(stored) > maxRawRecordBytes {
		return nil, &ValidationError{Field: kind.String(), Reason: "encoded record exceeds append-store limit"}
	}

	result := make([]byte, recordHeaderSize+len(stored))
	copy(result[:4], recordMagic[:])
	result[4] = recordFormatVersion
	result[5] = byte(kind)
	result[6] = byte(codec)
	binary.BigEndian.PutUint64(result[8:16], uint64(len(logical)))
	sum := sha256.Sum256(logical)
	copy(result[16:48], sum[:])
	copy(result[recordHeaderSize:], stored)
	return result, nil
}

func unwrapRecord(data []byte, expected recordKind, decoder *zstd.Decoder) ([]byte, error) {
	if len(data) < recordHeaderSize {
		return nil, errors.New("record is shorter than its header")
	}
	if !bytes.Equal(data[:4], recordMagic[:]) {
		return nil, errors.New("invalid record magic")
	}
	if data[4] != recordFormatVersion {
		return nil, &UnsupportedVersionError{Kind: expected.String(), Version: uint64(data[4])}
	}
	if recordKind(data[5]) != expected {
		return nil, fmt.Errorf("record kind is %s, expected %s", recordKind(data[5]), expected)
	}
	if data[7] != 0 {
		return nil, errors.New("record uses non-zero reserved flags")
	}

	logicalLength := binary.BigEndian.Uint64(data[8:16])
	if logicalLength > uint64(maxRawRecordBytes) || logicalLength > uint64(maxInt()) {
		return nil, errors.New("record length exceeds local limits")
	}
	stored := data[recordHeaderSize:]
	var logical []byte
	switch recordCodec(data[6]) {
	case codecRaw:
		if logicalLength != uint64(len(stored)) {
			return nil, errors.New("raw record length does not match header")
		}
		// appendstore.Get already returns caller-owned bytes. Retain that owned
		// view so high-entropy parity and patch reads do not incur another full
		// payload copy merely to remove the framing header.
		logical = stored
	case codecZstd:
		if logicalLength > maxMetadataBytes {
			return nil, errors.New("compressed record exceeds metadata limit")
		}
		if decoder == nil {
			return nil, errors.New("metadata decoder is unavailable")
		}
		var err error
		logical, err = decoder.DecodeAll(stored, make([]byte, 0, int(logicalLength)))
		if err != nil {
			return nil, fmt.Errorf("decompress record: %w", err)
		}
		if uint64(len(logical)) != logicalLength {
			return nil, errors.New("decompressed record length does not match header")
		}
	default:
		return nil, fmt.Errorf("unknown record codec %d", data[6])
	}

	want := data[16:48]
	got := sha256.Sum256(logical)
	if !bytes.Equal(want, got[:]) {
		return nil, errors.New("record SHA-256 does not match contents")
	}
	return logical, nil
}

type compactWriter struct {
	data []byte
}

func (w *compactWriter) u64(value uint64)   { w.data = binary.AppendUvarint(w.data, value) }
func (w *compactWriter) i64(value int64)    { w.data = binary.AppendVarint(w.data, value) }
func (w *compactWriter) fixed(value []byte) { w.data = append(w.data, value...) }
func (w *compactWriter) string(value string) {
	w.u64(uint64(len(value)))
	w.data = append(w.data, value...)
}

type compactReader struct {
	data []byte
	off  int
}

func (r *compactReader) u64(field string) (uint64, error) {
	if r.off >= len(r.data) {
		return 0, fmt.Errorf("%s: unexpected end of record", field)
	}
	value, n := binary.Uvarint(r.data[r.off:])
	if n <= 0 {
		return 0, fmt.Errorf("%s: invalid unsigned integer", field)
	}
	r.off += n
	return value, nil
}

func (r *compactReader) i64(field string) (int64, error) {
	if r.off >= len(r.data) {
		return 0, fmt.Errorf("%s: unexpected end of record", field)
	}
	value, n := binary.Varint(r.data[r.off:])
	if n <= 0 {
		return 0, fmt.Errorf("%s: invalid signed integer", field)
	}
	r.off += n
	return value, nil
}

func (r *compactReader) fixed(field string, size int) ([]byte, error) {
	if size < 0 || size > len(r.data)-r.off {
		return nil, fmt.Errorf("%s: unexpected end of record", field)
	}
	value := r.data[r.off : r.off+size]
	r.off += size
	return value, nil
}

func (r *compactReader) string(field string) (string, error) {
	size, err := r.u64(field + " length")
	if err != nil {
		return "", err
	}
	if size > maxStringBytes || size > uint64(len(r.data)-r.off) {
		return "", fmt.Errorf("%s: invalid string length %d", field, size)
	}
	value := string(r.data[r.off : r.off+int(size)])
	r.off += int(size)
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("%s: string is not valid UTF-8", field)
	}
	return value, nil
}

func (r *compactReader) count(field string) (int, error) {
	value, err := r.u64(field)
	if err != nil {
		return 0, err
	}
	if value > uint64(maxInt()) || value > maxMetadataBytes {
		return 0, fmt.Errorf("%s: collection length %d is too large", field, value)
	}
	return int(value), nil
}

func (r *compactReader) boundedCount(field string, maximum int) (int, error) {
	value, err := r.count(field)
	if err != nil {
		return 0, err
	}
	if value > maximum {
		return 0, fmt.Errorf("%s: collection length %d exceeds %d", field, value, maximum)
	}
	return value, nil
}

func (r *compactReader) done() error {
	if r.off != len(r.data) {
		return fmt.Errorf("record has %d trailing bytes", len(r.data)-r.off)
	}
	return nil
}

func maxInt() int { return int(^uint(0) >> 1) }

func validateString(field, value string, required bool) error {
	if required && value == "" {
		return &ValidationError{Field: field, Reason: "must not be empty"}
	}
	if len(value) > maxStringBytes {
		return &ValidationError{Field: field, Reason: fmt.Sprintf("exceeds %d bytes", maxStringBytes)}
	}
	if !utf8.ValidString(value) {
		return &ValidationError{Field: field, Reason: "must be valid UTF-8"}
	}
	if strings.IndexByte(value, 0) >= 0 {
		return &ValidationError{Field: field, Reason: "must not contain NUL"}
	}
	return nil
}

func validateNZBID(nzbID string) error {
	if err := validateString("NZB ID", nzbID, true); err != nil {
		return err
	}
	if len(nzbID) > maxNZBIDBytes {
		return &ValidationError{Field: "NZB ID", Reason: fmt.Sprintf("exceeds %d bytes", maxNZBIDBytes)}
	}
	return nil
}
