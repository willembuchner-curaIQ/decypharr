package par2

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	ErrInvalidMagic   = errors.New("par2: invalid packet magic")
	ErrInvalidLength  = errors.New("par2: invalid packet length")
	ErrPacketTooLarge = errors.New("par2: packet exceeds configured limit")
	ErrPacketHash     = errors.New("par2: packet hash mismatch")
	ErrTruncated      = errors.New("par2: truncated packet")
	ErrInvalidPacket  = errors.New("par2: invalid packet body")
	ErrUnsafePath     = errors.New("par2: unsafe filename")
)

var packetMagic = [8]byte{'P', 'A', 'R', '2', 0, 'P', 'K', 'T'}

const DefaultMaxPacketSize uint64 = 512 << 20

// ParseOptions bounds allocations made while reading an untrusted stream.
type ParseOptions struct {
	// MaxPacketSize includes the 64-byte packet header. Zero selects
	// DefaultMaxPacketSize.
	MaxPacketSize uint64
}

// ParseError annotates a packet error with its logical stream offset.
type ParseError struct {
	Offset        int64
	RecoverySetID RecoverySetID
	Type          PacketType
	Err           error
}

func (e *ParseError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.RecoverySetID == (RecoverySetID{}) {
		return fmt.Sprintf("par2 packet at offset %d: %v", e.Offset, e.Err)
	}
	return fmt.Sprintf("par2 packet at offset %d (set %s, type %q): %v",
		e.Offset, e.RecoverySetID, e.Type.String(), e.Err)
}

func (e *ParseError) Unwrap() error { return e.Err }

// Parse parses one blob containing zero or more concatenated PAR2 packets.
func Parse(blob []byte) (*Index, error) {
	return ParseReader(bytes.NewReader(blob))
}

// ParseBlobs parses blobs as one logical byte stream. Packet headers and
// bodies may be split at arbitrary blob boundaries.
func ParseBlobs(blobs ...[]byte) (*Index, error) {
	readers := make([]io.Reader, len(blobs))
	for i := range blobs {
		readers[i] = bytes.NewReader(blobs[i])
	}
	return ParseReader(io.MultiReader(readers...))
}

// ParseReader parses concatenated packets from r with the default allocation
// limit.
func ParseReader(r io.Reader) (*Index, error) {
	return ParseReaderWithOptions(r, ParseOptions{})
}

// ParseReaderWithOptions parses concatenated packets from r.
func ParseReaderWithOptions(r io.Reader, options ParseOptions) (*Index, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: nil reader", ErrInvalidPacket)
	}
	maxPacketSize := options.MaxPacketSize
	if maxPacketSize == 0 {
		maxPacketSize = DefaultMaxPacketSize
	}
	if maxPacketSize < packetHeaderSize {
		return nil, fmt.Errorf("%w: maximum packet size %d is smaller than the header", ErrInvalidLength, maxPacketSize)
	}

	index := newIndex()
	var offset int64
	for {
		packet, err := readPacket(r, offset, maxPacketSize)
		if errors.Is(err, io.EOF) {
			return index, nil
		}
		if err != nil {
			return nil, err
		}
		index.addPacket(packet)
		if packet.Length > math.MaxInt64 || offset > math.MaxInt64-int64(packet.Length) {
			return nil, &ParseError{Offset: offset, Err: fmt.Errorf("%w: stream offset overflow", ErrInvalidLength)}
		}
		offset += int64(packet.Length)
	}
}

func readPacket(r io.Reader, offset int64, maxPacketSize uint64) (Packet, error) {
	var header [packetHeaderSize]byte
	n, err := io.ReadFull(r, header[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return Packet{}, io.EOF
		}
		return Packet{}, &ParseError{Offset: offset, Err: fmt.Errorf("%w: header has %d of %d bytes", ErrTruncated, n, packetHeaderSize)}
	}
	if !bytes.Equal(header[:8], packetMagic[:]) {
		return Packet{}, &ParseError{Offset: offset, Err: ErrInvalidMagic}
	}

	length := binary.LittleEndian.Uint64(header[8:16])
	if length < packetHeaderSize || length%4 != 0 || length > math.MaxInt64 {
		return Packet{}, &ParseError{Offset: offset, Err: fmt.Errorf("%w: %d", ErrInvalidLength, length)}
	}
	if length > maxPacketSize {
		return Packet{}, &ParseError{Offset: offset, Err: fmt.Errorf("%w: %d > %d", ErrPacketTooLarge, length, maxPacketSize)}
	}

	var hash Digest
	copy(hash[:], header[16:32])
	var setID RecoverySetID
	copy(setID[:], header[32:48])
	var packetType PacketType
	copy(packetType[:], header[48:64])

	bodyLength := int(length - packetHeaderSize)
	if uint64(bodyLength) != length-packetHeaderSize {
		return Packet{}, &ParseError{Offset: offset, Err: fmt.Errorf("%w: body does not fit in memory", ErrInvalidLength)}
	}
	body := make([]byte, bodyLength)
	if _, err := io.ReadFull(r, body); err != nil {
		return Packet{}, &ParseError{
			Offset: offset, RecoverySetID: setID, Type: packetType,
			Err: fmt.Errorf("%w: body needs %d bytes: %v", ErrTruncated, bodyLength, err),
		}
	}
	if computePacketHash(setID, packetType, body) != hash {
		return Packet{}, &ParseError{Offset: offset, RecoverySetID: setID, Type: packetType, Err: ErrPacketHash}
	}

	packet := Packet{
		Offset: offset, Length: length, Hash: hash, RecoverySetID: setID,
		Type: packetType, Body: body,
	}
	if err := decodePacketBody(&packet); err != nil {
		return Packet{}, &ParseError{
			Offset: offset, RecoverySetID: setID, Type: packetType,
			Err: fmt.Errorf("%w: %w", ErrInvalidPacket, err),
		}
	}
	return packet, nil
}

func computePacketHash(setID RecoverySetID, packetType PacketType, body []byte) Digest {
	h := md5.New()
	_, _ = h.Write(setID[:])
	_, _ = h.Write(packetType[:])
	_, _ = h.Write(body)
	var digest Digest
	copy(digest[:], h.Sum(nil))
	return digest
}

func decodePacketBody(packet *Packet) error {
	switch string(packet.Type[:]) {
	case packetTypeMain:
		main, err := parseMain(packet.Body)
		if err != nil {
			return err
		}
		packet.Kind, packet.Main = PacketMain, &main
	case packetTypeFileDesc:
		desc, err := parseFileDescription(packet.Body)
		if err != nil {
			return err
		}
		packet.Kind, packet.FileDescription = PacketFileDescription, &desc
	case packetTypeIFSC:
		ifsc, err := parseIFSC(packet.Body)
		if err != nil {
			return err
		}
		packet.Kind, packet.IFSC = PacketInputFileSliceChecksum, &ifsc
	case packetTypeRecvSlic:
		recovery, err := parseRecoverySlice(packet.Body)
		if err != nil {
			return err
		}
		packet.Kind, packet.RecoverySlice = PacketRecoverySlice, &recovery
	default:
		packet.Kind = PacketUnknown
		packet.Unknown = &UnknownPacket{Type: packet.Type, Body: packet.Body}
	}
	return nil
}

func parseMain(body []byte) (MainPacket, error) {
	if len(body) < 12 || (len(body)-12)%16 != 0 {
		return MainPacket{}, fmt.Errorf("main body has invalid length %d", len(body))
	}
	sliceSize := binary.LittleEndian.Uint64(body[:8])
	if sliceSize == 0 || sliceSize%4 != 0 || sliceSize > math.MaxInt64 {
		return MainPacket{}, fmt.Errorf("invalid slice size %d", sliceSize)
	}
	recoveryCount := uint64(binary.LittleEndian.Uint32(body[8:12]))
	fileCount := uint64((len(body) - 12) / 16)
	if recoveryCount == 0 || recoveryCount > fileCount {
		return MainPacket{}, fmt.Errorf("invalid recovery file count %d for %d file IDs", recoveryCount, fileCount)
	}

	ids := make([]FileID, int(fileCount))
	for i := range ids {
		copy(ids[i][:], body[12+i*16:12+(i+1)*16])
	}
	main := MainPacket{SliceSize: sliceSize}
	main.RecoveryFileIDs = append(main.RecoveryFileIDs, ids[:int(recoveryCount)]...)
	main.NonRecoveryFileIDs = append(main.NonRecoveryFileIDs, ids[int(recoveryCount):]...)
	return main, nil
}

func parseFileDescription(body []byte) (FileDescriptionPacket, error) {
	const fixedLength = 56
	if len(body) <= fixedLength || (len(body)-fixedLength)%4 != 0 {
		return FileDescriptionPacket{}, fmt.Errorf("file description body has invalid length %d", len(body))
	}
	var desc FileDescriptionPacket
	copy(desc.FileID[:], body[:16])
	copy(desc.FileHash[:], body[16:32])
	copy(desc.First16KHash[:], body[32:48])
	desc.FileLength = binary.LittleEndian.Uint64(body[48:56])
	if desc.FileLength > math.MaxInt64 {
		return FileDescriptionPacket{}, fmt.Errorf("file length %d exceeds supported range", desc.FileLength)
	}

	filenameBytes := body[fixedLength:]
	if nul := bytes.IndexByte(filenameBytes, 0); nul >= 0 {
		for _, b := range filenameBytes[nul:] {
			if b != 0 {
				return FileDescriptionPacket{}, errors.New("non-zero bytes follow filename terminator")
			}
		}
		filenameBytes = filenameBytes[:nul]
	}
	if len(filenameBytes) == 0 {
		return FileDescriptionPacket{}, errors.New("empty filename")
	}
	if !utf8.Valid(filenameBytes) {
		return FileDescriptionPacket{}, errors.New("filename is not valid UTF-8")
	}
	for _, r := range string(filenameBytes) {
		if unicode.IsControl(r) {
			return FileDescriptionPacket{}, fmt.Errorf("filename contains control character %U", r)
		}
	}
	desc.Filename = string(filenameBytes)
	if err := ValidateFilename(desc.Filename); err != nil {
		return FileDescriptionPacket{}, err
	}
	if computeFileID(desc.First16KHash, desc.FileLength, filenameBytes) != desc.FileID {
		return FileDescriptionPacket{}, errors.New("file ID does not match description")
	}
	return desc, nil
}

func computeFileID(first16K Digest, length uint64, filename []byte) FileID {
	h := md5.New()
	_, _ = h.Write(first16K[:])
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], length)
	_, _ = h.Write(size[:])
	_, _ = h.Write(filename)
	var id FileID
	copy(id[:], h.Sum(nil))
	return id
}

func parseIFSC(body []byte) (IFSCPacket, error) {
	if len(body) <= 16 || (len(body)-16)%20 != 0 {
		return IFSCPacket{}, fmt.Errorf("IFSC body has invalid length %d", len(body))
	}
	var packet IFSCPacket
	copy(packet.FileID[:], body[:16])
	packet.Checksums = make([]SliceChecksum, (len(body)-16)/20)
	for i := range packet.Checksums {
		start := 16 + i*20
		copy(packet.Checksums[i].MD5[:], body[start:start+16])
		packet.Checksums[i].CRC32 = binary.LittleEndian.Uint32(body[start+16 : start+20])
	}
	return packet, nil
}

func parseRecoverySlice(body []byte) (RecoverySlicePacket, error) {
	if len(body) <= 4 || len(body)%4 != 0 {
		return RecoverySlicePacket{}, fmt.Errorf("recovery slice body has invalid length %d", len(body))
	}
	exponent := binary.LittleEndian.Uint32(body[:4])
	if exponent > math.MaxUint16 {
		return RecoverySlicePacket{}, fmt.Errorf("recovery exponent %d exceeds 16 bits", exponent)
	}
	return RecoverySlicePacket{Exponent: uint16(exponent), Data: body[4:]}, nil
}

// ValidateFilename applies platform-independent path safety rules. Both slash
// styles are treated as separators so a PAR2 created on one OS cannot traverse
// directories when repaired on another.
func ValidateFilename(filename string) error {
	if filename == "" {
		return fmt.Errorf("%w: empty", ErrUnsafePath)
	}
	normalized := strings.ReplaceAll(filename, "\\", "/")
	if strings.HasPrefix(normalized, "/") || path.IsAbs(normalized) {
		return fmt.Errorf("%w: absolute path %q", ErrUnsafePath, filename)
	}
	if path.Clean(normalized) != normalized {
		return fmt.Errorf("%w: non-canonical path %q", ErrUnsafePath, filename)
	}
	for _, component := range strings.Split(normalized, "/") {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("%w: invalid component in %q", ErrUnsafePath, filename)
		}
		if strings.ContainsRune(component, ':') || strings.ContainsAny(component, "<>\"|?*") ||
			strings.HasSuffix(component, ".") || strings.HasSuffix(component, " ") {
			return fmt.Errorf("%w: unsafe component %q", ErrUnsafePath, component)
		}
		base := component
		if dot := strings.IndexByte(base, '.'); dot >= 0 {
			base = base[:dot]
		}
		upper := strings.ToUpper(base)
		if isWindowsDeviceName(upper) {
			return fmt.Errorf("%w: reserved component %q", ErrUnsafePath, component)
		}
	}
	return nil
}

func isWindowsDeviceName(name string) bool {
	switch name {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if len(name) == 4 && (strings.HasPrefix(name, "COM") || strings.HasPrefix(name, "LPT")) {
		return name[3] >= '1' && name[3] <= '9'
	}
	return false
}
