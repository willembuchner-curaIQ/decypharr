// Package par2 parses PAR2 metadata and reconstructs selected byte ranges of
// missing source slices. It deliberately has no knowledge of NNTP, files, or
// repository storage: callers supply range readers and a recovery sink.
package par2

import (
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	packetHeaderSize = 64

	packetTypeMain     = "PAR 2.0\x00Main\x00\x00\x00\x00"
	packetTypeFileDesc = "PAR 2.0\x00FileDesc"
	packetTypeIFSC     = "PAR 2.0\x00IFSC\x00\x00\x00\x00"
	packetTypeRecvSlic = "PAR 2.0\x00RecvSlic"
)

// RecoverySetID identifies one independent PAR2 recovery set.
type RecoverySetID [16]byte

func (id RecoverySetID) String() string { return hex.EncodeToString(id[:]) }

// FileID identifies a protected file inside a recovery set.
type FileID [16]byte

func (id FileID) String() string { return hex.EncodeToString(id[:]) }

// Digest is an MD5 digest stored by the PAR2 format.
type Digest [16]byte

func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// PacketType is the 16-byte on-wire packet type identifier.
type PacketType [16]byte

func (t PacketType) String() string { return strings.TrimRight(string(t[:]), "\x00") }

// PacketKind classifies packet types understood by this package.
type PacketKind uint8

const (
	PacketUnknown PacketKind = iota
	PacketMain
	PacketFileDescription
	PacketInputFileSliceChecksum
	PacketRecoverySlice
)

func (k PacketKind) String() string {
	switch k {
	case PacketMain:
		return "main"
	case PacketFileDescription:
		return "file description"
	case PacketInputFileSliceChecksum:
		return "input file slice checksum"
	case PacketRecoverySlice:
		return "recovery slice"
	default:
		return "unknown"
	}
}

// MainPacket describes the files and slice size belonging to a recovery set.
type MainPacket struct {
	SliceSize          uint64
	RecoveryFileIDs    []FileID
	NonRecoveryFileIDs []FileID
}

// FileDescriptionPacket describes one protected file.
type FileDescriptionPacket struct {
	FileID       FileID
	FileHash     Digest
	First16KHash Digest
	FileLength   uint64
	Filename     string
}

// SliceChecksum identifies one source slice. CRC32 is stored in standard
// numeric form even though the packet encodes it little-endian.
type SliceChecksum struct {
	MD5   Digest
	CRC32 uint32
}

// IFSCPacket contains the per-slice checksums for one protected file.
type IFSCPacket struct {
	FileID    FileID
	Checksums []SliceChecksum
}

// RecoverySlicePacket contains one recovery row. Data is exactly the encoded
// recovery slice and excludes the four-byte exponent prefix.
type RecoverySlicePacket struct {
	Exponent uint16
	Data     []byte
}

// UnknownPacket preserves a valid packet whose type is not interpreted.
type UnknownPacket struct {
	Type PacketType
	Body []byte
}

// Packet is one validated on-wire packet. Body and all typed byte slices are
// owned by the returned Index and never alias parser scratch storage. Large
// recovery bodies share immutable backing storage between the packet envelope
// and RecoverySet index instead of being copied for each view.
type Packet struct {
	Offset        int64
	Length        uint64
	Hash          Digest
	RecoverySetID RecoverySetID
	Type          PacketType
	Kind          PacketKind
	Body          []byte

	Main            *MainPacket
	FileDescription *FileDescriptionPacket
	IFSC            *IFSCPacket
	RecoverySlice   *RecoverySlicePacket
	Unknown         *UnknownPacket
}

// RecoverySet indexes packets belonging to one recovery set. PAR2 files
// commonly repeat metadata packets, so map values are slices rather than a
// single canonical packet.
type RecoverySet struct {
	ID RecoverySetID

	PacketIndexes    []int
	MainPackets      []MainPacket
	FileDescriptions map[FileID][]FileDescriptionPacket
	IFSC             map[FileID][]IFSCPacket
	RecoverySlices   map[uint16][]RecoverySlicePacket
	UnknownPackets   []UnknownPacket
}

// Index is the result of parsing one logical stream. Order records first-seen
// set order and Packets records wire order across all supplied blobs.
type Index struct {
	Sets    map[RecoverySetID]*RecoverySet
	Order   []RecoverySetID
	Packets []Packet
}

func newIndex() *Index {
	return &Index{Sets: make(map[RecoverySetID]*RecoverySet)}
}

func (i *Index) recoverySet(id RecoverySetID) *RecoverySet {
	if set := i.Sets[id]; set != nil {
		return set
	}
	set := &RecoverySet{
		ID:               id,
		FileDescriptions: make(map[FileID][]FileDescriptionPacket),
		IFSC:             make(map[FileID][]IFSCPacket),
		RecoverySlices:   make(map[uint16][]RecoverySlicePacket),
	}
	i.Sets[id] = set
	i.Order = append(i.Order, id)
	return set
}

func (i *Index) addPacket(packet Packet) {
	packetIndex := len(i.Packets)
	i.Packets = append(i.Packets, packet)
	set := i.recoverySet(packet.RecoverySetID)
	set.PacketIndexes = append(set.PacketIndexes, packetIndex)
	switch packet.Kind {
	case PacketMain:
		set.MainPackets = append(set.MainPackets, cloneMain(*packet.Main))
	case PacketFileDescription:
		desc := *packet.FileDescription
		set.FileDescriptions[desc.FileID] = append(set.FileDescriptions[desc.FileID], desc)
	case PacketInputFileSliceChecksum:
		ifsc := cloneIFSC(*packet.IFSC)
		set.IFSC[ifsc.FileID] = append(set.IFSC[ifsc.FileID], ifsc)
	case PacketRecoverySlice:
		recovery := cloneRecoverySlice(*packet.RecoverySlice)
		set.RecoverySlices[recovery.Exponent] = append(set.RecoverySlices[recovery.Exponent], recovery)
	case PacketUnknown:
		set.UnknownPackets = append(set.UnknownPackets, cloneUnknown(*packet.Unknown))
	}
}

func cloneMain(in MainPacket) MainPacket {
	in.RecoveryFileIDs = append([]FileID(nil), in.RecoveryFileIDs...)
	in.NonRecoveryFileIDs = append([]FileID(nil), in.NonRecoveryFileIDs...)
	return in
}

func cloneIFSC(in IFSCPacket) IFSCPacket {
	in.Checksums = append([]SliceChecksum(nil), in.Checksums...)
	return in
}

func cloneRecoverySlice(in RecoverySlicePacket) RecoverySlicePacket {
	// Recovery data can be many MiB. It already lives in parser-owned memory;
	// keep the index as a second immutable view rather than duplicating it.
	return in
}

func cloneUnknown(in UnknownPacket) UnknownPacket {
	return in
}

func packetTypeFromString(s string) PacketType {
	if len(s) != len(PacketType{}) {
		panic(fmt.Sprintf("PAR2 packet type has length %d, want 16", len(s)))
	}
	var out PacketType
	copy(out[:], s)
	return out
}
