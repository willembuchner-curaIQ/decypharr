package recovery

const StoredSetVersion = 1

// StoredSet is the durable, engine-independent representation of a parsed
// PAR2 recovery set. Recovery payloads are deliberately not embedded; they are
// fetched and verified lazily and stored under their exponent.
type StoredSet struct {
	Version   uint32
	SetID     SetID
	SliceSize uint64
	Files     []StoredSourceFile
	Recovery  []RecoverySliceDescriptor
}

// StoredSourceFile is one PAR2 file-description and its optional input-slice
// checksums. RawFile maps the PAR2 FileID directly to manifest provenance;
// filenames are not unique or trustworthy enough for that join. FullMD5 and
// First16KMD5 come directly from the file-description packet.
type StoredSourceFile struct {
	FileID         FileID
	RawFile        RawFileKey
	Name           string
	Length         uint64
	FullMD5        [16]byte
	First16KMD5    [16]byte
	SliceChecksums []SliceChecksum
}

// SliceChecksum is the verification data for one source slice, in source
// order. The final slice is zero-padded for PAR2 arithmetic but its checksum is
// over the actual file bytes.
type SliceChecksum struct {
	MD5   [16]byte
	CRC32 uint32
}

// RecoverySliceDescriptor points at one recovery-slice payload in a raw PAR2
// file. PacketMD5 is the wire header's digest over set ID, packet type,
// little-endian exponent, and payload, so it can verify a lazily fetched body.
// PayloadOffset is the decoded raw-file offset of the first payload byte (the
// packet offset plus the 64-byte header and four-byte exponent).
// Duplicate exponents are allowed because releases commonly repeat the same
// packet in multiple volume files.
type RecoverySliceDescriptor struct {
	Exponent      uint32
	RawFile       RawFileKey
	PayloadOffset uint64
	PayloadLength uint64
	PacketMD5     [16]byte
}
