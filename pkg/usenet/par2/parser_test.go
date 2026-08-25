package par2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestParseBlobsKnownPacketsAndMultipleSets(t *testing.T) {
	setA := RecoverySetID{1, 2, 3}
	setB := RecoverySetID{9, 8, 7}
	first16K := Digest{4, 5, 6}
	filename := "release/movie.mkv"
	fileID := computeFileID(first16K, 12345, []byte(filename))

	mainBody := encodeMainBody(64, []FileID{fileID}, []FileID{{0xaa}})
	descBody := encodeFileDescriptionBody(fileID, Digest{7}, first16K, 12345, filename)
	ifscBody := encodeIFSCBody(fileID, []SliceChecksum{
		{MD5: Digest{1}, CRC32: 0x12345678},
		{MD5: Digest{2}, CRC32: 0x90abcdef},
	})
	recoveryBody := make([]byte, 4+64)
	binary.LittleEndian.PutUint32(recoveryBody[:4], 37)
	for i := 4; i < len(recoveryBody); i++ {
		recoveryBody[i] = byte(i)
	}
	unknownType := packetTypeFromString("PAR 2.0\x00Future!!")

	stream := bytes.Join([][]byte{
		encodePacket(setA, packetTypeFromString(packetTypeMain), mainBody),
		encodePacket(setA, packetTypeFromString(packetTypeFileDesc), descBody),
		encodePacket(setA, packetTypeFromString(packetTypeIFSC), ifscBody),
		encodePacket(setA, packetTypeFromString(packetTypeRecvSlic), recoveryBody),
		encodePacket(setA, unknownType, []byte{1, 2, 3, 4}),
		encodePacket(setB, packetTypeFromString(packetTypeMain), encodeMainBody(128, []FileID{{0xbb}}, nil)),
	}, nil)

	// Split in headers, packet bodies, and the boundary between packets.
	blobs := splitBytes(stream, 1, 7, 63, 65, 113, 197, 311, len(stream)-2)
	index, err := ParseBlobs(blobs...)
	if err != nil {
		t.Fatalf("ParseBlobs: %v", err)
	}
	if len(index.Sets) != 2 || len(index.Order) != 2 {
		t.Fatalf("sets=%d order=%d", len(index.Sets), len(index.Order))
	}
	if index.Order[0] != setA || index.Order[1] != setB {
		t.Fatalf("unexpected set order: %v", index.Order)
	}
	if len(index.Packets) != 6 {
		t.Fatalf("packets=%d, want 6", len(index.Packets))
	}

	a := index.Sets[setA]
	if a == nil || len(a.MainPackets) != 1 {
		t.Fatalf("set A main packets: %+v", a)
	}
	if a.MainPackets[0].SliceSize != 64 || a.MainPackets[0].RecoveryFileIDs[0] != fileID {
		t.Fatalf("unexpected main packet: %+v", a.MainPackets[0])
	}
	descriptions := a.FileDescriptions[fileID]
	if len(descriptions) != 1 || descriptions[0].Filename != filename || descriptions[0].FileLength != 12345 {
		t.Fatalf("unexpected file descriptions: %+v", descriptions)
	}
	checksums := a.IFSC[fileID]
	if len(checksums) != 1 || len(checksums[0].Checksums) != 2 || checksums[0].Checksums[1].CRC32 != 0x90abcdef {
		t.Fatalf("unexpected IFSC packets: %+v", checksums)
	}
	recovery := a.RecoverySlices[37]
	if len(recovery) != 1 || len(recovery[0].Data) != 64 || recovery[0].Data[0] != 4 {
		t.Fatalf("unexpected recovery slice: %+v", recovery)
	}
	if len(a.UnknownPackets) != 1 || a.UnknownPackets[0].Type != unknownType || !bytes.Equal(a.UnknownPackets[0].Body, []byte{1, 2, 3, 4}) {
		t.Fatalf("unknown packet was not preserved: %+v", a.UnknownPackets)
	}
	if got := index.Sets[setB].MainPackets[0].SliceSize; got != 128 {
		t.Fatalf("set B slice size=%d", got)
	}
	for i, packet := range index.Packets {
		if packet.Offset < 0 || packet.Length < packetHeaderSize || packet.Hash == (Digest{}) {
			t.Fatalf("packet %d has incomplete envelope: %+v", i, packet)
		}
	}
}

func TestParseRejectsCorruptPacketHash(t *testing.T) {
	packet := encodePacket(RecoverySetID{1}, packetTypeFromString(packetTypeRecvSlic), append([]byte{0, 0, 0, 0}, make([]byte, 8)...))
	packet[len(packet)-1] ^= 0xff
	_, err := Parse(packet)
	if !errors.Is(err, ErrPacketHash) {
		t.Fatalf("Parse error=%v, want ErrPacketHash", err)
	}
	var parseErr *ParseError
	if !errors.As(err, &parseErr) || parseErr.Offset != 0 {
		t.Fatalf("missing parse context: %#v", err)
	}
}

func TestParseRejectsTruncatedPackets(t *testing.T) {
	complete := encodePacket(RecoverySetID{1}, packetTypeFromString(packetTypeRecvSlic), append([]byte{0, 0, 0, 0}, make([]byte, 8)...))
	for _, tc := range []struct {
		name string
		blob []byte
	}{
		{name: "header", blob: complete[:17]},
		{name: "body", blob: complete[:len(complete)-1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.blob)
			if !errors.Is(err, ErrTruncated) {
				t.Fatalf("Parse error=%v, want ErrTruncated", err)
			}
		})
	}
}

func TestParsePreservesUnknownPacket(t *testing.T) {
	setID := RecoverySetID{3}
	typ := packetTypeFromString("PAR 2.0\x00Vendor!!")
	index, err := Parse(encodePacket(setID, typ, []byte{9, 8, 7, 6}))
	if err != nil {
		t.Fatal(err)
	}
	packet := index.Packets[0]
	if packet.Kind != PacketUnknown || packet.Unknown == nil || !bytes.Equal(packet.Unknown.Body, []byte{9, 8, 7, 6}) {
		t.Fatalf("unexpected packet: %+v", packet)
	}
}

func TestParseRejectsUnsafeFilename(t *testing.T) {
	for _, filename := range []string{
		"../escape.bin",
		"/absolute.bin",
		"C:\\escape.bin",
		"safe/../../escape.bin",
		"safe/CON.txt",
	} {
		t.Run(filename, func(t *testing.T) {
			first16K := Digest{4}
			fileID := computeFileID(first16K, 42, []byte(filename))
			body := encodeFileDescriptionBody(fileID, Digest{}, first16K, 42, filename)
			_, err := Parse(encodePacket(RecoverySetID{1}, packetTypeFromString(packetTypeFileDesc), body))
			if !errors.Is(err, ErrUnsafePath) {
				t.Fatalf("Parse error=%v, want ErrUnsafePath", err)
			}
		})
	}
}

func TestParseAcceptsSafeUTF8Filename(t *testing.T) {
	filename := "Séries/映画.mkv"
	first16K := Digest{4}
	fileID := computeFileID(first16K, 42, []byte(filename))
	body := encodeFileDescriptionBody(fileID, Digest{}, first16K, 42, filename)
	index, err := Parse(encodePacket(RecoverySetID{1}, packetTypeFromString(packetTypeFileDesc), body))
	if err != nil {
		t.Fatalf("Parse UTF-8 filename: %v", err)
	}
	if got := index.Packets[0].FileDescription.Filename; got != filename {
		t.Fatalf("filename = %q, want %q", got, filename)
	}
}

func TestParseRejectsInvalidUTF8Filename(t *testing.T) {
	filename := string([]byte{'b', 'a', 'd', 0xff})
	first16K := Digest{4}
	fileID := computeFileID(first16K, 42, []byte(filename))
	body := encodeFileDescriptionBody(fileID, Digest{}, first16K, 42, filename)
	_, err := Parse(encodePacket(RecoverySetID{1}, packetTypeFromString(packetTypeFileDesc), body))
	if !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("Parse error=%v, want ErrInvalidPacket", err)
	}
}

func TestParseRejectsInvalidAndOversizedLengths(t *testing.T) {
	var header [packetHeaderSize]byte
	copy(header[:8], packetMagic[:])
	binary.LittleEndian.PutUint64(header[8:16], packetHeaderSize-1)
	if _, err := Parse(header[:]); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("short length error=%v", err)
	}

	binary.LittleEndian.PutUint64(header[8:16], 1024)
	_, err := ParseReaderWithOptions(bytes.NewReader(header[:]), ParseOptions{MaxPacketSize: 128})
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("oversized length error=%v", err)
	}
}

func TestParseRejectsMalformedKnownBodies(t *testing.T) {
	setID := RecoverySetID{1}
	for _, tc := range []struct {
		name string
		typ  string
		body []byte
	}{
		{name: "main", typ: packetTypeMain, body: make([]byte, 12)},
		{name: "file description", typ: packetTypeFileDesc, body: make([]byte, 56)},
		{name: "IFSC", typ: packetTypeIFSC, body: make([]byte, 16)},
		{name: "recovery", typ: packetTypeRecvSlic, body: make([]byte, 4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(encodePacket(setID, packetTypeFromString(tc.typ), tc.body))
			if !errors.Is(err, ErrInvalidPacket) {
				t.Fatalf("Parse error=%v, want ErrInvalidPacket", err)
			}
		})
	}
}

func encodePacket(setID RecoverySetID, typ PacketType, body []byte) []byte {
	if len(body)%4 != 0 {
		panic("test packet body must be four-byte aligned")
	}
	packet := make([]byte, packetHeaderSize+len(body))
	copy(packet[:8], packetMagic[:])
	binary.LittleEndian.PutUint64(packet[8:16], uint64(len(packet)))
	hash := computePacketHash(setID, typ, body)
	copy(packet[16:32], hash[:])
	copy(packet[32:48], setID[:])
	copy(packet[48:64], typ[:])
	copy(packet[64:], body)
	return packet
}

func encodeMainBody(sliceSize uint64, recovery, nonRecovery []FileID) []byte {
	body := make([]byte, 12+16*(len(recovery)+len(nonRecovery)))
	binary.LittleEndian.PutUint64(body[:8], sliceSize)
	binary.LittleEndian.PutUint32(body[8:12], uint32(len(recovery)))
	off := 12
	for _, ids := range [][]FileID{recovery, nonRecovery} {
		for _, id := range ids {
			copy(body[off:off+16], id[:])
			off += 16
		}
	}
	return body
}

func encodeFileDescriptionBody(fileID FileID, fileHash, first16K Digest, size uint64, filename string) []byte {
	filenameLength := (len(filename) + 3) &^ 3
	body := make([]byte, 56+filenameLength)
	copy(body[:16], fileID[:])
	copy(body[16:32], fileHash[:])
	copy(body[32:48], first16K[:])
	binary.LittleEndian.PutUint64(body[48:56], size)
	copy(body[56:], filename)
	return body
}

func encodeIFSCBody(fileID FileID, checksums []SliceChecksum) []byte {
	body := make([]byte, 16+20*len(checksums))
	copy(body[:16], fileID[:])
	for i, checksum := range checksums {
		off := 16 + i*20
		copy(body[off:off+16], checksum.MD5[:])
		binary.LittleEndian.PutUint32(body[off+16:off+20], checksum.CRC32)
	}
	return body
}

func splitBytes(data []byte, cuts ...int) [][]byte {
	var out [][]byte
	start := 0
	for _, cut := range cuts {
		if cut <= start || cut >= len(data) {
			continue
		}
		out = append(out, data[start:cut])
		start = cut
	}
	out = append(out, data[start:])
	return out
}
