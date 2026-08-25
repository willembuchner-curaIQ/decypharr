package recovery

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	maxStoredSetFiles      = 32_768
	maxStoredSetSlices     = 32_768
	maxRecoveryDescriptors = 1_000_000
)

func encodeStoredSet(set StoredSet) ([]byte, error) {
	if err := validateStoredSet(set); err != nil {
		return nil, err
	}
	w := compactWriter{data: make([]byte, 0, 256)}
	w.u64(uint64(set.Version))
	w.fixed(set.SetID[:])
	w.u64(set.SliceSize)
	w.u64(uint64(len(set.Files)))
	for i := range set.Files {
		file := &set.Files[i]
		w.fixed(file.FileID[:])
		w.u64(uint64(file.RawFile))
		w.string(file.Name)
		w.u64(file.Length)
		w.fixed(file.FullMD5[:])
		w.fixed(file.First16KMD5[:])
		w.u64(uint64(len(file.SliceChecksums)))
		for j := range file.SliceChecksums {
			checksum := &file.SliceChecksums[j]
			w.fixed(checksum.MD5[:])
			w.data = binary.BigEndian.AppendUint32(w.data, checksum.CRC32)
		}
	}
	w.u64(uint64(len(set.Recovery)))
	for i := range set.Recovery {
		descriptor := &set.Recovery[i]
		w.u64(uint64(descriptor.Exponent))
		w.u64(uint64(descriptor.RawFile))
		w.u64(descriptor.PayloadOffset)
		w.u64(descriptor.PayloadLength)
		w.fixed(descriptor.PacketMD5[:])
	}
	return w.data, nil
}

func decodeStoredSet(data []byte) (StoredSet, error) {
	r := compactReader{data: data}
	version, err := r.u64("stored set version")
	if err != nil {
		return StoredSet{}, err
	}
	if version != StoredSetVersion {
		return StoredSet{}, &UnsupportedVersionError{Kind: "parsed set", Version: version}
	}
	set := StoredSet{Version: uint32(version)}
	setID, err := r.fixed("set ID", len(set.SetID))
	if err != nil {
		return StoredSet{}, err
	}
	copy(set.SetID[:], setID)
	if set.SliceSize, err = r.u64("slice size"); err != nil {
		return StoredSet{}, err
	}
	fileCount, err := r.boundedCount("source file count", maxStoredSetFiles)
	if err != nil {
		return StoredSet{}, err
	}
	set.Files = make([]StoredSourceFile, fileCount)
	for i := range set.Files {
		file := &set.Files[i]
		fileID, err := r.fixed("file ID", len(file.FileID))
		if err != nil {
			return StoredSet{}, err
		}
		copy(file.FileID[:], fileID)
		rawFile, err := r.u64("source raw file key")
		if err != nil || rawFile > math.MaxUint32 {
			return StoredSet{}, valueError("source raw file key", rawFile, err)
		}
		file.RawFile = RawFileKey(rawFile)
		if file.Name, err = r.string("source filename"); err != nil {
			return StoredSet{}, err
		}
		if file.Length, err = r.u64("source file length"); err != nil {
			return StoredSet{}, err
		}
		fullMD5, err := r.fixed("full-file MD5", len(file.FullMD5))
		if err != nil {
			return StoredSet{}, err
		}
		copy(file.FullMD5[:], fullMD5)
		firstMD5, err := r.fixed("first-16KiB MD5", len(file.First16KMD5))
		if err != nil {
			return StoredSet{}, err
		}
		copy(file.First16KMD5[:], firstMD5)
		checksumCount, err := r.boundedCount("source slice checksum count", maxStoredSetSlices)
		if err != nil {
			return StoredSet{}, err
		}
		file.SliceChecksums = make([]SliceChecksum, checksumCount)
		for j := range file.SliceChecksums {
			checksum := &file.SliceChecksums[j]
			md5Bytes, err := r.fixed("source slice MD5", len(checksum.MD5))
			if err != nil {
				return StoredSet{}, err
			}
			copy(checksum.MD5[:], md5Bytes)
			crcBytes, err := r.fixed("source slice CRC32", 4)
			if err != nil {
				return StoredSet{}, err
			}
			checksum.CRC32 = binary.BigEndian.Uint32(crcBytes)
		}
	}
	recoveryCount, err := r.boundedCount("recovery slice descriptor count", maxRecoveryDescriptors)
	if err != nil {
		return StoredSet{}, err
	}
	set.Recovery = make([]RecoverySliceDescriptor, recoveryCount)
	for i := range set.Recovery {
		descriptor := &set.Recovery[i]
		exponent, err := r.u64("recovery exponent")
		if err != nil || exponent > math.MaxUint32 {
			return StoredSet{}, valueError("recovery exponent", exponent, err)
		}
		descriptor.Exponent = uint32(exponent)
		rawFile, err := r.u64("recovery raw file key")
		if err != nil || rawFile > math.MaxUint32 {
			return StoredSet{}, valueError("recovery raw file key", rawFile, err)
		}
		descriptor.RawFile = RawFileKey(rawFile)
		if descriptor.PayloadOffset, err = r.u64("recovery payload offset"); err != nil {
			return StoredSet{}, err
		}
		if descriptor.PayloadLength, err = r.u64("recovery payload length"); err != nil {
			return StoredSet{}, err
		}
		packetMD5, err := r.fixed("recovery packet MD5", len(descriptor.PacketMD5))
		if err != nil {
			return StoredSet{}, err
		}
		copy(descriptor.PacketMD5[:], packetMD5)
	}
	if err := r.done(); err != nil {
		return StoredSet{}, err
	}
	if err := validateStoredSet(set); err != nil {
		return StoredSet{}, err
	}
	return set, nil
}

func validateStoredSet(set StoredSet) error {
	if set.Version != StoredSetVersion {
		return &UnsupportedVersionError{Kind: "parsed set", Version: uint64(set.Version)}
	}
	if set.SliceSize == 0 || set.SliceSize%4 != 0 {
		return &ValidationError{Field: "slice size", Reason: "must be positive and divisible by four"}
	}
	if len(set.Files) > maxStoredSetFiles {
		return &ValidationError{Field: "files", Reason: fmt.Sprintf("has %d entries; maximum is %d", len(set.Files), maxStoredSetFiles)}
	}
	if len(set.Recovery) > maxRecoveryDescriptors {
		return &ValidationError{Field: "recovery", Reason: fmt.Sprintf("has %d entries; maximum is %d", len(set.Recovery), maxRecoveryDescriptors)}
	}
	fileIDs := make(map[FileID]struct{}, len(set.Files))
	rawFiles := make(map[RawFileKey]struct{}, len(set.Files))
	for i := range set.Files {
		file := &set.Files[i]
		prefix := fmt.Sprintf("files[%d]", i)
		if _, exists := fileIDs[file.FileID]; exists {
			return &ValidationError{Field: prefix + ".file_id", Reason: "is duplicated"}
		}
		fileIDs[file.FileID] = struct{}{}
		if file.RawFile == 0 {
			return &ValidationError{Field: prefix + ".raw_file", Reason: "zero is reserved"}
		}
		if _, exists := rawFiles[file.RawFile]; exists {
			return &ValidationError{Field: prefix + ".raw_file", Reason: "is duplicated"}
		}
		rawFiles[file.RawFile] = struct{}{}
		if err := validateString(prefix+".name", file.Name, true); err != nil {
			return err
		}
		if file.Length > math.MaxInt64 {
			return &ValidationError{Field: prefix + ".length", Reason: "exceeds supported int64 range"}
		}
		if len(file.SliceChecksums) > 0 {
			if len(file.SliceChecksums) > maxStoredSetSlices {
				return &ValidationError{Field: prefix + ".slice_checksums", Reason: fmt.Sprintf("has %d entries; maximum is %d", len(file.SliceChecksums), maxStoredSetSlices)}
			}
			expected := file.Length / set.SliceSize
			if file.Length%set.SliceSize != 0 {
				expected++
			}
			if uint64(len(file.SliceChecksums)) != expected {
				return &ValidationError{Field: prefix + ".slice_checksums", Reason: fmt.Sprintf("got %d, want %d for file length", len(file.SliceChecksums), expected)}
			}
		}
	}
	exponents := make(map[uint32][16]byte, len(set.Recovery))
	for i := range set.Recovery {
		descriptor := &set.Recovery[i]
		prefix := fmt.Sprintf("recovery[%d]", i)
		if descriptor.RawFile == 0 {
			return &ValidationError{Field: prefix + ".raw_file", Reason: "zero is reserved"}
		}
		if descriptor.PayloadLength != set.SliceSize {
			return &ValidationError{Field: prefix + ".payload_length", Reason: fmt.Sprintf("got %d, want slice size %d", descriptor.PayloadLength, set.SliceSize)}
		}
		if descriptor.PayloadOffset > math.MaxInt64 || descriptor.PayloadLength > math.MaxInt64-descriptor.PayloadOffset {
			return &ValidationError{Field: prefix, Reason: "payload range exceeds supported int64 range"}
		}
		if checksum, exists := exponents[descriptor.Exponent]; exists && checksum != descriptor.PacketMD5 {
			return &ValidationError{Field: prefix + ".packet_md5", Reason: "conflicts with another descriptor for the same exponent"}
		}
		exponents[descriptor.Exponent] = descriptor.PacketMD5
	}
	return nil
}
