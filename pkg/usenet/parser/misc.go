package parser

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Tensai75/nzbparser"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// getRARVolumeOrder returns a sort key for RAR volume ordering.
// .rar or .part01.rar = 0 (first volume)
// .r00 = 1, .r01 = 2, etc.; legacy sequences may continue at .s00.
// .part02.rar = 2, .part03.rar = 3, etc.
func getRARVolumeOrder(filename string) int {
	lower := strings.ToLower(filename)
	ext := filepath.Ext(lower)
	base := strings.TrimSuffix(lower, ext)

	// Old-style naming: .rar, .r00, .r01, ...
	if ext == ".rar" {
		// Check for .partXX.rar pattern (new style)
		partPattern := regexp.MustCompile(`\.part(\d+)$`)
		if matches := partPattern.FindStringSubmatch(base); len(matches) == 2 {
			num, _ := strconv.Atoi(matches[1])
			return num // .part01.rar = 1, .part02.rar = 2
		}
		// Plain .rar is the first volume
		return 0
	}

	// .rXX/.rXXX pattern (old style continuation)
	if (len(ext) == 4 || len(ext) == 5) && strings.HasPrefix(ext, ".r") {
		numStr := ext[2:]
		if num, err := strconv.Atoi(numStr); err == nil {
			return num + 1 // .r00 = 1, .r01 = 2, etc.
		}
	}

	// After .r99, classic RAR naming continues with .s00, .t00, ... .z99.
	if len(ext) == 4 && ext[0] == '.' && ext[1] >= 's' && ext[1] <= 'z' {
		if num, err := strconv.Atoi(ext[2:]); err == nil {
			return 101 + int(ext[1]-'s')*100 + num
		}
	}

	// Unknown pattern, put at end
	return 999999
}

// getZIPVolumeOrder puts split volumes (.z01, .z02, ..., .z100) before the
// terminal .zip file. Numeric comparison avoids lexicographic z100/z99 bugs.
func getZIPVolumeOrder(filename string) int {
	lower := strings.ToLower(filename)
	if strings.HasSuffix(lower, ".zip") {
		return int(^uint(0) >> 1)
	}
	ext := filepath.Ext(lower)
	if zipPartPattern.MatchString(ext) {
		if num, err := strconv.Atoi(strings.TrimPrefix(ext, ".z")); err == nil {
			return num
		}
	}
	return int(^uint(0)>>1) - 1
}

func wrapNZBFile(f *storage.NZBFile) ([]*storage.NZBFile, error) {
	if f == nil {
		return nil, fmt.Errorf("nzb file is nil")
	}
	return []*storage.NZBFile{f}, nil
}

// fileMetaKey returns a stable key for associating per-file metadata.
func fileMetaKey(file nzbparser.NzbFile) string {
	if len(file.Segments) > 0 {
		return "m:" + file.Segments[0].Id
	}
	if file.Subject != "" {
		return "s:" + file.Subject
	}
	if file.Number > 0 {
		return fmt.Sprintf("n:%d", file.Number)
	}
	return ""
}

func rawFileIdentity(file nzbparser.NzbFile) string {
	firstMessageID := ""
	if len(file.Segments) > 0 {
		firstMessageID = file.Segments[0].Id
	}
	return file.Subject + "\x00" + firstMessageID
}

func (f *FileGroup) rawFileKey(file nzbparser.NzbFile) recovery.RawFileKey {
	if f == nil {
		return 0
	}
	if len(file.Segments) > 0 && f.rawArticleKeys != nil {
		if key := f.rawArticleKeys[file.Segments[0].Id]; key != 0 {
			return key
		}
	}
	if f.rawFileKeys == nil {
		return 0
	}
	return f.rawFileKeys[rawFileIdentity(file)]
}

func (f *FileGroup) rawFileKeyForSegment(file nzbparser.NzbFile, segment nzbparser.NzbSegment) recovery.RawFileKey {
	if f != nil && f.rawArticleKeys != nil {
		if key := f.rawArticleKeys[segment.Id]; key != 0 {
			return key
		}
	}
	return f.rawFileKey(file)
}

func getGroupsList(groups map[string]struct{}) []string {
	result := make([]string, 0, len(groups))
	for g := range groups {
		result = append(result, g)
	}
	return result
}

func determineNZBName(filename string, meta map[string]string) string {
	// Prefer filename if it exists
	if filename != "" {
		filename = strings.TrimSuffix(filename, filepath.Ext(filename))
	} else if name := meta["Name"]; name != "" {
		filename = name
	} else if title := meta["title"]; title != "" {
		filename = title
	}
	return utils.RemoveInvalidChars(filename)
}

func determineExtension(group *FileGroup) string {
	// Try to determine extension from filenames
	for _, file := range group.Files {
		ext := filepath.Ext(file.Filename)
		if ext != "" {
			return ext
		}
	}
	return ""
}

func getNZBSegments(index int, file nzbparser.NzbFile, group *FileGroup) (int64, []storage.NZBSegment) {
	if len(file.Segments) == 0 {
		return 0, nil
	}

	sort.Slice(file.Segments, func(i, j int) bool {
		return file.Segments[i].Number < file.Segments[j].Number
	})

	// Segment numbers must form one contiguous range (usually 1..N, some
	// posters number from 0). A file with holes or duplicates cannot produce
	// a consistent offset map: the old code zero-filled missing slots, which
	// leaked segments with empty message-ids and offset 0 into the .meta
	// files, the streaming reader (non-monotonic offset table breaks its
	// binary search), and Download. Reject such files outright.
	minSegNum, maxSegNum := file.Segments[0].Number, file.Segments[0].Number
	for _, seg := range file.Segments {
		if seg.Number < minSegNum {
			minSegNum = seg.Number
		}
		if seg.Number > maxSegNum {
			maxSegNum = seg.Number
		}
	}
	if maxSegNum-minSegNum+1 != len(file.Segments) {
		return 0, nil
	}

	nzbSegments := make([]storage.NZBSegment, len(file.Segments))

	currentOffset := int64(0)
	metadata := *group.getMetadata()
	partMeta := group.fileMeta[fileMetaKey(file)]
	if partMeta.fileSize > 0 {
		metadata.fileSize = partMeta.fileSize
	}
	if partMeta.segmentSize > 0 {
		metadata.segmentSize = partMeta.segmentSize
	}

	fileSize := metadata.fileSize
	isLegacyPositionalLast := metadata.lastFileKey == "" && index == len(group.Files)-1
	isRecordedLast := metadata.lastFileKey != "" && metadata.lastFileKey == fileMetaKey(file)
	if partMeta.fileSize <= 0 && (isLegacyPositionalLast || isRecordedLast) {
		fileSize = metadata.lastFileSize
	}
	segmentGroup := ""
	if len(file.Groups) > 0 {
		segmentGroup = file.Groups[0]
	}

	for idx, segment := range file.Segments {
		rawFileKey := group.rawFileKeyForSegment(file, segment)
		// A segment without a message id can never be fetched; it would also
		// defeat the empty-slot duplicate check below.
		if segment.Id == "" {
			return 0, nil
		}
		segSize := metadata.segmentSize
		if segSize <= 0 {
			segSize = int64(float64(segment.Bytes) * 0.97)
		}
		if idx == len(file.Segments)-1 {
			// Last segment may be smaller
			// Last segment calculation
			// Check if the file size metadata assumes a different file (e.g. mixed groups)
			// Expected total size if all segments were full
			fullSegsSize := metadata.segmentSize * int64(len(file.Segments)-1) // size of all previous segments

			// If fileSize is inconsistent with the number of segments (too small or too large),
			// fallback to estimation for this last segment.
			// Threshold: if difference > 1.5 segments
			isSizeMismatch := false
			expectedTotal := fullSegsSize + metadata.segmentSize // rough estimate
			diff := fileSize - expectedTotal
			if diff < 0 {
				diff = -diff
			}
			if diff > (metadata.segmentSize*3)/2 {
				isSizeMismatch = true
			}

			if isSizeMismatch {
				// Fallback: estimate from encoded bytes
				segSize = int64(float64(segment.Bytes) * 0.97)
			} else {
				segSize = fileSize - fullSegsSize
			}
		}
		if segSize <= 0 {
			segSize = int64(float64(segment.Bytes) * 0.97)
		}
		if segSize <= 0 {
			return 0, nil
		}

		rawOffset := currentOffset
		layoutConfidence := recovery.LayoutEstimated
		if partMeta.partNumber > 0 && partMeta.partBegin > 0 && partMeta.segmentSize > 0 {
			delta := int64(segment.Number) - partMeta.partNumber
			candidate := partMeta.partBegin - 1 + delta*partMeta.segmentSize
			if candidate >= 0 {
				rawOffset = candidate
			}
			if partMeta.partNumber == int64(segment.Number) {
				layoutConfidence = recovery.LayoutExact
			}
		}
		seg := storage.NZBSegment{
			Number:      segment.Number,
			MessageID:   segment.Id,
			Bytes:       segSize,
			StartOffset: currentOffset,
			EndOffset:   currentOffset + segSize - 1,
			Group:       segmentGroup,
			RawFileKey:  uint32(rawFileKey),
			RawOffset:   rawOffset,
			RawLength:   segSize,
		}

		// Normalize to the range base so 0- and 1-indexed numbering both map
		// onto a dense array. A duplicate number means the range check above
		// passed on count alone while another slot stays empty — reject.
		segIdx := segment.Number - minSegNum
		if nzbSegments[segIdx].MessageID != "" {
			return 0, nil
		}
		nzbSegments[segIdx] = seg
		group.manifest.UpdateArticleLayout(rawFileKey, segment.Number, rawOffset, segSize, layoutConfidence)
		currentOffset += segSize
	}
	return currentOffset, nzbSegments
}

func buildBaseSegments(group *FileGroup) ([]storage.NZBSegment, []storage.ArchiveVolumeInfo, int64, error) {
	if len(group.Files) == 0 {
		return nil, nil, 0, fmt.Errorf("archive group %s has no raw files", group.BaseName)
	}

	baseSegments := make([]storage.NZBSegment, 0)
	volumeInfos := make([]storage.ArchiveVolumeInfo, 0, len(group.Files))
	currentOffset := int64(0)

	for idx, nzbFile := range group.Files {
		totalSize, segments := getNZBSegments(idx, nzbFile, group)
		if totalSize == 0 || len(segments) == 0 {
			return nil, nil, 0, fmt.Errorf("archive volume %q has incomplete or inconsistent segments", nzbFile.Filename)
		}
		start := len(baseSegments)
		baseSegments = append(baseSegments, segments...)
		volumeInfos = append(volumeInfos, storage.ArchiveVolumeInfo{
			Name:         nzbFile.Filename,
			Size:         totalSize,
			SegmentStart: start,
			SegmentEnd:   len(baseSegments),
		})
		currentOffset += totalSize
	}

	return baseSegments, volumeInfos, currentOffset, nil
}

func buildArchiveVolumeDescriptors(group *FileGroup) ([]*types.Volume, error) {
	var volumes []*types.Volume

	if len(group.Files) == 0 {
		return nil, fmt.Errorf("archive group %s has no raw files", group.BaseName)
	}

	for idx, nzbFile := range group.Files {
		if len(nzbFile.Segments) == 0 {
			return nil, fmt.Errorf("archive volume %q has no segments", nzbFile.Filename)
		}

		totalSize, volumeSegments := getNZBSegments(idx, nzbFile, group)
		if totalSize == 0 || len(volumeSegments) == 0 {
			return nil, fmt.Errorf("archive volume %q has incomplete or inconsistent segments", nzbFile.Filename)
		}

		volumeName := nzbFile.Filename
		if volumeName == "" {
			volumeName = fmt.Sprintf("%s.part%03d", group.BaseName, idx+1)
		}

		volumes = append(volumes, &types.Volume{
			Index:    idx,
			Name:     volumeName,
			Size:     totalSize,
			Segments: volumeSegments,
		})
	}

	return volumes, nil
}

func buildExtractedArchiveFiles(
	group *FileGroup,
	password string,
	fileType storage.NZBFileType,
	baseSegments []storage.NZBSegment,
	volumeInfos []storage.ArchiveVolumeInfo,
	infos []*storage.ExtractedFileInfo,
) ([]*storage.NZBFile, error) {
	if len(baseSegments) == 0 {
		return nil, fmt.Errorf("archive has no base segments")
	}
	files := make([]*storage.NZBFile, 0, len(infos))

	for _, info := range infos {
		if info == nil || info.FileSize <= 0 {
			continue
		}
		if info.InternalPath == "" {
			info.InternalPath = NormalizeArchivePath(info.FileName)
		}
		name := info.FileName
		if name == "" {
			name = group.BaseName
		}
		name = utils.RemoveInvalidChars(name)

		// Use pre-sliced segments if available, otherwise slice based on offset
		var segments []storage.NZBSegment
		if len(info.Segments) > 0 {
			// Use pre-computed segments (from RAR parser, etc.)
			segments = info.Segments
		} else if info.DataOffset > 0 || info.FileSize > 0 {
			// Slice segments for this file's byte range
			sliced, err := sliceSegmentsForRangeSimple(baseSegments, info.DataOffset, info.FileSize)
			if err != nil || len(sliced) == 0 {
				if err == nil {
					err = fmt.Errorf("no source segments overlap the file range")
				}
				return nil, fmt.Errorf("map archived file %q to raw source: %w", info.InternalPath, err)
			} else {
				segments = sliced
			}
		} else {
			return nil, fmt.Errorf("archived file %q has no source offset", info.InternalPath)
		}

		files = append(files, &storage.NZBFile{
			Name:         name,
			InternalPath: info.InternalPath,
			Groups:       getGroupsList(group.Groups),
			Segments:     segments,
			Password:     password,
			FileType:     fileType,
			Size:         info.FileSize,
			IsStored:     info.IsStored,
		})
	}

	return files, nil
}

func NormalizeArchivePath(name string) string {
	trimmed := strings.TrimSpace(name)
	trimmed = strings.TrimLeft(trimmed, "./")
	if trimmed == "" {
		return ""
	}
	trimmed = strings.ReplaceAll(trimmed, "\\", "/")
	trimmed = path.Clean(trimmed)
	trimmed = strings.Trim(trimmed, "/")
	return trimmed
}
