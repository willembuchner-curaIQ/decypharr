package recovery

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/par2"
)

const maxMappingProbes = 8

var volumeNamePattern = regexp.MustCompile(`(?i)\.vol([0-9]+)\+([0-9]+)\.par2$`)

type fetchedArticle struct {
	body     []byte
	metadata *nntp.YencMetadata
	err      error
}

type repairOperation struct {
	coordinator *Coordinator
	ctx         context.Context
	nzbID       string
	manifest    *Manifest
	meter       *trafficMeter
	local       reader.ArticleRangeSource

	articleCache     map[string]fetchedArticle
	reserved         map[string]bool
	parsedRaw        map[RawFileKey]bool
	aliases          map[FileID][]RawFileKey
	mappingProbe     int
	mappingPreflight bool
	planSealed       bool
}

type flatShard struct {
	index      int
	setID      SetID
	source     StoredSourceFile
	aliases    []RawFileKey
	fileOffset int64
	actual     int64
	sliceIndex int
}

type parityPayload struct {
	exponent uint16
	data     []byte
}

type volumeCandidate struct {
	file       RawFile
	rows       []uint16
	postedSize int64
}

type volumeSelection struct {
	cost      int64
	fileCount int
	rows      int
	node      *volumeSelectionNode
	valid     bool
}

type volumeSelectionNode struct {
	index int
	prev  *volumeSelectionNode
}

func (op *repairOperation) recover(binding targetBinding, segment reader.SegmentMeta) ([]byte, error) {
	if binding.set.SliceSize == 0 || binding.set.SliceSize > math.MaxInt64 {
		return nil, &LayoutError{RawFile: RawFileKey(segment.RawFileKey), Reason: "invalid PAR2 slice size"}
	}
	if uint64(segment.RawOffset) > binding.source.Length || uint64(segment.RawLength) > binding.source.Length-uint64(segment.RawOffset) {
		return nil, &LayoutError{RawFile: RawFileKey(segment.RawFileKey), Offset: segment.RawOffset, Length: segment.RawLength, Reason: "logical range is outside the PAR2 source file"}
	}
	shards, err := flattenShards(binding.set, op)
	if err != nil {
		return nil, err
	}
	missing, ranges, err := targetPlan(shards, binding.source.FileID, segment.RawOffset, segment.RawLength, int64(binding.set.SliceSize))
	if err != nil {
		return nil, err
	}
	missingSet := make(map[int]bool, len(missing))
	for _, shard := range missing {
		missingSet[shard] = true
	}

	// Derive the complete initial network plan from stored Main/FileDesc
	// metadata, reserve it atomically, and only then issue any source or volume
	// BODY. If this fails after first-time discovery, only the bounded base PAR2
	// bootstrap has been downloaded.
	networkRanges := alignRecoveryRanges(ranges, int64(binding.set.SliceSize))
	selectedVolumes, sourceRefs, err := op.remainingPlan(binding.set, shards, missingSet, networkRanges, len(missing))
	if err != nil {
		return nil, err
	}
	volumeRefs := make([]articleRef, 0)
	for _, volume := range selectedVolumes {
		for _, grouped := range op.rawGroup(volume) {
			for _, article := range grouped.Articles {
				volumeRefs = append(volumeRefs, articleRef{raw: grouped.Key, article: article})
			}
		}
	}
	if err := op.reserveArticles(append(sourceRefs, volumeRefs...)); err != nil {
		return nil, err
	}
	op.planSealed = true
	for _, volume := range selectedVolumes {
		if err := op.ingestRaw(volume); err != nil {
			return nil, err
		}
	}
	current, err := op.coordinator.store.GetParsedSet(op.nzbID, binding.set.SetID)
	if err != nil {
		return nil, err
	}
	binding.set = current
	parity, err := op.availableParity(current)
	if err != nil {
		return nil, err
	}

	for {
		needed := len(missingSet)
		if len(parity) < needed {
			return nil, &NoRecoverySetError{NZBID: op.nzbID, RawFile: RawFileKey(segment.RawFileKey)}
		}
		missing = missing[:0]
		for shard := range missingSet {
			missing = append(missing, shard)
		}
		sort.Ints(missing)
		request := par2.PlanRequest{DataShards: len(shards), SliceSize: int64(current.SliceSize), Missing: missing}
		for _, shard := range shards {
			if missingSet[shard.index] {
				continue
			}
			shardCopy := shard
			request.Data = append(request.Data, par2.DataSource{Shard: shard.index, Read: func(ctx context.Context, offset int64, dst []byte) error {
				if err := op.readSourceRange(ctx, shardCopy, offset, dst); err != nil {
					if nntp.IsArticleNotFoundError(err) || nntp.IsYencDecodeError(err) {
						return &sourceShardFailure{shard: shardCopy.index, cause: err}
					}
					return err
				}
				return nil
			}})
		}
		// Begin with the minimum distinct exponent count. Extra verified rows
		// from the same downloaded volume are added only for singular retry or a
		// newly discovered missing source shard.
		candidateCount := needed
		var plan *par2.Plan
		for {
			request.Recovery = request.Recovery[:0]
			for _, recovery := range parity[:candidateCount] {
				recoveryCopy := recovery
				request.Recovery = append(request.Recovery, par2.RecoverySource{Exponent: recovery.exponent, Read: func(_ context.Context, offset int64, dst []byte) error {
					if offset < 0 || offset > int64(len(recoveryCopy.data))-int64(len(dst)) {
						return io.ErrUnexpectedEOF
					}
					copy(dst, recoveryCopy.data[offset:offset+int64(len(dst))])
					return nil
				}})
			}
			plan, err = par2.NewPlan(request)
			if err == nil {
				break
			}
			if errors.Is(err, par2.ErrSingularSelection) && candidateCount < len(parity) {
				candidateCount++
				continue
			}
			return nil, err
		}
		err = plan.Recover(op.ctx, ranges, func(_ context.Context, shardIndex int, coordinate int64, data []byte) error {
			shard := shards[shardIndex]
			if coordinate >= shard.actual {
				return nil
			}
			writeLength := min(int64(len(data)), shard.actual-coordinate)
			if writeLength <= 0 {
				return nil
			}
			patch := bytes.Clone(data[:writeLength])
			if err := op.coordinator.checkStorage(int64(len(patch))); err != nil {
				return err
			}
			if err := op.coordinator.store.PutPatch(op.nzbID, current.SetID, shard.source.FileID, uint64(shard.fileOffset+coordinate), patch); err != nil {
				return err
			}
			op.coordinator.counters.patchBytes.Add(uint64(len(patch)))
			return nil
		}, par2.RecoverOptions{})
		if err == nil {
			break
		}
		var failed *sourceShardFailure
		if !errors.As(err, &failed) || missingSet[failed.shard] {
			return nil, err
		}
		missingSet[failed.shard] = true
		// The initial volume may already contain a spare row. Additional volume
		// downloads are never attempted unless their complete article costs can
		// still be reserved atomically.
		if len(parity) < len(missingSet) {
			if err := op.fetchAdditionalParity(&current, len(missingSet)-len(parity)); err != nil {
				return nil, err
			}
			parity, err = op.availableParity(current)
			if err != nil {
				return nil, err
			}
		}
	}

	data, err := op.coordinator.store.ReadRepairedRange(op.nzbID, current.SetID, binding.source.FileID, uint64(segment.RawOffset), uint64(segment.RawLength))
	if err != nil {
		return nil, err
	}
	return articleShape(segment, data)
}

func flattenShards(set StoredSet, op *repairOperation) ([]flatShard, error) {
	sliceSize := int64(set.SliceSize)
	shards := make([]flatShard, 0)
	for _, source := range set.Files {
		if source.Length > math.MaxInt64 {
			return nil, &LayoutError{RawFile: source.RawFile, Reason: "source file exceeds supported offset range"}
		}
		count := 0
		if source.Length > 0 {
			count = int((source.Length-1)/set.SliceSize + 1)
		}
		if len(source.SliceChecksums) != 0 && len(source.SliceChecksums) != count {
			return nil, &CorruptionError{Kind: "PAR2 IFSC", Cause: fmt.Errorf("file %x has %d checksums for %d slices", source.FileID, len(source.SliceChecksums), count)}
		}
		aliases := op.aliases[source.FileID]
		if len(aliases) == 0 {
			aliases = op.aliasesForStoredSource(source)
			op.aliases[source.FileID] = aliases
		}
		for slice := range count {
			offset := int64(slice) * sliceSize
			actual := min(sliceSize, int64(source.Length)-offset)
			shards = append(shards, flatShard{index: len(shards), setID: set.SetID, source: source, aliases: aliases, fileOffset: offset, actual: actual, sliceIndex: slice})
		}
	}
	if len(shards) == 0 {
		return nil, &NoRecoverySetError{NZBID: op.nzbID}
	}
	return shards, nil
}

func targetPlan(shards []flatShard, target FileID, offset, length, sliceSize int64) ([]int, []par2.ByteRange, error) {
	end := offset + length
	var missing []int
	var ranges []par2.ByteRange
	for _, shard := range shards {
		if shard.source.FileID != target {
			continue
		}
		shardStart, shardEnd := shard.fileOffset, shard.fileOffset+shard.actual
		start := max(offset, shardStart)
		stop := min(end, shardEnd)
		if start >= stop {
			continue
		}
		missing = append(missing, shard.index)
		ranges = append(ranges, par2.ByteRange{Offset: start - shardStart, Length: stop - start})
	}
	if len(missing) == 0 {
		return nil, nil, &LayoutError{Offset: offset, Length: length, Reason: "target range does not intersect its PAR2 source"}
	}
	return missing, mergeByteRanges(ranges, sliceSize), nil
}

func mergeByteRanges(ranges []par2.ByteRange, limit int64) []par2.ByteRange {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Offset < ranges[j].Offset })
	result := make([]par2.ByteRange, 0, len(ranges))
	for _, current := range ranges {
		current.Length = min(current.Length, limit-current.Offset)
		if current.Length <= 0 {
			continue
		}
		if len(result) == 0 {
			result = append(result, current)
			continue
		}
		last := &result[len(result)-1]
		lastEnd := last.Offset + last.Length
		if current.Offset > lastEnd {
			result = append(result, current)
			continue
		}
		last.Length = max(lastEnd, current.Offset+current.Length) - last.Offset
	}
	return result
}

// PAR2 arithmetic reads 16-bit symbols. The solver expands an odd range by
// at most one byte on either edge, so the network preflight must reserve those
// edge bytes too (and possibly the adjacent NNTP article that contains them).
func alignRecoveryRanges(ranges []par2.ByteRange, sliceSize int64) []par2.ByteRange {
	aligned := make([]par2.ByteRange, 0, len(ranges))
	for _, requested := range ranges {
		start := requested.Offset &^ 1
		end := requested.Offset + requested.Length
		if end&1 != 0 {
			end++
		}
		end = min(end, sliceSize)
		if end > start {
			aligned = append(aligned, par2.ByteRange{Offset: start, Length: end - start})
		}
	}
	return mergeByteRanges(aligned, sliceSize)
}

func (op *repairOperation) remainingPlan(set StoredSet, shards []flatShard, missing map[int]bool, ranges []par2.ByteRange, needed int) ([]RawFile, []articleRef, error) {
	refs := make([]articleRef, 0)
	for _, shard := range shards {
		if missing[shard.index] {
			continue
		}
		for _, requested := range ranges {
			if requested.Offset >= shard.actual {
				continue // PAR2 tail padding is local zeroes, not network data.
			}
			length := min(requested.Length, shard.actual-requested.Offset)
			covered, err := op.networkArticleRefsForRange(shard.aliases, shard.fileOffset+requested.Offset, length)
			if err != nil {
				return nil, nil, err
			}
			refs = append(refs, covered...)
		}
	}
	parity, err := op.availableParity(set)
	if err != nil {
		return nil, nil, err
	}
	volumes, err := op.selectVolumes(set, needed-len(parity))
	if err != nil {
		return nil, nil, err
	}
	return volumes, refs, nil
}

func (op *repairOperation) articleRefsForRange(aliases []RawFileKey, offset, length int64) ([]articleRef, error) {
	if length <= 0 {
		return nil, nil
	}
	files := make([]RawFile, 0, len(aliases))
	for _, key := range aliases {
		if file, ok := op.manifest.File(key); ok {
			files = append(files, file)
		}
	}
	end, cursor := offset+length, offset
	refs := make([]articleRef, 0, 2)
	for cursor < end {
		var exact []articleRef
		var estimated []articleRef
		for _, file := range files {
			for _, article := range file.Articles {
				if article.DecodedSize <= 0 || article.DecodedOffset > cursor || article.DecodedOffset+article.DecodedSize <= cursor {
					continue
				}
				ref := articleRef{raw: file.Key, article: article}
				if article.Layout == LayoutExact {
					exact = append(exact, ref)
				} else if article.Layout == LayoutEstimated {
					estimated = append(estimated, ref)
				}
			}
		}
		candidates := exact
		if len(candidates) == 0 {
			candidates = estimated
		}
		if len(candidates) == 0 {
			return nil, &LayoutError{RawFile: firstKey(aliases), Offset: cursor, Length: end - cursor}
		}
		chosen := candidates[0]
		for _, candidate := range candidates[1:] {
			if candidate.article.MessageID != chosen.article.MessageID {
				return nil, &AmbiguousMappingError{Candidates: aliases}
			}
		}
		refs = append(refs, chosen)
		cursor = min(end, chosen.article.DecodedOffset+chosen.article.DecodedSize)
	}
	return refs, nil
}

func (op *repairOperation) networkArticleRefsForRange(aliases []RawFileKey, offset, length int64) ([]articleRef, error) {
	refs, err := op.articleRefsForRange(aliases, offset, length)
	if err != nil || op.local == nil {
		return refs, err
	}
	end, cursor := offset+length, offset
	network := make([]articleRef, 0, len(refs))
	for _, ref := range refs {
		if cursor >= end {
			break
		}
		coveredEnd := min(end, ref.article.DecodedOffset+ref.article.DecodedSize)
		if coveredEnd <= cursor || !op.local.HasArticleRange(uint32(ref.raw), ref.article.MessageID, cursor, coveredEnd-cursor) {
			network = append(network, ref)
		}
		cursor = coveredEnd
	}
	return network, nil
}

func firstKey(keys []RawFileKey) RawFileKey {
	if len(keys) == 0 {
		return 0
	}
	return keys[0]
}

func (op *repairOperation) availableParity(set StoredSet) ([]parityPayload, error) {
	result := make([]parityPayload, 0, len(set.Recovery))
	seen := make(map[uint16]bool)
	for _, descriptor := range set.Recovery {
		if descriptor.Exponent > math.MaxUint16 {
			continue
		}
		exponent := uint16(descriptor.Exponent)
		if seen[exponent] {
			continue
		}
		payload, err := op.coordinator.store.GetRecoverySlice(op.nzbID, set.SetID, descriptor.Exponent)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(payload) != int(set.SliceSize) {
			return nil, &CorruptionError{Kind: "PAR2 recovery payload", Cause: errors.New("slice length mismatch")}
		}
		seen[exponent] = true
		op.parsedRaw[descriptor.RawFile] = true
		result = append(result, parityPayload{exponent: exponent, data: payload})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].exponent < result[j].exponent })
	return result, nil
}

func (op *repairOperation) selectVolumes(set StoredSet, needed int) ([]RawFile, error) {
	if needed <= 0 {
		return nil, nil
	}
	existing := make(map[uint16]bool)
	for _, descriptor := range set.Recovery {
		has, err := op.coordinator.store.HasRecoverySlice(op.nzbID, set.SetID, descriptor.Exponent)
		if err != nil {
			return nil, err
		}
		if has && descriptor.Exponent <= math.MaxUint16 {
			existing[uint16(descriptor.Exponent)] = true
		}
	}
	var candidates []volumeCandidate
	var firstUnbounded RawFileKey
	for _, file := range op.volumePAR2Files() {
		match := volumeNamePattern.FindStringSubmatch(rawFilename(file))
		if len(match) != 3 {
			continue
		}
		start, startErr := strconv.ParseUint(match[1], 10, 16)
		count, countErr := strconv.ParseUint(match[2], 10, 17)
		if startErr != nil || countErr != nil || count == 0 || count > uint64(math.MaxUint16)+1-start {
			continue
		}
		rows := make([]uint16, 0, int(count))
		for exponent := start; exponent < start+count; exponent++ {
			if !existing[uint16(exponent)] {
				rows = append(rows, uint16(exponent))
			}
		}
		if len(rows) == 0 {
			continue
		}
		postedSize, bounded := op.rawGroupPostedBytes(file)
		if !bounded {
			if firstUnbounded == 0 {
				firstUnbounded = file.Key
			}
			continue
		}
		candidates = append(candidates, volumeCandidate{file: file, rows: rows, postedSize: postedSize})
	}
	indexes := minimumVolumeSelection(candidates, needed)
	if len(indexes) == 0 {
		if firstUnbounded != 0 {
			return nil, &UnboundedTrafficError{RawFile: firstUnbounded}
		}
		return nil, &NoRecoverySetError{NZBID: op.nzbID}
	}
	selected := make([]RawFile, len(indexes))
	for i, index := range indexes {
		selected[i] = candidates[index].file
	}
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].Ordinal < selected[j].Ordinal })
	return selected, nil
}

// minimumVolumeSelection minimizes modeled BODY bytes for the standard,
// disjoint .volSTART+COUNT layout. Overlapping/nonstandard row declarations
// use a marginal-cost fallback: weighted set cover is NP-hard in general, but
// the fallback remains bounded and never weakens the traffic preflight.
func minimumVolumeSelection(candidates []volumeCandidate, needed int) []int {
	if needed <= 0 {
		return []int{}
	}
	if len(candidates) == 0 {
		return nil
	}
	if volumeRowsDisjoint(candidates) {
		best := make([]volumeSelection, needed+1)
		best[0] = volumeSelection{valid: true}
		for index, candidate := range candidates {
			for covered := needed - 1; covered >= 0; covered-- {
				current := best[covered]
				if !current.valid {
					continue
				}
				nextCovered := min(needed, covered+len(candidate.rows))
				proposal := volumeSelection{
					cost:      saturatingAddInt64(current.cost, candidate.postedSize),
					fileCount: current.fileCount + 1,
					rows:      current.rows + len(candidate.rows),
					valid:     true,
				}
				if betterVolumeSelection(proposal, best[nextCovered]) {
					proposal.node = &volumeSelectionNode{index: index, prev: current.node}
					best[nextCovered] = proposal
				}
			}
		}
		return volumeSelectionIndexes(best[needed])
	}

	covered := make(map[uint16]bool)
	selected := make([]int, 0)
	used := make([]bool, len(candidates))
	for len(covered) < needed {
		choice, marginal := -1, 0
		for index, candidate := range candidates {
			if used[index] {
				continue
			}
			newRows := 0
			for _, row := range candidate.rows {
				if !covered[row] {
					newRows++
				}
			}
			if newRows == 0 {
				continue
			}
			if choice < 0 || volumeMarginalLess(candidate.postedSize, newRows, candidates[choice].postedSize, marginal) {
				choice, marginal = index, newRows
			}
		}
		if choice < 0 {
			return nil
		}
		used[choice] = true
		selected = append(selected, choice)
		for _, row := range candidates[choice].rows {
			covered[row] = true
		}
	}
	return selected
}

func volumeRowsDisjoint(candidates []volumeCandidate) bool {
	seen := make(map[uint16]bool)
	for _, candidate := range candidates {
		for _, row := range candidate.rows {
			if seen[row] {
				return false
			}
			seen[row] = true
		}
	}
	return true
}

func betterVolumeSelection(candidate, current volumeSelection) bool {
	if !candidate.valid {
		return false
	}
	if !current.valid || candidate.cost != current.cost {
		return !current.valid || candidate.cost < current.cost
	}
	if candidate.fileCount != current.fileCount {
		return candidate.fileCount < current.fileCount
	}
	return candidate.rows < current.rows
}

func volumeSelectionIndexes(selection volumeSelection) []int {
	if !selection.valid || selection.node == nil {
		return nil
	}
	indexes := make([]int, selection.fileCount)
	for i, node := len(indexes)-1, selection.node; i >= 0 && node != nil; i, node = i-1, node.prev {
		indexes[i] = node.index
	}
	return indexes
}

func volumeMarginalLess(leftCost int64, leftRows int, rightCost int64, rightRows int) bool {
	left := float64(leftCost) / float64(leftRows)
	right := float64(rightCost) / float64(rightRows)
	if left != right {
		return left < right
	}
	return leftCost < rightCost
}

func (op *repairOperation) fetchAdditionalParity(set *StoredSet, needed int) error {
	volumes, err := op.selectVolumes(*set, needed)
	if err != nil {
		return err
	}
	refs := make([]articleRef, 0)
	for _, volume := range volumes {
		for _, grouped := range op.rawGroup(volume) {
			for _, article := range grouped.Articles {
				refs = append(refs, articleRef{raw: grouped.Key, article: article})
			}
		}
	}
	if err := op.reserveArticles(refs); err != nil {
		return err
	}
	for _, volume := range volumes {
		if err := op.ingestRaw(volume); err != nil {
			return err
		}
	}
	updated, err := op.coordinator.store.GetParsedSet(op.nzbID, set.SetID)
	if err != nil {
		return err
	}
	*set = updated
	return nil
}

func (op *repairOperation) readSourceRange(ctx context.Context, shard flatShard, coordinate int64, dst []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clear(dst)
	if coordinate >= shard.actual {
		return nil
	}
	length := min(int64(len(dst)), shard.actual-coordinate)
	fileOffset := shard.fileOffset + coordinate
	patched, err := op.coordinator.store.ReadRepairedRange(op.nzbID, shard.setID, shard.source.FileID, uint64(fileOffset), uint64(length))
	if err == nil {
		copy(dst, patched)
		return nil
	}
	if !errors.Is(err, ErrRangeNotCovered) {
		return err
	}
	if err := op.readRawRange(shard, fileOffset, dst[:length]); err != nil {
		return err
	}
	if coordinate == 0 && length == shard.actual && len(shard.source.SliceChecksums) > 0 {
		if shard.sliceIndex < len(shard.source.SliceChecksums) {
			expected := shard.source.SliceChecksums[shard.sliceIndex].MD5
			if actual := md5.Sum(dst[:length]); actual != expected {
				return nntp.NewYencDecodeError(errors.New("PAR2 source-slice checksum mismatch"))
			}
		}
	}
	return nil
}

func (op *repairOperation) readRawRange(shard flatShard, offset int64, dst []byte) error {
	refs, err := op.articleRefsForRange(shard.aliases, offset, int64(len(dst)))
	if err != nil {
		return err
	}
	cursor, end := offset, offset+int64(len(dst))
	for _, ref := range refs {
		if cursor >= end {
			break
		}
		plannedEnd := min(end, ref.article.DecodedOffset+ref.article.DecodedSize)
		if plannedEnd <= cursor {
			return &LayoutError{RawFile: ref.raw, Offset: cursor, Length: end - cursor, Reason: "planned article range does not advance"}
		}
		if op.local != nil {
			local := dst[cursor-offset : plannedEnd-offset]
			present, localErr := op.local.ReadArticleRange(uint32(ref.raw), ref.article.MessageID, cursor, local)
			if localErr != nil {
				return localErr
			}
			if present {
				op.coordinator.counters.localSourceBytes.Add(uint64(len(local)))
				cursor = plannedEnd
				continue
			}
		}
		if op.planSealed && !op.reserved[ref.article.MessageID] {
			return &LayoutError{RawFile: ref.raw, Offset: cursor, Length: end - cursor, Reason: "article was neither locally available nor part of the atomically reserved network plan"}
		}
		body, metadata, err := op.fetchArticle(ref.raw, ref.article)
		if err != nil {
			return err
		}
		if metadata.Size != int64(shard.source.Length) || metadata.Offset > cursor || metadata.Offset+metadata.PartSize <= cursor {
			return &LayoutError{RawFile: ref.raw, Offset: cursor, Length: end - cursor, Reason: "observed yEnc range does not cover its planned source interval"}
		}
		if normalizedFilename(metadata.Name) != normalizedFilename(shard.source.Name) {
			return &AmbiguousMappingError{Filename: shard.source.Name, FileID: shard.source.FileID, Candidates: shard.aliases}
		}
		copyEnd := min(end, metadata.Offset+metadata.PartSize)
		copy(dst[cursor-offset:copyEnd-offset], body[cursor-metadata.Offset:copyEnd-metadata.Offset])
		cursor = copyEnd
	}
	if cursor != end {
		return &LayoutError{RawFile: firstKey(shard.aliases), Offset: cursor, Length: end - cursor}
	}
	return nil
}

type targetBinding struct {
	set    StoredSet
	source StoredSourceFile
}

type sourceShardFailure struct {
	shard int
	cause error
}

func (e *sourceShardFailure) Error() string {
	return fmt.Sprintf("source shard %d unavailable: %v", e.shard, e.cause)
}
func (e *sourceShardFailure) Unwrap() error { return e.cause }

func (c *Coordinator) recoverArticle(ctx context.Context, nzbID string, segment reader.SegmentMeta, local reader.ArticleRangeSource) ([]byte, error) {
	manifest, err := c.manifest(nzbID)
	if err != nil {
		return nil, err
	}
	op := &repairOperation{
		coordinator: c, ctx: ctx, nzbID: nzbID, manifest: manifest,
		meter: newTrafficMeter(c.policy, manifest), articleCache: make(map[string]fetchedArticle),
		reserved: make(map[string]bool), parsedRaw: make(map[RawFileKey]bool), aliases: make(map[FileID][]RawFileKey),
		local: local,
	}
	targetKey := RawFileKey(segment.RawFileKey)
	sets, err := c.store.ListParsedSets(nzbID)
	if err != nil {
		return nil, err
	}
	if binding, ok, bindErr := op.findTarget(sets, targetKey); bindErr != nil {
		return nil, bindErr
	} else if ok {
		if data, patchErr := c.store.ReadRepairedRange(nzbID, binding.set.SetID, binding.source.FileID, uint64(segment.RawOffset), uint64(segment.RawLength)); patchErr == nil {
			c.counters.patchHits.Add(1)
			return articleShape(segment, data)
		} else if !errors.Is(patchErr, ErrRangeNotCovered) {
			return nil, patchErr
		}
		return op.recover(binding, segment)
	}

	// Metadata is fetched in ascending raw size order. Usually the first and
	// smallest plain .par2 is enough; another base is considered only when it
	// belongs to a different recovery set and budget remains.
	var candidateErr error
	for _, raw := range op.basePAR2Files() {
		if err := op.ingestRaw(raw); err != nil {
			if skippableBootstrapError(err) {
				candidateErr = err
				continue
			}
			return nil, err
		}
		sets, err = c.store.ListParsedSets(nzbID)
		if err != nil {
			return nil, err
		}
		binding, ok, bindErr := op.findTarget(sets, targetKey)
		if bindErr != nil {
			return nil, bindErr
		}
		if ok {
			return op.recover(binding, segment)
		}
	}
	noSet := &NoRecoverySetError{NZBID: nzbID, RawFile: targetKey}
	if candidateErr != nil {
		return nil, errors.Join(noSet, candidateErr)
	}
	return nil, noSet
}

func skippableBootstrapError(err error) bool {
	return nntp.IsArticleNotFoundError(err) || nntp.IsYencDecodeError(err) ||
		errors.Is(err, par2.ErrPacketHash) || errors.Is(err, par2.ErrTruncated) ||
		errors.Is(err, par2.ErrInvalidMagic) || errors.Is(err, par2.ErrInvalidLength) ||
		errors.Is(err, par2.ErrPacketTooLarge) || errors.Is(err, par2.ErrInvalidPacket) ||
		errors.Is(err, par2.ErrUnsafePath) || errors.Is(err, ErrNoRecoverySet) ||
		errors.Is(err, ErrLayoutUnavailable) || errors.Is(err, ErrAmbiguousMapping)
}

func articleShape(segment reader.SegmentMeta, data []byte) ([]byte, error) {
	if segment.SegmentDataStart < 0 || segment.SegmentDataStart > int64(maxInt()-len(data)) {
		return nil, &LayoutError{RawFile: RawFileKey(segment.RawFileKey), Offset: segment.RawOffset, Length: segment.RawLength, Reason: "article-shaped recovery buffer exceeds addressable memory"}
	}
	result := make([]byte, int(segment.SegmentDataStart)+len(data))
	copy(result[int(segment.SegmentDataStart):], data)
	return result, nil
}

func (op *repairOperation) basePAR2Files() []RawFile {
	files := op.manifestFiles()
	bases := make([]RawFile, 0)
	volumes := make([]RawFile, 0)
	for _, file := range files {
		name := rawFilename(file)
		if !(file.IsPAR2 || isPAR2Filename(name)) || op.parsedRaw[file.Key] {
			continue
		}
		if volumeNamePattern.MatchString(name) {
			volumes = append(volumes, file)
			continue
		}
		bases = append(bases, file)
	}
	if len(bases) == 0 {
		// Some posters ship only self-contained volume files. The smallest one
		// is a safe bounded bootstrap when no plain index exists.
		bases = volumes
		volumes = nil
	}
	sortRawByPostedBytes(bases)
	sortRawByPostedBytes(volumes)
	return append(bases, volumes...)
}

func sortRawByPostedBytes(files []RawFile) {
	sort.SliceStable(files, func(i, j int) bool {
		left, right := rawPostedBytes(files[i]), rawPostedBytes(files[j])
		if left != right {
			return left < right
		}
		return files[i].Ordinal < files[j].Ordinal
	})
}

func (op *repairOperation) volumePAR2Files() []RawFile {
	files := op.manifestFiles()
	volumes := make([]RawFile, 0)
	for _, file := range files {
		if op.parsedRaw[file.Key] {
			continue
		}
		name := rawFilename(file)
		if (file.IsPAR2 || isPAR2Filename(name)) && volumeNamePattern.MatchString(name) {
			volumes = append(volumes, file)
		}
	}
	return volumes
}

func (op *repairOperation) manifestFiles() []RawFile {
	op.manifest.mu.RLock()
	files := make([]RawFile, len(op.manifest.Files))
	for i := range op.manifest.Files {
		files[i] = cloneRawFile(op.manifest.Files[i])
	}
	op.manifest.mu.RUnlock()
	return files
}

func rawPostedBytes(file RawFile) int64 {
	var total int64
	for _, article := range file.Articles {
		if article.PostedBytes <= 0 || total > math.MaxInt64-article.PostedBytes {
			continue
		}
		total += article.PostedBytes
	}
	if total > 0 {
		return total
	}
	return file.PostedBytes
}

// rawGroupPostedBytes returns the exact manifest-side cost that
// fetchWholeRaw will reserve for a possibly split literal XML file. Unknown
// article sizes deliberately make the group ineligible for automatic repair.
func (op *repairOperation) rawGroupPostedBytes(primary RawFile) (int64, bool) {
	byMessageID := make(map[string]int64)
	var total int64
	for _, file := range op.rawGroup(primary) {
		for _, article := range file.Articles {
			if article.MessageID == "" || article.PostedBytes <= 0 {
				return 0, false
			}
			if prior := byMessageID[article.MessageID]; prior > 0 {
				if article.PostedBytes > prior {
					total = saturatingAddInt64(total, article.PostedBytes-prior)
					byMessageID[article.MessageID] = article.PostedBytes
				}
				continue
			}
			total = saturatingAddInt64(total, article.PostedBytes)
			byMessageID[article.MessageID] = article.PostedBytes
		}
	}
	return total, len(byMessageID) > 0 && total > 0
}

func rawFilename(file RawFile) string {
	for _, candidate := range []string{file.ActualFilename, file.BaseFilename, file.SubjectFilename} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return file.Subject
}

func normalizedFilename(name string) string {
	name = strings.TrimSpace(strings.ReplaceAll(name, "\\", "/"))
	return strings.ToLower(path.Base(name))
}

func (op *repairOperation) findTarget(sets []StoredSet, target RawFileKey) (targetBinding, bool, error) {
	var candidates []targetBinding
	var unsafe *AmbiguousMappingError
	targetFile, targetExists := op.manifest.File(target)
	for _, set := range sets {
		for _, source := range set.Files {
			aliases := op.aliasesForStoredSource(source)
			op.aliases[source.FileID] = aliases
			if slices.Contains(aliases, target) {
				candidates = append(candidates, targetBinding{set: set, source: source})
			} else if targetExists && normalizedFilename(rawFilename(targetFile)) == normalizedFilename(source.Name) {
				unsafe = &AmbiguousMappingError{Filename: source.Name, FileID: source.FileID, Candidates: []RawFileKey{source.RawFile, target}}
			}
		}
	}
	if len(candidates) == 0 {
		if unsafe != nil {
			return targetBinding{}, false, unsafe
		}
		return targetBinding{}, false, nil
	}
	if len(candidates) > 1 {
		// Identical FileIDs duplicated across volumes describe one source. A
		// different set/file identity is not safe to choose heuristically.
		first := candidates[0]
		for _, candidate := range candidates[1:] {
			if candidate.set.SetID != first.set.SetID || candidate.source.FileID != first.source.FileID {
				keys := []RawFileKey{target}
				return targetBinding{}, false, &AmbiguousMappingError{Filename: first.source.Name, FileID: first.source.FileID, Candidates: keys}
			}
		}
	}
	return candidates[0], true, nil
}

func (op *repairOperation) aliasesForStoredSource(source StoredSourceFile) []RawFileKey {
	aliases := []RawFileKey{source.RawFile}
	wanted := normalizedFilename(source.Name)
	if wanted == "" {
		return aliases
	}
	anchor, anchorOK := op.manifest.File(source.RawFile)
	for _, file := range op.manifestFiles() {
		if file.Key == source.RawFile || normalizedFilename(rawFilename(file)) != wanted {
			continue
		}
		// Alias expansion is based only on exact layout evidence. It permits
		// duplicate-subject XML entries split across the same yEnc file while
		// refusing unrelated same-name postings whose ranges overlap.
		samePosting := anchorOK && anchor.Subject != "" && anchor.Subject == file.Subject && rangesWithinFile(anchor, source.Length) && rangesWithinFile(file, source.Length)
		exactContinuation := anchorOK && exactRangesDisjoint(anchor, file, source.Length)
		if samePosting || exactContinuation {
			aliases = append(aliases, file.Key)
		}
	}
	sort.Slice(aliases, func(i, j int) bool { return aliases[i] < aliases[j] })
	return slices.Compact(aliases)
}

func rangesWithinFile(file RawFile, length uint64) bool {
	if length > math.MaxInt64 {
		return false
	}
	seen := false
	for _, article := range file.Articles {
		if article.DecodedSize <= 0 || article.DecodedOffset < 0 || article.DecodedOffset > int64(length)-article.DecodedSize {
			return false
		}
		seen = true
	}
	return seen
}

func exactRangesDisjoint(left, right RawFile, length uint64) bool {
	if !rangesWithinFile(left, length) || !rangesWithinFile(right, length) {
		return false
	}
	leftSeen, rightSeen := false, false
	for _, a := range left.Articles {
		if a.Layout != LayoutExact {
			continue
		}
		leftSeen = true
		for _, b := range right.Articles {
			if b.Layout != LayoutExact {
				continue
			}
			rightSeen = true
			if a.DecodedOffset < b.DecodedOffset+b.DecodedSize && b.DecodedOffset < a.DecodedOffset+a.DecodedSize {
				return false
			}
		}
	}
	return leftSeen && rightSeen
}

func exactRawEvidence(file RawFile, length uint64) bool {
	if length > math.MaxInt64 {
		return false
	}
	for _, article := range file.Articles {
		if article.Layout != LayoutExact || article.DecodedOffset < 0 || article.DecodedSize <= 0 {
			continue
		}
		if article.DecodedOffset <= int64(length) && article.DecodedSize <= int64(length)-article.DecodedOffset {
			return true
		}
	}
	return false
}

type articleRef struct {
	raw     RawFileKey
	article Article
}

func (op *repairOperation) reserveArticles(refs []articleRef) error {
	byMessage := make(map[string]articleRef, len(refs))
	var total int64
	for _, ref := range refs {
		if op.reserved[ref.article.MessageID] || op.articleCache[ref.article.MessageID].body != nil {
			continue
		}
		if ref.article.MessageID == "" || ref.article.PostedBytes <= 0 {
			return &UnboundedTrafficError{RawFile: ref.raw, MessageID: ref.article.MessageID}
		}
		if prior, ok := byMessage[ref.article.MessageID]; ok {
			if ref.article.PostedBytes > prior.article.PostedBytes {
				total += ref.article.PostedBytes - prior.article.PostedBytes
				byMessage[ref.article.MessageID] = ref
			}
			continue
		}
		if total > math.MaxInt64-ref.article.PostedBytes {
			return &BudgetExceededError{Limit: op.meter.limit, Used: op.meter.used, Requested: math.MaxInt64}
		}
		total += ref.article.PostedBytes
		byMessage[ref.article.MessageID] = ref
	}
	if total == 0 {
		return nil
	}
	if err := op.meter.reserveTotal(total); err != nil {
		return err
	}
	for messageID := range byMessage {
		op.reserved[messageID] = true
	}
	op.coordinator.counters.modeledDownloadBytes.Add(uint64(total))
	return nil
}

func (op *repairOperation) fetchArticle(raw RawFileKey, article Article) ([]byte, *nntp.YencMetadata, error) {
	if cached, ok := op.articleCache[article.MessageID]; ok {
		return cached.body, cached.metadata, cached.err
	}
	if !op.reserved[article.MessageID] {
		if err := op.reserveArticles([]articleRef{{raw: raw, article: article}}); err != nil {
			return nil, nil, err
		}
	}
	if op.coordinator.fetch == nil {
		return nil, nil, &ValidationError{Field: "NNTP recovery client", Reason: "no client or fetch function configured"}
	}
	op.coordinator.counters.bodyCalls.Add(1)
	body, metadata, err := op.coordinator.fetch(op.ctx, article.MessageID)
	if err == nil {
		if metadata == nil || metadata.Offset < 0 || metadata.PartSize <= 0 || metadata.PartSize != int64(len(body)) {
			err = &LayoutError{RawFile: raw, Reason: "BODY response has no consistent yEnc range"}
		} else if metadata.Size <= 0 || metadata.Offset > metadata.Size-metadata.PartSize {
			err = &LayoutError{RawFile: raw, Offset: metadata.Offset, Length: metadata.PartSize, Reason: "yEnc range is outside its declared file size"}
		}
	}
	if err == nil {
		body = bytes.Clone(body)
		metadataCopy := *metadata
		metadata = &metadataCopy
		op.manifest.UpdateArticleLayout(raw, article.Number, metadata.Offset, metadata.PartSize, LayoutExact)
		op.manifest.UpdateClassification(raw, detectedType(metadata.Name), metadata.Name, isPAR2Filename(metadata.Name))
		op.coordinator.markManifestDirty(op.nzbID)
	}
	op.articleCache[article.MessageID] = fetchedArticle{body: body, metadata: metadata, err: err}
	return body, metadata, err
}

func (op *repairOperation) rawGroup(primary RawFile) []RawFile {
	name := normalizedFilename(rawFilename(primary))
	group := []RawFile{primary}
	if name == "" {
		return group
	}
	for _, candidate := range op.manifestFiles() {
		if candidate.Key == primary.Key || normalizedFilename(rawFilename(candidate)) != name {
			continue
		}
		if candidate.IsPAR2 || isPAR2Filename(rawFilename(candidate)) {
			group = append(group, candidate)
		}
	}
	sort.SliceStable(group, func(i, j int) bool { return group[i].Ordinal < group[j].Ordinal })
	return group
}

func (op *repairOperation) fetchWholeRaw(primary RawFile) ([]byte, error) {
	group := op.rawGroup(primary)
	refs := make([]articleRef, 0)
	for _, file := range group {
		for _, article := range file.Articles {
			refs = append(refs, articleRef{raw: file.Key, article: article})
		}
	}
	if len(refs) == 0 {
		return nil, &LayoutError{RawFile: primary.Key, Reason: "raw PAR2 file has no articles"}
	}
	// This is the bounded bootstrap preflight: every article needed to assemble
	// the selected base file is reserved before its first BODY.
	if err := op.reserveArticles(refs); err != nil {
		return nil, err
	}
	type region struct {
		start int64
		data  []byte
	}
	regions := make([]region, 0, len(refs))
	var total int64 = -1
	for _, ref := range refs {
		body, metadata, err := op.fetchArticle(ref.raw, ref.article)
		if err != nil {
			return nil, err
		}
		if total < 0 {
			total = metadata.Size
		} else if metadata.Size != total {
			return nil, &AmbiguousMappingError{Filename: metadata.Name, Candidates: rawKeys(group)}
		}
		regions = append(regions, region{start: metadata.Offset, data: body})
	}
	if total <= 0 || total > int64(maxInt()) {
		return nil, &LayoutError{RawFile: primary.Key, Reason: "PAR2 decoded size is invalid or too large"}
	}
	sort.SliceStable(regions, func(i, j int) bool { return regions[i].start < regions[j].start })
	result := make([]byte, int(total))
	var covered int64
	for _, region := range regions {
		end := region.start + int64(len(region.data))
		if region.start > covered {
			return nil, &LayoutError{RawFile: primary.Key, Offset: covered, Length: region.start - covered, Reason: "PAR2 article layout has a gap"}
		}
		if region.start < covered {
			overlap := min(covered-region.start, int64(len(region.data)))
			if !bytes.Equal(result[region.start:region.start+overlap], region.data[:overlap]) {
				return nil, &AmbiguousMappingError{Filename: rawFilename(primary), Candidates: rawKeys(group)}
			}
		}
		copy(result[region.start:end], region.data)
		covered = max(covered, end)
	}
	if covered != total {
		return nil, &LayoutError{RawFile: primary.Key, Offset: covered, Length: total - covered, Reason: "PAR2 article layout is incomplete"}
	}
	return result, nil
}

func rawKeys(files []RawFile) []RawFileKey {
	keys := make([]RawFileKey, len(files))
	for i := range files {
		keys[i] = files[i].Key
	}
	return keys
}

func (op *repairOperation) ingestRaw(raw RawFile) error {
	if op.parsedRaw[raw.Key] {
		return nil
	}
	blob, err := op.fetchWholeRaw(raw)
	if err != nil {
		return err
	}
	index, err := par2.Parse(blob)
	if err != nil {
		return err
	}
	if len(index.Order) == 0 {
		return &NoRecoverySetError{NZBID: op.nzbID, RawFile: raw.Key}
	}
	if err := op.mergeIndex(raw, index); err != nil {
		return err
	}
	op.parsedRaw[raw.Key] = true
	return nil
}

func (op *repairOperation) mergeIndex(raw RawFile, index *par2.Index) error {
	if err := op.preflightIndexMappings(index); err != nil {
		return err
	}
	for _, parsedID := range index.Order {
		parsedSet := index.Sets[parsedID]
		setID := toSetID(parsedID)
		stored, err := op.coordinator.store.GetParsedSet(op.nzbID, setID)
		if errors.Is(err, ErrNotFound) {
			stored = StoredSet{Version: StoredSetVersion, SetID: setID}
		} else if err != nil {
			return err
		}

		if len(parsedSet.MainPackets) > 0 {
			main, err := canonicalMain(parsedSet.MainPackets)
			if err != nil {
				return err
			}
			if stored.SliceSize != 0 && stored.SliceSize != main.SliceSize {
				return &CorruptionError{Kind: "PAR2 main packet", Cause: errors.New("conflicting slice sizes")}
			}
			stored.SliceSize = main.SliceSize
			existing := make(map[FileID]StoredSourceFile, len(stored.Files))
			for _, source := range stored.Files {
				existing[source.FileID] = source
			}
			ordered := make([]StoredSourceFile, 0, len(main.RecoveryFileIDs))
			for _, parsedFileID := range main.RecoveryFileIDs {
				fileID := toFileID(parsedFileID)
				if source, ok := existing[fileID]; ok {
					ordered = append(ordered, source)
					op.aliases[fileID] = op.aliasesForStoredSource(source)
					continue
				}
				descriptions := parsedSet.FileDescriptions[parsedFileID]
				description, err := canonicalDescription(descriptions)
				if err != nil {
					return err
				}
				source, aliases, err := op.mapDescription(description)
				if err != nil {
					return err
				}
				if packets := parsedSet.IFSC[parsedFileID]; len(packets) > 0 {
					checksums, err := canonicalChecksums(packets)
					if err != nil {
						return err
					}
					source.SliceChecksums = checksums
				}
				ordered = append(ordered, source)
				op.aliases[fileID] = aliases
			}
			stored.Files = ordered // Main recovery-file order is authoritative.
		}
		if stored.SliceSize == 0 || len(stored.Files) == 0 {
			// A volume containing only recovery packets is useful only after its
			// base metadata has been parsed.
			continue
		}

		for _, packetIndex := range parsedSet.PacketIndexes {
			packet := index.Packets[packetIndex]
			if packet.RecoverySlice == nil {
				continue
			}
			descriptor := RecoverySliceDescriptor{
				Exponent: uint32(packet.RecoverySlice.Exponent), RawFile: raw.Key,
				PayloadOffset: uint64(packet.Offset) + 68, PayloadLength: uint64(len(packet.RecoverySlice.Data)),
				PacketMD5: toDigest(packet.Hash),
			}
			if err := mergeRecoveryDescriptor(&stored, descriptor); err != nil {
				return err
			}
		}
		if err := op.coordinator.checkStorage(storedSetStorageEstimate(stored, op.aliases)); err != nil {
			return err
		}
		if err := op.coordinator.store.PutParsedSet(op.nzbID, stored); err != nil {
			return err
		}
		for _, packetIndex := range parsedSet.PacketIndexes {
			packet := index.Packets[packetIndex]
			if packet.RecoverySlice == nil {
				continue
			}
			payload := packet.RecoverySlice.Data
			if err := op.coordinator.checkStorage(int64(len(payload))); err != nil {
				return err
			}
			if err := op.coordinator.store.PutRecoverySlice(op.nzbID, setID, uint32(packet.RecoverySlice.Exponent), payload); err != nil {
				return err
			}
			op.coordinator.counters.recoveryPayloadBytes.Add(uint64(len(payload)))
		}
	}
	return nil
}

func (op *repairOperation) preflightIndexMappings(index *par2.Index) error {
	refs := make([]articleRef, 0)
	probedRaw := make(map[RawFileKey]bool)
	for _, parsedID := range index.Order {
		parsedSet := index.Sets[parsedID]
		if len(parsedSet.MainPackets) == 0 {
			continue
		}
		main, err := canonicalMain(parsedSet.MainPackets)
		if err != nil {
			return err
		}
		existing := make(map[FileID]bool)
		stored, err := op.coordinator.store.GetParsedSet(op.nzbID, toSetID(parsedID))
		if err == nil {
			for _, source := range stored.Files {
				existing[source.FileID] = true
			}
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		for _, parsedFileID := range main.RecoveryFileIDs {
			if existing[toFileID(parsedFileID)] {
				continue
			}
			description, err := canonicalDescription(parsedSet.FileDescriptions[parsedFileID])
			if err != nil {
				return err
			}
			planned, candidates, err := op.mappingProbePlan(description)
			if err != nil {
				return err
			}
			refs = append(refs, planned...)
			for _, key := range candidates {
				probedRaw[key] = true
			}
		}
	}
	if op.mappingProbe+len(probedRaw) > maxMappingProbes {
		return &AmbiguousMappingError{Candidates: sortedRawKeySet(probedRaw)}
	}
	if err := op.reserveArticles(refs); err != nil {
		return err
	}
	op.mappingProbe += len(probedRaw)
	op.mappingPreflight = true
	return nil
}

func (op *repairOperation) mappingProbePlan(description par2.FileDescriptionPacket) ([]articleRef, []RawFileKey, error) {
	wanted := normalizedFilename(description.Filename)
	all := op.manifestFiles()
	named := make([]RawFile, 0)
	for _, file := range all {
		if file.IsPAR2 || isPAR2Filename(rawFilename(file)) {
			continue
		}
		if normalizedFilename(rawFilename(file)) == wanted {
			named = append(named, file)
		}
	}
	if len(named) == 1 {
		return nil, nil, nil
	}
	if len(named) > 1 {
		qualified := make([]RawFile, 0, len(named))
		for _, candidate := range named {
			if exactRawEvidence(candidate, description.FileLength) {
				qualified = append(qualified, candidate)
			}
		}
		if len(qualified) == 1 || (len(qualified) > 1 && aliasesHaveDisjointExactRanges(qualified, description.FileLength)) {
			return nil, nil, nil
		}
		refs := make([]articleRef, 0, len(named))
		keys := make([]RawFileKey, 0, len(named))
		for _, candidate := range named {
			if len(candidate.Articles) == 0 {
				continue
			}
			refs = append(refs, articleRef{raw: candidate.Key, article: candidate.Articles[0]})
			keys = append(keys, candidate.Key)
		}
		return refs, keys, nil
	}
	possible := make([]RawFile, 0)
	for _, file := range all {
		if file.IsPAR2 || isPAR2Filename(rawFilename(file)) || len(file.Articles) == 0 {
			continue
		}
		possible = append(possible, file)
	}
	if len(possible) > maxMappingProbes {
		return nil, nil, &AmbiguousMappingError{Filename: description.Filename, FileID: toFileID(description.FileID), Candidates: rawKeys(possible)}
	}
	prefixLength := min(int64(16<<10), int64(description.FileLength))
	refs := make([]articleRef, 0)
	for _, candidate := range possible {
		covered, err := op.articleRefsForRange([]RawFileKey{candidate.Key}, 0, prefixLength)
		if err != nil {
			return nil, nil, err
		}
		refs = append(refs, covered...)
	}
	return refs, rawKeys(possible), nil
}

func sortedRawKeySet(set map[RawFileKey]bool) []RawFileKey {
	keys := make([]RawFileKey, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func canonicalMain(packets []par2.MainPacket) (par2.MainPacket, error) {
	if len(packets) == 0 {
		return par2.MainPacket{}, &CorruptionError{Kind: "PAR2 main packet", Cause: errors.New("missing")}
	}
	first := packets[0]
	for _, packet := range packets[1:] {
		if packet.SliceSize != first.SliceSize || !slices.Equal(packet.RecoveryFileIDs, first.RecoveryFileIDs) || !slices.Equal(packet.NonRecoveryFileIDs, first.NonRecoveryFileIDs) {
			return par2.MainPacket{}, &CorruptionError{Kind: "PAR2 main packet", Cause: errors.New("conflicting duplicates")}
		}
	}
	return first, nil
}

func canonicalDescription(packets []par2.FileDescriptionPacket) (par2.FileDescriptionPacket, error) {
	if len(packets) == 0 {
		return par2.FileDescriptionPacket{}, &CorruptionError{Kind: "PAR2 file description", Cause: errors.New("missing for protected file")}
	}
	first := packets[0]
	for _, packet := range packets[1:] {
		if packet != first {
			return par2.FileDescriptionPacket{}, &CorruptionError{Kind: "PAR2 file description", Cause: errors.New("conflicting duplicates")}
		}
	}
	return first, nil
}

func canonicalChecksums(packets []par2.IFSCPacket) ([]SliceChecksum, error) {
	first := packets[0].Checksums
	for _, packet := range packets[1:] {
		if !slices.Equal(packet.Checksums, first) {
			return nil, &CorruptionError{Kind: "PAR2 IFSC", Cause: errors.New("conflicting duplicates")}
		}
	}
	result := make([]SliceChecksum, len(first))
	for i := range first {
		result[i] = SliceChecksum{MD5: toDigest(first[i].MD5), CRC32: first[i].CRC32}
	}
	return result, nil
}

func mergeRecoveryDescriptor(set *StoredSet, descriptor RecoverySliceDescriptor) error {
	for _, existing := range set.Recovery {
		if existing.Exponent != descriptor.Exponent {
			continue
		}
		if existing.PacketMD5 != descriptor.PacketMD5 || existing.PayloadLength != descriptor.PayloadLength {
			return &CorruptionError{Kind: "PAR2 recovery descriptor", Cause: fmt.Errorf("conflicting exponent %d", descriptor.Exponent)}
		}
		return nil
	}
	set.Recovery = append(set.Recovery, descriptor)
	sort.SliceStable(set.Recovery, func(i, j int) bool { return set.Recovery[i].Exponent < set.Recovery[j].Exponent })
	return nil
}

func toSetID(id par2.RecoverySetID) SetID  { return SetID(id) }
func toFileID(id par2.FileID) FileID       { return FileID(id) }
func toDigest(digest par2.Digest) [16]byte { return [16]byte(digest) }

func (op *repairOperation) mapDescription(description par2.FileDescriptionPacket) (StoredSourceFile, []RawFileKey, error) {
	fileID := toFileID(description.FileID)
	base := StoredSourceFile{
		FileID: fileID, Name: description.Filename, Length: description.FileLength,
		FullMD5: toDigest(description.FileHash), First16KMD5: toDigest(description.First16KHash),
	}
	wanted := normalizedFilename(description.Filename)
	all := op.manifestFiles()
	candidates := make([]RawFile, 0)
	for _, file := range all {
		if file.IsPAR2 || isPAR2Filename(rawFilename(file)) {
			continue
		}
		if normalizedFilename(rawFilename(file)) == wanted {
			candidates = append(candidates, file)
		}
	}
	if len(candidates) == 1 {
		base.RawFile = candidates[0].Key
		return base, []RawFileKey{base.RawFile}, nil
	}
	if len(candidates) > 1 {
		aliases, err := op.disambiguateNamed(description, candidates)
		if err != nil {
			return StoredSourceFile{}, nil, err
		}
		base.RawFile = aliases[0]
		return base, aliases, nil
	}

	// Obfuscated subjects fall back to bounded yEnc metadata and, only when
	// necessary, the FileDesc first-16KiB digest. No filename-only arbitrary
	// selection is made.
	possible := make([]RawFile, 0)
	for _, file := range all {
		if file.IsPAR2 || isPAR2Filename(rawFilename(file)) || len(file.Articles) == 0 {
			continue
		}
		possible = append(possible, file)
	}
	if len(possible) > maxMappingProbes {
		return StoredSourceFile{}, nil, &AmbiguousMappingError{Filename: description.Filename, FileID: fileID, Candidates: rawKeys(possible)}
	}
	prefixLength := min(int64(16<<10), int64(description.FileLength))
	if err := op.reserveMappingPrefixes(possible, prefixLength); err != nil {
		return StoredSourceFile{}, nil, err
	}
	matches := make([]RawFile, 0)
	for _, file := range possible {
		prefix, actualName, actualSize, err := op.probeRawPrefix(file, prefixLength)
		if err != nil {
			continue
		}
		if actualSize != int64(description.FileLength) {
			continue
		}
		if normalizedFilename(actualName) == wanted {
			matches = append(matches, file)
			continue
		}
		if int64(len(prefix)) == prefixLength && md5.Sum(prefix) == base.First16KMD5 {
			matches = append(matches, file)
		}
	}
	if len(matches) == 1 {
		base.RawFile = matches[0].Key
		return base, []RawFileKey{base.RawFile}, nil
	}
	return StoredSourceFile{}, nil, &AmbiguousMappingError{Filename: description.Filename, FileID: fileID, Candidates: rawKeys(matches)}
}

func (op *repairOperation) disambiguateNamed(description par2.FileDescriptionPacket, candidates []RawFile) ([]RawFileKey, error) {
	qualified := make([]RawFile, 0, len(candidates))
	for _, candidate := range candidates {
		if exactRawEvidence(candidate, description.FileLength) {
			qualified = append(qualified, candidate)
		}
	}
	if len(qualified) == 1 {
		return []RawFileKey{qualified[0].Key}, nil
	}
	if len(qualified) > 1 && aliasesHaveDisjointExactRanges(qualified, description.FileLength) {
		keys := rawKeys(qualified)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		return keys, nil
	}
	if len(candidates) > maxMappingProbes {
		return nil, &AmbiguousMappingError{Filename: description.Filename, FileID: toFileID(description.FileID), Candidates: rawKeys(candidates)}
	}
	if err := op.reserveMappingProbes(candidates); err != nil {
		return nil, err
	}
	qualified = qualified[:0]
	for _, candidate := range candidates {
		if len(candidate.Articles) == 0 {
			continue
		}
		_, metadata, err := op.fetchArticle(candidate.Key, candidate.Articles[0])
		if err == nil && metadata.Size == int64(description.FileLength) && normalizedFilename(metadata.Name) == normalizedFilename(description.Filename) {
			qualified = append(qualified, candidate)
		}
	}
	if len(qualified) == 1 {
		return []RawFileKey{qualified[0].Key}, nil
	}
	if len(qualified) > 1 && aliasesHaveDisjointExactRanges(qualified, description.FileLength) {
		keys := rawKeys(qualified)
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		return keys, nil
	}
	return nil, &AmbiguousMappingError{Filename: description.Filename, FileID: toFileID(description.FileID), Candidates: rawKeys(qualified)}
}

func (op *repairOperation) reserveMappingProbes(candidates []RawFile) error {
	if op.mappingPreflight {
		return nil
	}
	refs := make([]articleRef, 0, len(candidates))
	for _, candidate := range candidates {
		if len(candidate.Articles) == 0 {
			continue
		}
		if _, cached := op.articleCache[candidate.Articles[0].MessageID]; cached {
			continue
		}
		refs = append(refs, articleRef{raw: candidate.Key, article: candidate.Articles[0]})
	}
	if op.mappingProbe+len(refs) > maxMappingProbes {
		return &AmbiguousMappingError{Candidates: rawKeys(candidates)}
	}
	// Reserve the complete bounded probe batch before its first BODY. A repair
	// that cannot afford mapping therefore stops after base discovery with zero
	// source/volume traffic.
	if err := op.reserveArticles(refs); err != nil {
		return err
	}
	op.mappingProbe += len(refs)
	return nil
}

func (op *repairOperation) reserveMappingPrefixes(candidates []RawFile, prefixLength int64) error {
	if op.mappingPreflight {
		return nil
	}
	if op.mappingProbe+len(candidates) > maxMappingProbes {
		return &AmbiguousMappingError{Candidates: rawKeys(candidates)}
	}
	refs := make([]articleRef, 0)
	for _, candidate := range candidates {
		covered, err := op.articleRefsForRange([]RawFileKey{candidate.Key}, 0, prefixLength)
		if err != nil {
			return err
		}
		refs = append(refs, covered...)
	}
	if err := op.reserveArticles(refs); err != nil {
		return err
	}
	op.mappingProbe += len(candidates)
	return nil
}

func (op *repairOperation) probeRawPrefix(file RawFile, prefixLength int64) ([]byte, string, int64, error) {
	if prefixLength < 0 || prefixLength > int64(maxInt()) {
		return nil, "", 0, &LayoutError{RawFile: file.Key, Length: prefixLength, Reason: "invalid first-16KiB probe length"}
	}
	refs, err := op.articleRefsForRange([]RawFileKey{file.Key}, 0, prefixLength)
	if err != nil {
		return nil, "", 0, err
	}
	prefix := make([]byte, int(prefixLength))
	var name string
	var total int64 = -1
	cursor := int64(0)
	for _, ref := range refs {
		body, metadata, err := op.fetchArticle(file.Key, ref.article)
		if err != nil {
			return nil, "", 0, err
		}
		if total < 0 {
			total, name = metadata.Size, metadata.Name
		} else if metadata.Size != total || normalizedFilename(metadata.Name) != normalizedFilename(name) {
			return nil, "", 0, &AmbiguousMappingError{Filename: name, Candidates: []RawFileKey{file.Key}}
		}
		if metadata.Offset > cursor || metadata.Offset+metadata.PartSize <= cursor {
			return nil, "", 0, &LayoutError{RawFile: file.Key, Offset: cursor, Length: prefixLength - cursor}
		}
		end := min(prefixLength, metadata.Offset+metadata.PartSize)
		copy(prefix[cursor:end], body[cursor-metadata.Offset:end-metadata.Offset])
		cursor = end
	}
	if cursor != prefixLength {
		return nil, "", 0, &LayoutError{RawFile: file.Key, Offset: cursor, Length: prefixLength - cursor}
	}
	return prefix, name, total, nil
}

func aliasesHaveDisjointExactRanges(files []RawFile, length uint64) bool {
	type interval struct {
		start, end int64
		key        RawFileKey
	}
	var ranges []interval
	for _, file := range files {
		for _, article := range file.Articles {
			if article.Layout != LayoutExact || article.DecodedOffset < 0 || article.DecodedSize <= 0 {
				continue
			}
			end := article.DecodedOffset + article.DecodedSize
			if end > int64(length) {
				return false
			}
			ranges = append(ranges, interval{article.DecodedOffset, end, file.Key})
		}
	}
	if len(ranges) < len(files) {
		return false
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
	for i := 1; i < len(ranges); i++ {
		if ranges[i].key != ranges[i-1].key && ranges[i].start < ranges[i-1].end {
			return false
		}
	}
	return true
}
