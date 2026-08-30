package repair

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type managedArrFile struct {
	entryName string
	fileName  string
}

type arrFileCollectionStats struct {
	total     int
	resolved  int
	fallback  int
	unmapped  int
	ambiguous int
}

func (s *arrFileCollectionStats) add(other arrFileCollectionStats) {
	s.total += other.total
	s.resolved += other.resolved
	s.fallback += other.fallback
	s.unmapped += other.unmapped
	s.ambiguous += other.ambiguous
}

func (r *Service) enumerateArrCandidates(ctx context.Context, cfg config.RepairConfig) (map[string]*candidate, error) {
	candidates := make(map[string]*candidate)
	var mu sync.Mutex
	var collectionErrors []error

	arrs := r.eligibleArrs(cfg.Arrs)
	if len(arrs) == 0 {
		return candidates, nil
	}

	group, groupCtx := errgroup.WithContext(ctx)
	for _, a := range arrs {
		group.Go(func() error {
			found, err := r.collectArrMediaCandidates(groupCtx, a, "")
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Sweep: GetMedia failed; skipping arr")
				mu.Lock()
				collectionErrors = append(collectionErrors, fmt.Errorf("arr %s: %w", a.Name, err))
				mu.Unlock()
				return nil
			}
			mu.Lock()
			mergeCandidates(candidates, found)
			mu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 && len(collectionErrors) > 0 {
		return nil, errors.Join(collectionErrors...)
	}
	return candidates, nil
}

func (r *Service) collectArrMediaCandidates(ctx context.Context, a *arr.Arr, mediaID string) (map[string]*candidate, error) {
	candidates := make(map[string]*candidate)
	a = r.resolveArrType(ctx, a)
	media, err := a.GetMedia(ctx, mediaID)
	if err != nil {
		return nil, err
	}
	managedFiles := r.managedArrFileIndex(a.Name)
	var stats arrFileCollectionStats
	for _, content := range media {
		select {
		case <-ctx.Done():
			return candidates, ctx.Err()
		default:
		}
		grouped, contentStats := collectArrFiles(content, managedFiles)
		stats.add(contentStats)
		for entryPath, files := range grouped {
			name := filepath.Clean(filepath.Base(entryPath))
			item, err := r.storage.GetEntryItem(name)
			if err != nil || item == nil {
				continue
			}
			found := candidates[name]
			if found == nil {
				found = &candidate{
					name:       name,
					item:       item,
					arrName:    a.Name,
					arrKind:    arrKindFromType(a.Type),
					contentMap: make(map[string]arr.ContentFile),
				}
				candidates[name] = found
			}
			for _, file := range files {
				file.EntryName = name
				found.contentMap[file.TargetPath] = file
			}
		}
	}
	if len(candidates) == 0 && stats.total > 0 {
		return nil, fmt.Errorf(
			"arr returned %d media files but none mapped to managed entries (local symlinks=%d unique size matches=%d unmapped=%d ambiguous=%d); mount the Arr library paths in Decypharr or align the Arr/Decypharr category",
			stats.total, stats.resolved, stats.fallback, stats.unmapped, stats.ambiguous,
		)
	}
	return candidates, nil
}

func (r *Service) managedArrFileIndex(arrName string) map[int64][]managedArrFile {
	index := make(map[int64][]managedArrFile)
	seen := make(map[string]struct{})
	_ = r.storage.ForEach(func(entry *storage.Entry) error {
		if entry == nil || !strings.EqualFold(strings.TrimSpace(entry.Category), strings.TrimSpace(arrName)) {
			return nil
		}
		entryName := entry.GetFolder()
		for fileName, file := range entry.Files {
			if entryName == "" || file == nil || file.Deleted || file.Size <= 0 {
				continue
			}
			key := entryName + "\x00" + fileName
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			index[file.Size] = append(index[file.Size], managedArrFile{entryName: entryName, fileName: fileName})
		}
		return nil
	})
	return index
}

func mergeCandidates(dst, src map[string]*candidate) {
	for name, candidate := range src {
		existing := dst[name]
		if existing == nil {
			dst[name] = candidate
			continue
		}
		if existing.arrName == "" {
			existing.arrName = candidate.arrName
			existing.arrKind = candidate.arrKind
		}
		if existing.contentMap == nil {
			existing.contentMap = make(map[string]arr.ContentFile)
		}
		maps.Copy(existing.contentMap, candidate.contentMap)
	}
}

func (r *Service) eligibleArrs(filter []string) []*arr.Arr {
	wanted := make(map[string]struct{}, len(filter))
	for _, name := range filter {
		if name = strings.TrimSpace(name); name != "" {
			wanted[name] = struct{}{}
		}
	}
	eligible := make([]*arr.Arr, 0)
	for _, a := range r.arrs.GetAll() {
		if a == nil || a.Host == "" || a.Token == "" || a.SkipRepair {
			continue
		}
		if len(wanted) > 0 {
			if _, ok := wanted[a.Name]; !ok {
				continue
			}
		}
		eligible = append(eligible, a)
	}
	return eligible
}

// resolveArrType narrows an instance the name and host could not identify, so
// candidates are classified with the application the instance actually runs.
func (r *Service) resolveArrType(ctx context.Context, a *arr.Arr) *arr.Arr {
	if a == nil || r.arrs == nil || a.Type == arr.Sonarr || a.Type == arr.Radarr {
		return a
	}
	kind, err := a.DetectType(ctx)
	if err != nil {
		r.logger.Debug().Err(err).Str("arr", a.Name).Msg("Sweep: could not detect arr type")
		return a
	}
	if resolved := r.arrs.ResolveType(a.Name, kind); resolved != nil {
		return resolved
	}
	return a
}

func (r *Service) attachArrContext(ctx context.Context, candidate *candidate) {
	for _, a := range r.eligibleArrs(nil) {
		if ctx.Err() != nil {
			return
		}
		a = r.resolveArrType(ctx, a)
		media, err := a.GetMedia(ctx, "")
		if err != nil {
			continue
		}
		managedFiles := r.managedArrFileIndex(a.Name)
		for _, content := range media {
			grouped, _ := collectArrFiles(content, managedFiles)
			for entryPath, files := range grouped {
				if filepath.Clean(filepath.Base(entryPath)) != candidate.name {
					continue
				}
				if candidate.contentMap == nil {
					candidate.contentMap = make(map[string]arr.ContentFile)
				}
				candidate.arrName = a.Name
				candidate.arrKind = arrKindFromType(a.Type)
				for _, file := range files {
					file.EntryName = candidate.name
					candidate.contentMap[file.TargetPath] = file
				}
			}
		}
	}
}

// collectArrFiles uses a readable symlink first, then a unique category/size match.
func collectArrFiles(media arr.Content, managedFiles map[int64][]managedArrFile) (map[string][]arr.ContentFile, arrFileCollectionStats) {
	grouped := make(map[string][]arr.ContentFile)
	var stats arrFileCollectionStats
	for _, file := range media.Files {
		stats.total++
		if target := readSymlinkTarget(file.Path); target != "" {
			file.IsSymlink = true
			dir, name := filepath.Split(target)
			file.TargetPath = name
			entryPath := filepath.Clean(dir)
			grouped[entryPath] = append(grouped[entryPath], file)
			stats.resolved++
			continue
		}

		matches := managedFiles[file.Size]
		if len(matches) != 1 {
			if len(matches) > 1 {
				stats.ambiguous++
			} else {
				stats.unmapped++
			}
			continue
		}
		match := matches[0]
		file.EntryName = match.entryName
		file.TargetPath = match.fileName
		grouped[match.entryName] = append(grouped[match.entryName], file)
		stats.fallback++
	}
	return grouped, stats
}

func readSymlinkTarget(path string) string {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return ""
	}
	target, err := os.Readlink(path)
	if err != nil {
		return ""
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(path), target)
	}
	return target
}

func arrKindFromType(kind arr.Type) storage.ArrKind {
	switch kind {
	case arr.Sonarr:
		return storage.ArrKindSonarr
	case arr.Radarr:
		return storage.ArrKindRadarr
	case arr.Lidarr:
		return storage.ArrKindLidarr
	case arr.Readarr:
		return storage.ArrKindReadarr
	default:
		return storage.ArrKindOther
	}
}
