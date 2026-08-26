package repair

import (
	"context"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (r *Service) repairBroken(ctx context.Context, run *storage.RepairRun, healths *xsync.Map[string, *storage.EntryHealth]) {
	var statsMu sync.Mutex
	healths.Range(func(name string, health *storage.EntryHealth) bool {
		if ctx != nil && ctx.Err() != nil {
			return false
		}
		r.healBrokenEntry(ctx, run, &statsMu, name, health)
		return true
	})
}

func (r *Service) healBrokenEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, health *storage.EntryHealth) {
	if health == nil || health.Status != storage.HealthBroken {
		return
	}

	byArr := make(map[string][]arr.ContentFile)
	for _, broken := range health.BrokenFiles {
		if broken.ArrName == "" || broken.ArrFileID == 0 {
			continue
		}
		byArr[broken.ArrName] = append(byArr[broken.ArrName], arr.ContentFile{
			Id:        broken.MediaID,
			EpisodeId: broken.EpisodeID,
			FileId:    broken.ArrFileID,
			Name:      broken.FileName,
			Path:      broken.SourcePath,
			Size:      broken.Size,
			IsBroken:  true,
		})
	}

	succeeded := make(map[string]struct{}, len(byArr))
	for arrName, files := range byArr {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		a := r.arrs.Get(arrName)
		if a != nil && r.repairArrFiles(ctx, run, statsMu, a, files) {
			succeeded[arrName] = struct{}{}
		}
	}
	if len(byArr) > 0 {
		r.finalizeEntryRepair(name, health, succeeded)
	}
}

func (r *Service) repairArrFiles(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, a *arr.Arr, files []arr.ContentFile) bool {
	historyIDs := make(map[int]struct{})
	needSearch := make([]arr.ContentFile, 0)
	for _, file := range files {
		if ctx != nil && ctx.Err() != nil {
			return false
		}
		mediaID := arrMediaID(a.Type, file)
		if mediaID == 0 {
			needSearch = append(needSearch, file)
			continue
		}
		historyID, _, err := a.FindGrabHistoryID(mediaID)
		if err != nil || historyID == 0 {
			needSearch = append(needSearch, file)
			continue
		}
		historyIDs[historyID] = struct{}{}
	}

	if err := a.DeleteFiles(ctx, files); err != nil {
		r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Repair: DeleteFiles failed")
		statsMu.Lock()
		run.Stats.RepairFailed += len(files)
		r.saveRun(run)
		statsMu.Unlock()
		return false
	}

	for historyID := range historyIDs {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if err := a.MarkHistoryFailed(historyID); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Int("history_id", historyID).Msg("Repair: MarkHistoryFailed failed")
		}
	}
	if len(needSearch) > 0 {
		if err := a.SearchMissing(ctx, needSearch); err != nil {
			r.logger.Warn().Err(err).Str("arr", a.Name).Msg("Repair: SearchMissing fallback failed")
		}
	}

	statsMu.Lock()
	run.Stats.Repaired += len(files)
	r.saveRun(run)
	statsMu.Unlock()
	return true
}

func (r *Service) finalizeEntryRepair(name string, health *storage.EntryHealth, succeeded map[string]struct{}) {
	now := time.Now()
	shouldDelete := health.BrokenCount > 0 && health.BrokenCount == health.FileCount
	hashes := make(map[string]struct{})
	if shouldDelete {
		for _, broken := range health.BrokenFiles {
			if broken.ArrName == "" || broken.ArrFileID == 0 {
				shouldDelete = false
				break
			}
			if _, ok := succeeded[broken.ArrName]; !ok {
				shouldDelete = false
				break
			}
			if broken.InfoHash != "" {
				hashes[broken.InfoHash] = struct{}{}
			}
		}
		shouldDelete = shouldDelete && len(hashes) > 0
	}
	if !shouldDelete {
		health.LastRepairAt = now
		r.saveHealth(health)
		return
	}

	for hash := range hashes {
		if err := r.backend.DeleteEntry(hash, true); err != nil {
			r.logger.Warn().Err(err).Str("entry", name).Str("infohash", hash).Msg("Repair: failed to delete fully-broken entry after re-search")
			continue
		}
		r.logger.Info().Str("entry", name).Str("infohash", hash).Msg("Repair: deleted fully-broken entry after re-search")
	}
}
