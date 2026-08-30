package manager

import (
	"context"
	"strings"

	"github.com/sirrobot01/decypharr/pkg/arr/reacquire"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// managedCatalogBatch bounds how many entries a full catalog scan holds at once.
const managedCatalogBatch = 256

type managedArrCatalog struct {
	storage *storage.Storage
}

// ListManagedFiles returns the managed files an Arr owns. A non-empty entryID
// reads that one entry: a targeted reindex runs after every completed
// download, so it must never walk the whole library.
func (c managedArrCatalog) ListManagedFiles(ctx context.Context, arrName, entryID string) ([]reacquire.ManagedFile, error) {
	if entryID != "" {
		entry, err := c.storage.Get(entryID)
		if err != nil || entry == nil {
			return nil, nil
		}
		if !ownedByArr(entry, arrName) {
			return nil, nil
		}
		return c.entryFiles(entry)
	}

	files := make([]reacquire.ManagedFile, 0)
	var missingIDs []*storage.Entry
	err := c.storage.ForEachBatch(managedCatalogBatch, func(entries []*storage.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, entry := range entries {
			if !ownedByArr(entry, arrName) {
				continue
			}
			if needsFileIDs(entry) {
				missingIDs = append(missingIDs, entry)
				continue
			}
			files = append(files, entryManagedFiles(entry)...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// File IDs are assigned by a write, which cannot run inside the scan.
	for _, entry := range missingIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entryFiles, err := c.entryFiles(entry)
		if err != nil {
			return nil, err
		}
		files = append(files, entryFiles...)
	}
	return files, nil
}

func (c managedArrCatalog) entryFiles(entry *storage.Entry) ([]reacquire.ManagedFile, error) {
	if needsFileIDs(entry) {
		if err := c.storage.AddOrUpdate(entry); err != nil {
			return nil, err
		}
	}
	return entryManagedFiles(entry), nil
}

func entryManagedFiles(entry *storage.Entry) []reacquire.ManagedFile {
	folder := entry.GetFolder()
	files := make([]reacquire.ManagedFile, 0, len(entry.Files))
	for fileName, file := range entry.Files {
		if file == nil || file.Deleted || file.ID == "" {
			continue
		}
		if file.Name != "" {
			fileName = file.Name
		}
		files = append(files, reacquire.ManagedFile{
			EntryID:     entry.InfoHash,
			EntryName:   entry.Name,
			EntryFolder: folder,
			FileID:      file.ID,
			FileName:    fileName,
			DownloadID:  entry.InfoHash,
		})
	}
	return files
}

func ownedByArr(entry *storage.Entry, arrName string) bool {
	return entry != nil && strings.EqualFold(strings.TrimSpace(entry.Category), strings.TrimSpace(arrName))
}

func needsFileIDs(entry *storage.Entry) bool {
	for _, file := range entry.Files {
		if file != nil && !file.Deleted && file.ID == "" {
			return true
		}
	}
	return false
}
