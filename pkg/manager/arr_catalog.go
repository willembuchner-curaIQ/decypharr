package manager

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type managedArrCatalog struct {
	storage *storage.Storage
}

func (c managedArrCatalog) ListManagedFiles(ctx context.Context, arrName string) ([]arr.ManagedFile, error) {
	entries, err := c.storage.List(func(entry *storage.Entry) bool {
		return entry != nil && strings.EqualFold(strings.TrimSpace(entry.Category), strings.TrimSpace(arrName))
	})
	if err != nil {
		return nil, err
	}

	files := make([]arr.ManagedFile, 0)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if needsFileIDs(entry) {
			if err := c.storage.AddOrUpdate(entry); err != nil {
				return nil, err
			}
		}

		folder := entry.GetFolder()
		for fileName, file := range entry.Files {
			if file == nil || file.Deleted || file.ID == "" {
				continue
			}
			if file.Name != "" {
				fileName = file.Name
			}
			files = append(files, arr.ManagedFile{
				ArrName:     arrName,
				EntryID:     entry.InfoHash,
				EntryName:   entry.Name,
				EntryFolder: folder,
				FileID:      file.ID,
				FileName:    fileName,
				DownloadID:  entry.InfoHash,
				Path:        filepath.Join(entry.DownloadPath(), fileName),
				Size:        file.Size,
			})
		}
	}
	return files, nil
}

func needsFileIDs(entry *storage.Entry) bool {
	for _, file := range entry.Files {
		if file != nil && !file.Deleted && file.ID == "" {
			return true
		}
	}
	return false
}
