package recovery

import (
	"fmt"
	"math"
)

const (
	maxManifestFiles    = 100_000
	maxManifestGroups   = 65_536
	maxManifestArticles = 2_000_000
)

func encodeManifest(manifest *Manifest) ([]byte, string, error) {
	if manifest == nil {
		return nil, "", &ValidationError{Field: "manifest", Reason: "must not be nil"}
	}
	manifest.mu.RLock()
	defer manifest.mu.RUnlock()
	if err := validateManifestLocked(manifest); err != nil {
		return nil, "", err
	}

	w := compactWriter{data: make([]byte, 0, 256)}
	w.u64(uint64(manifest.Version))
	w.string(manifest.NZBID)
	w.string(manifest.Name)
	w.u64(uint64(len(manifest.Files)))
	for i := range manifest.Files {
		file := &manifest.Files[i]
		w.u64(uint64(file.Key))
		w.u64(uint64(file.Ordinal))
		w.string(file.Subject)
		w.string(file.SubjectFilename)
		w.string(file.BaseFilename)
		w.string(file.ActualFilename)
		w.i64(file.Date)
		w.u64(uint64(len(file.Groups)))
		for _, group := range file.Groups {
			w.string(group)
		}
		w.u64(uint64(file.PostedBytes))
		w.u64(uint64(file.TotalSegments))
		w.string(file.DetectedType)
		if file.IsPAR2 {
			w.u64(1)
		} else {
			w.u64(0)
		}
		w.u64(uint64(len(file.Articles)))
		for j := range file.Articles {
			article := &file.Articles[j]
			w.u64(uint64(article.Number))
			w.string(article.MessageID)
			w.u64(uint64(article.PostedBytes))
			w.u64(uint64(article.DecodedOffset))
			w.u64(uint64(article.DecodedSize))
			w.u64(uint64(article.Layout))
		}
	}
	return w.data, manifest.NZBID, nil
}

func decodeManifest(data []byte) (*Manifest, error) {
	r := compactReader{data: data}
	version, err := r.u64("manifest version")
	if err != nil {
		return nil, err
	}
	if version != ManifestVersion {
		return nil, &UnsupportedVersionError{Kind: "manifest", Version: version}
	}
	nzbID, err := r.string("NZB ID")
	if err != nil {
		return nil, err
	}
	name, err := r.string("NZB name")
	if err != nil {
		return nil, err
	}
	fileCount, err := r.boundedCount("file count", maxManifestFiles)
	if err != nil {
		return nil, err
	}
	manifest := &Manifest{Version: uint32(version), NZBID: nzbID, Name: name, Files: make([]RawFile, fileCount)}
	totalArticles := 0
	for i := range manifest.Files {
		file := &manifest.Files[i]
		key, err := r.u64("raw file key")
		if err != nil || key > math.MaxUint32 {
			return nil, valueError("raw file key", key, err)
		}
		file.Key = RawFileKey(key)
		ordinal, err := readInt(&r, "raw file ordinal")
		if err != nil {
			return nil, err
		}
		file.Ordinal = ordinal
		if file.Subject, err = r.string("raw file subject"); err != nil {
			return nil, err
		}
		if file.SubjectFilename, err = r.string("raw file subject filename"); err != nil {
			return nil, err
		}
		if file.BaseFilename, err = r.string("raw file base filename"); err != nil {
			return nil, err
		}
		if file.ActualFilename, err = r.string("raw file actual filename"); err != nil {
			return nil, err
		}
		if file.Date, err = r.i64("raw file date"); err != nil {
			return nil, err
		}
		groupCount, err := r.boundedCount("group count", maxManifestGroups)
		if err != nil {
			return nil, err
		}
		file.Groups = make([]string, groupCount)
		for j := range file.Groups {
			if file.Groups[j], err = r.string("group"); err != nil {
				return nil, err
			}
		}
		if file.PostedBytes, err = readInt64(&r, "raw file posted bytes"); err != nil {
			return nil, err
		}
		if file.TotalSegments, err = readInt(&r, "raw file total segments"); err != nil {
			return nil, err
		}
		if file.DetectedType, err = r.string("raw file detected type"); err != nil {
			return nil, err
		}
		isPAR2, err := r.u64("raw file PAR2 flag")
		if err != nil || isPAR2 > 1 {
			return nil, valueError("raw file PAR2 flag", isPAR2, err)
		}
		file.IsPAR2 = isPAR2 == 1
		articleCount, err := r.boundedCount("article count", maxManifestArticles-totalArticles)
		if err != nil {
			return nil, err
		}
		totalArticles += articleCount
		file.Articles = make([]Article, articleCount)
		for j := range file.Articles {
			article := &file.Articles[j]
			if article.Number, err = readInt(&r, "article number"); err != nil {
				return nil, err
			}
			if article.MessageID, err = r.string("article message ID"); err != nil {
				return nil, err
			}
			if article.PostedBytes, err = readInt64(&r, "article posted bytes"); err != nil {
				return nil, err
			}
			if article.DecodedOffset, err = readInt64(&r, "article decoded offset"); err != nil {
				return nil, err
			}
			if article.DecodedSize, err = readInt64(&r, "article decoded size"); err != nil {
				return nil, err
			}
			layout, err := r.u64("article layout confidence")
			if err != nil || layout > math.MaxUint8 {
				return nil, valueError("article layout confidence", layout, err)
			}
			article.Layout = LayoutConfidence(layout)
		}
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	if err := validateManifestLocked(manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func validateManifestLocked(manifest *Manifest) error {
	if manifest.Version != ManifestVersion {
		return &UnsupportedVersionError{Kind: "manifest", Version: uint64(manifest.Version)}
	}
	if err := validateNZBID(manifest.NZBID); err != nil {
		return err
	}
	if err := validateString("NZB name", manifest.Name, false); err != nil {
		return err
	}
	if len(manifest.Files) > maxManifestFiles {
		return &ValidationError{Field: "files", Reason: fmt.Sprintf("has %d entries; maximum is %d", len(manifest.Files), maxManifestFiles)}
	}
	keys := make(map[RawFileKey]struct{}, len(manifest.Files))
	totalArticles := 0
	for i := range manifest.Files {
		file := &manifest.Files[i]
		prefix := fmt.Sprintf("files[%d]", i)
		if file.Key == 0 {
			return &ValidationError{Field: prefix + ".key", Reason: "zero is reserved"}
		}
		if _, exists := keys[file.Key]; exists {
			return &ValidationError{Field: prefix + ".key", Reason: "is duplicated"}
		}
		keys[file.Key] = struct{}{}
		if file.Ordinal < 0 || file.PostedBytes < 0 || file.TotalSegments < 0 {
			return &ValidationError{Field: prefix, Reason: "ordinal and sizes must be non-negative"}
		}
		for field, value := range map[string]string{
			"subject":          file.Subject,
			"subject_filename": file.SubjectFilename,
			"base_filename":    file.BaseFilename,
			"actual_filename":  file.ActualFilename,
			"detected_type":    file.DetectedType,
		} {
			if err := validateString(prefix+"."+field, value, false); err != nil {
				return err
			}
		}
		for j, group := range file.Groups {
			if err := validateString(fmt.Sprintf("%s.groups[%d]", prefix, j), group, false); err != nil {
				return err
			}
		}
		if len(file.Groups) > maxManifestGroups {
			return &ValidationError{Field: prefix + ".groups", Reason: fmt.Sprintf("has %d entries; maximum is %d", len(file.Groups), maxManifestGroups)}
		}
		if len(file.Articles) > maxManifestArticles-totalArticles {
			return &ValidationError{Field: prefix + ".articles", Reason: fmt.Sprintf("manifest exceeds %d total articles", maxManifestArticles)}
		}
		totalArticles += len(file.Articles)
		for j := range file.Articles {
			article := &file.Articles[j]
			articlePrefix := fmt.Sprintf("%s.articles[%d]", prefix, j)
			// Most NZBs number parts from one, but zero-based posts exist and
			// the logical parser intentionally supports them. Negative values
			// remain invalid because the compact codec uses an unsigned column.
			if article.Number < 0 {
				return &ValidationError{Field: articlePrefix + ".number", Reason: "must not be negative"}
			}
			if err := validateString(articlePrefix+".message_id", article.MessageID, true); err != nil {
				return err
			}
			if article.PostedBytes < 0 || article.DecodedOffset < 0 || article.DecodedSize < 0 {
				return &ValidationError{Field: articlePrefix, Reason: "sizes and offsets must be non-negative"}
			}
			if article.Layout > LayoutExact {
				return &ValidationError{Field: articlePrefix + ".layout", Reason: "is unknown"}
			}
			if article.DecodedOffset > math.MaxInt64-article.DecodedSize {
				return &ValidationError{Field: articlePrefix, Reason: "decoded range overflows int64"}
			}
		}
	}
	return nil
}

func readInt(r *compactReader, field string) (int, error) {
	value, err := r.u64(field)
	if err != nil {
		return 0, err
	}
	if value > uint64(maxInt()) {
		return 0, fmt.Errorf("%s: value %d overflows int", field, value)
	}
	return int(value), nil
}

func readInt64(r *compactReader, field string) (int64, error) {
	value, err := r.u64(field)
	if err != nil {
		return 0, err
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%s: value %d overflows int64", field, value)
	}
	return int64(value), nil
}

func valueError(field string, value uint64, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("%s: invalid value %d", field, value)
}
