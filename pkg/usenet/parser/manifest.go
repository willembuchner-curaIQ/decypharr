package parser

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"sort"

	"github.com/Tensai75/nzbparser"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
	"golang.org/x/net/html/charset"
)

// rawNZBEnvelope intentionally models only the XML elements needed by the
// recovery manifest. nzbparser.Parse calls MakeUnique before returning, which
// merges <file> elements that share a subject. Decoding this small envelope
// first is what lets the manifest retain every literal source entry.
type rawNZBEnvelope struct {
	Files nzbparser.NzbFiles `xml:"file"`
}

func parseRawNZBFiles(content []byte) (nzbparser.NzbFiles, error) {
	var envelope rawNZBEnvelope
	decoder := xml.NewDecoder(bytes.NewReader(content))
	decoder.CharsetReader = charset.NewReaderLabel
	decoder.Strict = false
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode raw NZB file manifest: %w", err)
	}

	// Populate subject-derived filenames and segment totals without invoking
	// MakeUnique or sorting the files, so Ordinal remains XML document order.
	nzb := &nzbparser.Nzb{Files: envelope.Files}
	nzbparser.ScanNzbFile(nzb)
	return nzb.Files, nil
}

func buildRawManifest(
	nzbID string,
	name string,
	files nzbparser.NzbFiles,
	detect func(string) storage.NZBFileType,
) *recovery.Manifest {
	manifest := recovery.NewManifest(nzbID, name)
	manifest.Files = make([]recovery.RawFile, 0, len(files))

	for ordinal := range files {
		file := files[ordinal]
		key := recovery.RawFileKey(ordinal + 1)
		fileType := detect(file.Filename)
		articles := make([]recovery.Article, 0, len(file.Segments))
		var postedBytes int64
		for _, segment := range file.Segments {
			posted := int64(segment.Bytes)
			postedBytes += posted
			articles = append(articles, recovery.Article{
				Number:      segment.Number,
				MessageID:   segment.Id,
				PostedBytes: posted,
			})
		}
		sort.SliceStable(articles, func(i, j int) bool {
			return articles[i].Number < articles[j].Number
		})

		manifest.Files = append(manifest.Files, recovery.RawFile{
			Key:             key,
			Ordinal:         ordinal,
			Subject:         file.Subject,
			SubjectFilename: file.Filename,
			BaseFilename:    file.Basefilename,
			Date:            int64(file.Date),
			Groups:          append([]string(nil), file.Groups...),
			PostedBytes:     postedBytes,
			TotalSegments:   file.TotalSegments,
			DetectedType:    string(fileType),
			IsPAR2:          fileType == storage.NZBFileTypePar2,
			Articles:        articles,
		})
	}
	return manifest
}

// rawFileKeyFor resolves a copied nzbparser.NzbFile back to the stable
// ordinal-derived manifest key. Subject plus first message ID avoids relying
// on the mutable file Number field used for archive ordering.
func rawFileKeyFor(manifest *recovery.Manifest, file nzbparser.NzbFile) recovery.RawFileKey {
	if manifest == nil {
		return 0
	}
	firstMessageID := ""
	if len(file.Segments) > 0 {
		firstMessageID = file.Segments[0].Id
	}
	for i := range manifest.Files {
		raw := &manifest.Files[i]
		if raw.Subject != file.Subject {
			continue
		}
		if firstMessageID == "" {
			if len(raw.Articles) == 0 {
				return raw.Key
			}
			continue
		}
		for _, article := range raw.Articles {
			if article.MessageID == firstMessageID {
				return raw.Key
			}
		}
	}
	return 0
}

func rawFileKeysFor(manifest *recovery.Manifest, file nzbparser.NzbFile) []recovery.RawFileKey {
	if manifest == nil {
		return nil
	}
	seen := make(map[recovery.RawFileKey]struct{})
	keys := make([]recovery.RawFileKey, 0, 1)
	for _, segment := range file.Segments {
		raw, _, ok := manifest.FindArticle(segment.Id)
		if !ok {
			continue
		}
		if _, exists := seen[raw.Key]; exists {
			continue
		}
		seen[raw.Key] = struct{}{}
		keys = append(keys, raw.Key)
	}
	return keys
}

func updateManifestClassificationForFile(
	manifest *recovery.Manifest,
	file nzbparser.NzbFile,
	detectedType storage.NZBFileType,
	actualFilename string,
) {
	detectedTypeName := string(detectedType)
	if detectedType == storage.NZBFileTypeUnknown {
		// A yEnc filename without a recognizable extension still improves the
		// manifest, but must not erase a stronger subject/content classification.
		detectedTypeName = ""
	}
	keys := rawFileKeysFor(manifest, file)
	if len(keys) == 0 {
		keys = append(keys, rawFileKeyFor(manifest, file))
	}
	for _, key := range keys {
		manifest.UpdateClassification(key, detectedTypeName, actualFilename, detectedType == storage.NZBFileTypePar2)
	}
}

// ManifestFromGroups returns the shared recovery manifest retained by groups
// produced from one Parse call. It lets the application persist yEnc layout
// and filename enrichment learned during Process without exposing mutable
// FileGroup internals.
func ManifestFromGroups(groups map[string]*FileGroup) *recovery.Manifest {
	for _, group := range groups {
		if group != nil && group.manifest != nil {
			return group.manifest
		}
	}
	return nil
}
