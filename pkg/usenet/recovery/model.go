// Package recovery contains the durable, transport-neutral description of
// raw NZB files needed by PAR2 recovery. It intentionally does not depend on
// the logical storage model: PAR2 protects the posted source files, while the
// files exposed by goBlack may be members sliced out of an archive.
package recovery

import "sync"

const ManifestVersion = 1

// RawFileKey is stable within one NZB manifest. Zero is reserved for logical
// segments that predate raw-origin tracking or otherwise have no known origin.
type RawFileKey uint32

// SetID and FileID are the 16-byte identifiers used by PAR2 packets.
type (
	SetID  [16]byte
	FileID [16]byte
)

// LayoutConfidence describes how trustworthy an article's decoded byte range
// is. NZB <segment bytes> values describe posted/yEnc bytes, so they are not an
// exact decoded layout by themselves.
type LayoutConfidence uint8

const (
	LayoutUnknown LayoutConfidence = iota
	LayoutEstimated
	LayoutExact
)

// Manifest preserves every raw <file> from an NZB, including PAR2, ignored,
// and unsupported files. Files remain in source order and use ordinal-derived
// keys so filtering or filename detection cannot change their identity.
type Manifest struct {
	Version uint32    `json:"version"`
	NZBID   string    `json:"nzb_id,omitempty"`
	Name    string    `json:"name,omitempty"`
	Files   []RawFile `json:"files"`

	mu sync.RWMutex
	// articleKeys maps message ID to owning raw-file key. Built lazily by
	// indexArticlesLocked; nil means not built yet.
	articleKeys map[string]RawFileKey
}

// RawFile is one posted file from the NZB rather than one logical file exposed
// by the mount. ActualFilename is learned from yEnc when the subject filename
// is absent or obfuscated.
type RawFile struct {
	Key             RawFileKey `json:"key"`
	Ordinal         int        `json:"ordinal"`
	Subject         string     `json:"subject,omitempty"`
	SubjectFilename string     `json:"subject_filename,omitempty"`
	BaseFilename    string     `json:"base_filename,omitempty"`
	ActualFilename  string     `json:"actual_filename,omitempty"`
	Date            int64      `json:"date,omitempty"`
	Groups          []string   `json:"groups,omitempty"`
	PostedBytes     int64      `json:"posted_bytes,omitempty"`
	TotalSegments   int        `json:"total_segments,omitempty"`
	DetectedType    string     `json:"detected_type,omitempty"`
	IsPAR2          bool       `json:"is_par2,omitempty"`
	Articles        []Article  `json:"articles,omitempty"`
}

// Article is an ordered NNTP article reference for a raw file. DecodedOffset
// and DecodedSize are filled opportunistically from yEnc metadata or a safe
// fixed-part inference; unknown values remain distinguishable via Layout.
type Article struct {
	Number        int              `json:"number"`
	MessageID     string           `json:"message_id,omitempty"`
	PostedBytes   int64            `json:"posted_bytes,omitempty"`
	DecodedOffset int64            `json:"decoded_offset,omitempty"`
	DecodedSize   int64            `json:"decoded_size,omitempty"`
	Layout        LayoutConfidence `json:"layout,omitempty"`
}

func NewManifest(nzbID, name string) *Manifest {
	return &Manifest{Version: ManifestVersion, NZBID: nzbID, Name: name}
}

// SetNZBID applies a caller-supplied NZB ID after parsing.
func (m *Manifest) SetNZBID(id string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.NZBID = id
	m.mu.Unlock()
}

// File returns an owned snapshot of a raw file.
func (m *Manifest) File(key RawFileKey) (RawFile, bool) {
	if m == nil || key == 0 {
		return RawFile{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.Files {
		if m.Files[i].Key == key {
			return cloneRawFile(m.Files[i]), true
		}
	}
	return RawFile{}, false
}

// HasPAR2 reports whether extension or content inspection identified at least
// one parity file in the raw manifest.
func (m *Manifest) HasPAR2() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.Files {
		if m.Files[i].IsPAR2 {
			return true
		}
	}
	return false
}

// UpdateClassification records information learned from an extension, a yEnc
// filename, or content magic without changing raw-file identity.
func (m *Manifest) UpdateClassification(key RawFileKey, detectedType, actualFilename string, isPAR2 bool) {
	if m == nil || key == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.Files {
		if m.Files[i].Key != key {
			continue
		}
		if detectedType != "" {
			m.Files[i].DetectedType = detectedType
		}
		if actualFilename != "" {
			m.Files[i].ActualFilename = actualFilename
		}
		m.Files[i].IsPAR2 = m.Files[i].IsPAR2 || isPAR2
		return
	}
}

// UpdateArticleLayout records the decoded range for a segment number.
func (m *Manifest) UpdateArticleLayout(key RawFileKey, number int, offset, size int64, confidence LayoutConfidence) {
	if m == nil || key == 0 || size <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.Files {
		if m.Files[i].Key != key {
			continue
		}
		for j := range m.Files[i].Articles {
			if m.Files[i].Articles[j].Number != number {
				continue
			}
			article := &m.Files[i].Articles[j]
			if confidence >= article.Layout {
				article.DecodedOffset = offset
				article.DecodedSize = size
				article.Layout = confidence
			}
			return
		}
		return
	}
}

// FindArticleKey resolves a message ID to the key of the raw file that posted
// it. The lookup index is built on first use: message IDs are assigned when the
// manifest is built and never change afterwards, so it needs no invalidation.
func (m *Manifest) FindArticleKey(messageID string) (RawFileKey, bool) {
	if m == nil || messageID == "" {
		return 0, false
	}
	m.mu.RLock()
	if m.articleKeys != nil {
		key, ok := m.articleKeys[messageID]
		m.mu.RUnlock()
		return key, ok
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.articleKeys == nil {
		m.indexArticlesLocked()
	}
	key, ok := m.articleKeys[messageID]
	return key, ok
}

func (m *Manifest) indexArticlesLocked() {
	total := 0
	for i := range m.Files {
		total += len(m.Files[i].Articles)
	}
	// A message ID may be posted under more than one raw file. The scan this
	// index replaced returned the first match in file order, so keep first-wins.
	index := make(map[string]RawFileKey, total)
	for i := range m.Files {
		for j := range m.Files[i].Articles {
			id := m.Files[i].Articles[j].MessageID
			if id == "" {
				continue
			}
			if _, exists := index[id]; !exists {
				index[id] = m.Files[i].Key
			}
		}
	}
	m.articleKeys = index
}

func cloneRawFile(file RawFile) RawFile {
	file.Groups = append([]string(nil), file.Groups...)
	file.Articles = append([]Article(nil), file.Articles...)
	return file
}
