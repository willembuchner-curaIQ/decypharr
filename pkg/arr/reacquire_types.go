package arr

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

var (
	ErrBindingNotFound   = errors.New("arr binding not found")
	ErrBindingUnsafe     = errors.New("arr binding is not authoritative")
	ErrServiceNotStarted = errors.New("arr service not started")
	ErrServiceClosed     = errors.New("arr service closed")
)

type BindingConfidence string

const (
	BindingConfidenceExactPath       BindingConfidence = "exact_path"
	BindingConfidenceDownloadHistory BindingConfidence = "download_history"
	BindingConfidenceHeuristic       BindingConfidence = "heuristic"
)

type Binding struct {
	ArrName                string            `json:"arrName"`
	ArrType                Type              `json:"arrType"`
	ArrInstanceFingerprint string            `json:"arrInstanceFingerprint,omitempty"`
	EntryID                string            `json:"entryId"`
	EntryName              string            `json:"entryName,omitempty"`
	EntryFileID            string            `json:"entryFileId"`
	EntryFileName          string            `json:"entryFileName,omitempty"`
	DownloadID             string            `json:"downloadId,omitempty"`
	ArrFileID              int               `json:"arrFileId,omitzero"`
	LibraryPath            string            `json:"libraryPath,omitempty"`
	SeriesID               int               `json:"seriesId,omitzero"`
	SeasonNumber           int               `json:"seasonNumber,omitzero"`
	EpisodeIDs             []int             `json:"episodeIds,omitempty"`
	MovieID                int               `json:"movieId,omitzero"`
	Confidence             BindingConfidence `json:"confidence,omitempty"`
	Generation             uint64            `json:"generation,omitzero"`
	UpdatedAt              time.Time         `json:"updatedAt"`
}

func (b Binding) AuthorizesMutation() bool {
	return b.ArrFileID > 0 &&
		b.ArrInstanceFingerprint != "" &&
		b.LibraryPath != "" &&
		(b.ArrType == Sonarr || b.ArrType == Radarr) &&
		(b.Confidence == BindingConfidenceExactPath || b.Confidence == BindingConfidenceDownloadHistory)
}

func (b Binding) validate() error {
	switch {
	case b.ArrName == "":
		return errors.New("arr name is required")
	case b.EntryID == "":
		return errors.New("entry ID is required")
	case b.EntryFileID == "":
		return errors.New("entry file ID is required")
	default:
		return nil
	}
}

func cloneBinding(binding Binding) Binding {
	binding.EpisodeIDs = slices.Clone(binding.EpisodeIDs)
	return binding
}

type ReacquireCause string

const (
	ReacquireCauseStream ReacquireCause = "stream"
	ReacquireCauseRepair ReacquireCause = "repair"
	ReacquireCauseManual ReacquireCause = "manual"
)

func (cause ReacquireCause) valid() bool {
	return cause == ReacquireCauseStream || cause == ReacquireCauseRepair || cause == ReacquireCauseManual
}

type ReacquireStrategy string

const (
	ReacquireStrategyHistoryFailed   ReacquireStrategy = "history_failed"
	ReacquireStrategyInteractiveBest ReacquireStrategy = "interactive_best"
	ReacquireStrategyCommandSearch   ReacquireStrategy = "command_search"
)

func (strategy ReacquireStrategy) normalized() ReacquireStrategy {
	if strategy == "" {
		return ReacquireStrategyHistoryFailed
	}
	return strategy
}

func (strategy ReacquireStrategy) valid() bool {
	strategy = strategy.normalized()
	return strategy == ReacquireStrategyHistoryFailed || strategy == ReacquireStrategyInteractiveBest || strategy == ReacquireStrategyCommandSearch
}

type ReacquireStatus string

const (
	ReacquireStatusQueued             ReacquireStatus = "queued"
	ReacquireStatusResolving          ReacquireStatus = "resolving"
	ReacquireStatusInvalidating       ReacquireStatus = "invalidating"
	ReacquireStatusBlocklisting       ReacquireStatus = "blocklisting"
	ReacquireStatusSearching          ReacquireStatus = "searching"
	ReacquireStatusWaitingForGrab     ReacquireStatus = "waiting_for_grab"
	ReacquireStatusWaitingForDownload ReacquireStatus = "waiting_for_download"
	ReacquireStatusWaitingForImport   ReacquireStatus = "waiting_for_import"
	ReacquireStatusReady              ReacquireStatus = "ready"
	ReacquireStatusFailed             ReacquireStatus = "failed"
	ReacquireStatusCancelled          ReacquireStatus = "cancelled"
)

func (status ReacquireStatus) Terminal() bool {
	return status == ReacquireStatusReady || status == ReacquireStatusFailed || status == ReacquireStatusCancelled
}

func (status ReacquireStatus) valid() bool {
	switch status {
	case ReacquireStatusQueued,
		ReacquireStatusResolving,
		ReacquireStatusInvalidating,
		ReacquireStatusBlocklisting,
		ReacquireStatusSearching,
		ReacquireStatusWaitingForGrab,
		ReacquireStatusWaitingForDownload,
		ReacquireStatusWaitingForImport,
		ReacquireStatusReady,
		ReacquireStatusFailed,
		ReacquireStatusCancelled:
		return true
	default:
		return false
	}
}

type ReacquireRequest struct {
	EntryID  string            `json:"entryId"`
	FileID   string            `json:"fileId"`
	Cause    ReacquireCause    `json:"cause"`
	Strategy ReacquireStrategy `json:"strategy,omitempty"`
}

func (request ReacquireRequest) validate() error {
	switch {
	case request.EntryID == "":
		return errors.New("entry ID is required")
	case request.FileID == "":
		return errors.New("file ID is required")
	case !request.Cause.valid():
		return fmt.Errorf("invalid reacquire cause %q", request.Cause)
	case !request.Strategy.valid():
		return fmt.Errorf("invalid reacquire strategy %q", request.Strategy)
	default:
		return nil
	}
}

type ReacquireJob struct {
	ID                     string              `json:"id"`
	Status                 ReacquireStatus     `json:"status"`
	Cause                  ReacquireCause      `json:"cause"`
	Strategy               ReacquireStrategy   `json:"strategy"`
	ArrName                string              `json:"arrName"`
	ArrType                Type                `json:"arrType"`
	EntryID                string              `json:"entryId"`
	FileID                 string              `json:"fileId"`
	DownloadID             string              `json:"downloadId,omitempty"`
	Bindings               []Binding           `json:"bindings"`
	ReplacementDownloadID  string              `json:"replacementDownloadId,omitempty"`
	ReplacementDownloadIDs []string            `json:"replacementDownloadIds,omitempty"`
	Mutations              []ReacquireMutation `json:"mutations,omitempty"`
	RetryAt                time.Time           `json:"retryAt,omitzero"`
	LastError              string              `json:"lastError,omitempty"`
	CreatedAt              time.Time           `json:"createdAt"`
	UpdatedAt              time.Time           `json:"updatedAt"`
	StartedAt              time.Time           `json:"startedAt,omitzero"`
	CompletedAt            time.Time           `json:"completedAt,omitzero"`
}

func cloneJob(job ReacquireJob) ReacquireJob {
	bindings := job.Bindings
	job.Bindings = make([]Binding, len(bindings))
	for i, binding := range bindings {
		job.Bindings[i] = cloneBinding(binding)
	}
	job.ReplacementDownloadIDs = slices.Clone(job.ReplacementDownloadIDs)
	mutations := job.Mutations
	job.Mutations = make([]ReacquireMutation, len(mutations))
	for i, mutation := range mutations {
		job.Mutations[i] = cloneMutation(mutation)
	}
	return job
}
