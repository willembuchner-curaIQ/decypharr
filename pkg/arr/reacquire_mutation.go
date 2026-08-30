package arr

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	mutationVisibilityDelay = 5 * time.Second
	mutationClockSkew       = 2 * time.Second
	maxMutationAttempts     = 3
)

var errMutationOutcomeUnknown = errors.New("arr mutation outcome is unknown")

type ReacquireMutationKind string

const (
	ReacquireMutationHistoryFailed ReacquireMutationKind = "history_failed"
	ReacquireMutationEpisodeSearch ReacquireMutationKind = "episode_search"
	ReacquireMutationSeasonSearch  ReacquireMutationKind = "season_search"
	ReacquireMutationMovieSearch   ReacquireMutationKind = "movie_search"
	ReacquireMutationReleaseGrab   ReacquireMutationKind = "release_grab"
)

func (kind ReacquireMutationKind) valid() bool {
	switch kind {
	case ReacquireMutationHistoryFailed,
		ReacquireMutationEpisodeSearch,
		ReacquireMutationSeasonSearch,
		ReacquireMutationMovieSearch,
		ReacquireMutationReleaseGrab:
		return true
	default:
		return false
	}
}

type ReacquireMutationState string

const (
	ReacquireMutationIntent    ReacquireMutationState = "intent"
	ReacquireMutationConfirmed ReacquireMutationState = "confirmed"
)

func (state ReacquireMutationState) valid() bool {
	return state == ReacquireMutationIntent || state == ReacquireMutationConfirmed
}

type ReacquireMutation struct {
	Key              string                 `json:"key"`
	Kind             ReacquireMutationKind  `json:"kind"`
	State            ReacquireMutationState `json:"state"`
	CommandName      string                 `json:"commandName,omitempty"`
	DownloadID       string                 `json:"downloadId,omitempty"`
	HistoryID        int                    `json:"historyId,omitzero"`
	EpisodeIDs       []int                  `json:"episodeIds,omitempty"`
	SeriesID         int                    `json:"seriesId,omitzero"`
	SeasonNumber     int                    `json:"seasonNumber,omitzero"`
	MovieIDs         []int                  `json:"movieIds,omitempty"`
	ReleaseGUID      string                 `json:"releaseGuid,omitempty"`
	ReleaseIndexer   string                 `json:"releaseIndexer,omitempty"`
	ReleaseIndexerID int                    `json:"releaseIndexerId,omitzero"`
	IntentAt         time.Time              `json:"intentAt"`
	LastDispatchedAt time.Time              `json:"lastDispatchedAt,omitzero"`
	Attempts         int                    `json:"attempts,omitzero"`
	ReceiptID        int                    `json:"receiptId,omitzero"`
	ConfirmedAt      time.Time              `json:"confirmedAt,omitzero"`
}

func (mutation ReacquireMutation) validate() error {
	switch {
	case mutation.Key == "":
		return errors.New("mutation key is required")
	case !mutation.Kind.valid():
		return fmt.Errorf("invalid mutation kind %q", mutation.Kind)
	case !mutation.State.valid():
		return fmt.Errorf("invalid mutation state %q", mutation.State)
	case mutation.IntentAt.IsZero():
		return errors.New("mutation intent timestamp is required")
	case mutation.Attempts < 0:
		return errors.New("mutation attempts cannot be negative")
	case mutation.Attempts > 0 && mutation.LastDispatchedAt.IsZero():
		return errors.New("attempted mutation requires a dispatch timestamp")
	case mutation.Attempts == 0 && !mutation.LastDispatchedAt.IsZero():
		return errors.New("unattempted mutation cannot have a dispatch timestamp")
	case mutation.State == ReacquireMutationConfirmed && mutation.ConfirmedAt.IsZero():
		return errors.New("confirmed mutation timestamp is required")
	}
	switch mutation.Kind {
	case ReacquireMutationHistoryFailed:
		if mutation.DownloadID == "" {
			return errors.New("failed-history mutation requires a download ID")
		}
	case ReacquireMutationEpisodeSearch:
		if mutation.CommandName == "" || !validMutationIDs(mutation.EpisodeIDs) {
			return errors.New("episode-search mutation requires a command and episode IDs")
		}
	case ReacquireMutationSeasonSearch:
		if mutation.CommandName == "" || mutation.SeriesID <= 0 || mutation.SeasonNumber < 0 {
			return errors.New("season-search mutation requires an exact season scope")
		}
	case ReacquireMutationMovieSearch:
		if mutation.CommandName == "" || !validMutationIDs(mutation.MovieIDs) {
			return errors.New("movie-search mutation requires a command and movie IDs")
		}
	case ReacquireMutationReleaseGrab:
		if mutation.ReleaseGUID == "" || mutation.ReleaseIndexer == "" || mutation.ReleaseIndexerID <= 0 {
			return errors.New("release-grab mutation requires a GUID and indexer identity")
		}
		movieScope := validMutationIDs(mutation.MovieIDs) && len(mutation.EpisodeIDs) == 0
		episodeScope := mutation.SeriesID > 0 && validMutationIDs(mutation.EpisodeIDs) && len(mutation.MovieIDs) == 0
		if !movieScope && !episodeScope {
			return errors.New("release-grab mutation requires one exact media scope")
		}
	}
	return nil
}

func validMutationIDs(ids []int) bool {
	if len(ids) == 0 {
		return false
	}
	for i, id := range ids {
		if id <= 0 || slices.Contains(ids[:i], id) {
			return false
		}
	}
	return true
}

func cloneMutation(mutation ReacquireMutation) ReacquireMutation {
	mutation.EpisodeIDs = slices.Clone(mutation.EpisodeIDs)
	mutation.MovieIDs = slices.Clone(mutation.MovieIDs)
	return mutation
}

type mutationOutcomeUnknownError struct {
	cause      error
	retryAfter time.Duration
}

func (err *mutationOutcomeUnknownError) Error() string {
	return fmt.Sprintf("%s: %v", errMutationOutcomeUnknown, err.cause)
}

func (err *mutationOutcomeUnknownError) Unwrap() error {
	return errors.Join(errMutationOutcomeUnknown, err.cause)
}

func unknownMutationOutcome(cause error, retryAfter time.Duration) error {
	if cause == nil {
		cause = errors.New("remote mutation was not visible during reconciliation")
	}
	return &mutationOutcomeUnknownError{cause: cause, retryAfter: retryAfter}
}

func mutationRetryAfter(err error) time.Duration {
	unknown, ok := errors.AsType[*mutationOutcomeUnknownError](err)
	if !ok {
		return 0
	}
	return unknown.retryAfter
}

func mutationForKey(job ReacquireJob, key string) (ReacquireMutation, bool) {
	for _, mutation := range job.Mutations {
		if mutation.Key == key {
			return cloneMutation(mutation), true
		}
	}
	return ReacquireMutation{}, false
}

func mutationOfKind(job ReacquireJob, kind ReacquireMutationKind) (ReacquireMutation, bool) {
	for _, mutation := range job.Mutations {
		if mutation.Kind == kind {
			return cloneMutation(mutation), true
		}
	}
	return ReacquireMutation{}, false
}

func setMutation(job *ReacquireJob, mutation ReacquireMutation) {
	for i := range job.Mutations {
		if job.Mutations[i].Key == mutation.Key {
			job.Mutations[i] = cloneMutation(mutation)
			return
		}
	}
	job.Mutations = append(job.Mutations, cloneMutation(mutation))
}

func persistMutation(job *ReacquireJob, progress JobProgress, status ReacquireStatus, mutation ReacquireMutation) error {
	if err := progress.UpdateDurable(status, func(current *ReacquireJob) {
		setMutation(current, mutation)
	}); err != nil {
		return err
	}
	setMutation(job, mutation)
	return nil
}

func ensureMutationIntent(job *ReacquireJob, progress JobProgress, status ReacquireStatus, mutation ReacquireMutation) (ReacquireMutation, error) {
	if existing, ok := mutationForKey(*job, mutation.Key); ok {
		return existing, nil
	}
	mutation.State = ReacquireMutationIntent
	mutation.IntentAt = time.Now().UTC()
	if err := persistMutation(job, progress, status, mutation); err != nil {
		return ReacquireMutation{}, err
	}
	return mutation, nil
}

func recordMutationAttempt(job *ReacquireJob, progress JobProgress, status ReacquireStatus, mutation ReacquireMutation) (ReacquireMutation, error) {
	mutation.State = ReacquireMutationIntent
	mutation.Attempts++
	mutation.LastDispatchedAt = time.Now().UTC()
	if err := persistMutation(job, progress, status, mutation); err != nil {
		return ReacquireMutation{}, err
	}
	return mutation, nil
}

func confirmMutation(job *ReacquireJob, progress JobProgress, status ReacquireStatus, mutation ReacquireMutation, receiptID int) error {
	mutation.State = ReacquireMutationConfirmed
	mutation.ReceiptID = receiptID
	mutation.ConfirmedAt = time.Now().UTC()
	if err := persistMutation(job, progress, status, mutation); err != nil {
		return unknownMutationOutcome(fmt.Errorf("persist confirmed Arr mutation: %w", err), 0)
	}
	return nil
}

func mutationVisibilityRemaining(mutation ReacquireMutation) time.Duration {
	if mutation.LastDispatchedAt.IsZero() {
		return 0
	}
	return max(0, time.Until(mutation.LastDispatchedAt.Add(mutationVisibilityDelay)))
}

func mutationRedispatchError(mutation ReacquireMutation) error {
	if remaining := mutationVisibilityRemaining(mutation); remaining > 0 {
		return unknownMutationOutcome(nil, remaining)
	}
	if mutation.Attempts >= maxMutationAttempts {
		return fmt.Errorf(
			"Arr mutation %q was not observed after %d dispatch attempts; refusing another automatic dispatch",
			mutation.Key,
			mutation.Attempts,
		)
	}
	return nil
}

func unresolvedMutation(mutation ReacquireMutation, dispatchErr, reconcileErr error) error {
	cause := dispatchErr
	if reconcileErr != nil {
		cause = errors.Join(dispatchErr, fmt.Errorf("reconcile Arr mutation: %w", reconcileErr))
	}
	return unknownMutationOutcome(cause, mutationVisibilityRemaining(mutation))
}

func unavailableMutationReconciliation(mutation ReacquireMutation, err error) error {
	if mutationVisibilityRemaining(mutation) == 0 && mutation.Attempts >= maxMutationAttempts {
		return fmt.Errorf(
			"Arr mutation %q reached %d dispatch attempts and reconciliation is unavailable; refusing another automatic dispatch: %w",
			mutation.Key,
			mutation.Attempts,
			err,
		)
	}
	return unknownMutationOutcome(err, mutationVisibilityRemaining(mutation))
}

func mutationKey(kind ReacquireMutationKind, values ...string) string {
	return string(kind) + ":" + strings.Join(values, ":")
}

func findCommandReceipt(commands []Command, mutation ReacquireMutation) (Command, bool) {
	for _, command := range commands {
		if (!strings.EqualFold(command.Name, mutation.CommandName) &&
			!strings.EqualFold(command.Body.Name, mutation.CommandName)) ||
			command.Queued.IsZero() ||
			command.Queued.Before(mutation.LastDispatchedAt.Add(-mutationClockSkew)) {
			continue
		}
		if commandScopeMatches(command.Body, mutation) {
			return command, true
		}
	}
	return Command{}, false
}

func commandScopeMatches(body CommandBody, mutation ReacquireMutation) bool {
	switch mutation.Kind {
	case ReacquireMutationEpisodeSearch:
		return equalNormalizedIDs(body.EpisodeIDs, mutation.EpisodeIDs)
	case ReacquireMutationSeasonSearch:
		return body.SeriesID == mutation.SeriesID && body.SeasonNumber == mutation.SeasonNumber
	case ReacquireMutationMovieSearch:
		return equalNormalizedIDs(body.MovieIDs, mutation.MovieIDs)
	default:
		return false
	}
}

func equalNormalizedIDs(left, right []int) bool {
	left = slices.Clone(left)
	right = slices.Clone(right)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

func findGrabReceipt(records []HistoryRecord, mutation ReacquireMutation) (HistoryRecord, bool) {
	for _, record := range records {
		if !strings.EqualFold(record.EventType, HistoryEventGrabbed) ||
			record.Date.IsZero() ||
			record.Date.Before(mutation.LastDispatchedAt.Add(-mutationClockSkew)) {
			continue
		}
		guid, guidFound := historyDataValue(record.Data, "guid")
		indexer, indexerFound := historyDataValue(record.Data, "indexer")
		if !guidFound || !indexerFound || guid != mutation.ReleaseGUID || indexer != mutation.ReleaseIndexer {
			continue
		}
		if grabScopeMatches(record, mutation) {
			return record, true
		}
	}
	return HistoryRecord{}, false
}

func grabScopeMatches(record HistoryRecord, mutation ReacquireMutation) bool {
	if len(mutation.MovieIDs) > 0 {
		return slices.Contains(mutation.MovieIDs, record.MovieID)
	}
	if len(mutation.EpisodeIDs) > 0 {
		return record.SeriesID == mutation.SeriesID && slices.Contains(mutation.EpisodeIDs, record.EpisodeID)
	}
	return false
}

func historyDataValue(data map[string]string, key string) (string, bool) {
	for currentKey, value := range data {
		if strings.EqualFold(currentKey, key) {
			return value, true
		}
	}
	return "", false
}
