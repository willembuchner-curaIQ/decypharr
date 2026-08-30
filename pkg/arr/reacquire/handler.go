package reacquire

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"slices"
	"strconv"
	"strings"
)

type arrHandler struct {
	registry    *arr.Storage
	invalidator Invalidator
}

type Invalidator interface {
	InvalidateReacquire(context.Context, Job) error
}

func NewHandler(registry *arr.Storage, invalidators ...Invalidator) Handler {
	var invalidator Invalidator
	if len(invalidators) > 0 {
		invalidator = invalidators[0]
	}
	return &arrHandler{registry: registry, invalidator: invalidator}
}

func (handler *arrHandler) Reacquire(ctx context.Context, job Job, progress JobProgress) error {
	if handler == nil || handler.registry == nil {
		return fmt.Errorf("arr reacquirer is not configured")
	}
	instance := handler.registry.Get(job.ArrName)
	if instance == nil {
		return fmt.Errorf("arr %q is not configured", job.ArrName)
	}
	bindings, err := mutationBindings(job)
	if err != nil {
		return err
	}
	if err := validateMutationInstance(instance, bindings); err != nil {
		return err
	}
	if err := validateSearchBindings(instance, bindings); err != nil {
		return err
	}
	downloadConfig, err := instance.GetDownloadClientConfig(ctx)
	if err != nil {
		return err
	}
	if !downloadConfig.EnableCompletedDownloadHandling {
		return fmt.Errorf("arr.Arr completed download handling is disabled")
	}

	var failurePlan exactDownloadFailure
	if job.Strategy == StrategyHistoryFailed || job.Strategy == StrategyInteractiveBest {
		failurePlan, err = prepareExactDownloadFailure(ctx, instance, job.DownloadID)
		if err != nil {
			return err
		}
	}
	if job.Strategy == StrategyInteractiveBest {
		if !failurePlan.grabFound {
			return fmt.Errorf("interactive reacquisition requires exact grab history")
		}
		if autoRedownloadsFailure(downloadConfig, failurePlan) {
			return fmt.Errorf("interactive reacquisition requires automatic failed-download redownload to be disabled")
		}
	}

	if err := progress.Update(StatusInvalidating, nil); err != nil {
		return err
	}
	if err := deleteArrFiles(ctx, instance, bindings); err != nil {
		return err
	}
	var waitingStatus Status
	switch job.Strategy {
	case StrategyHistoryFailed:
		waitingStatus, err = handler.failHistory(ctx, instance, &job, bindings, failurePlan, downloadConfig, progress)
	case StrategyCommandSearch:
		waitingStatus, err = searchBindings(ctx, instance, &job, bindings, progress)
	case StrategyInteractiveBest:
		if err = executeExactDownloadFailure(ctx, instance, &job, failurePlan, progress); err == nil {
			waitingStatus, err = grabBestRelease(ctx, instance, &job, bindings, progress)
		}
	default:
		return fmt.Errorf("unsupported reacquire strategy %q", job.Strategy)
	}
	if err != nil {
		return err
	}
	if handler.invalidator != nil {
		invalidationJob := job
		invalidationJob.Bindings = bindings
		if err := handler.invalidator.InvalidateReacquire(ctx, invalidationJob); err != nil {
			return err
		}
	}
	return progress.Update(waitingStatus, nil)
}

func (handler *arrHandler) failHistory(
	ctx context.Context,
	instance *arr.Arr,
	job *Job,
	bindings []Binding,
	failure exactDownloadFailure,
	config arr.DownloadClientConfig,
	progress JobProgress,
) (Status, error) {
	if !failure.found {
		return searchBindings(ctx, instance, job, bindings, progress)
	}
	if err := executeExactDownloadFailure(ctx, instance, job, failure, progress); err != nil {
		return "", err
	}
	if failure.alreadyFailed {
		mutation, _ := mutationForKey(*job, mutationKey(MutationHistoryFailed, failure.downloadID))
		if mutation.Attempts == 0 {
			return searchBindings(ctx, instance, job, bindings, progress)
		}
	}
	if !autoRedownloadsFailure(config, failure) {
		return searchBindings(ctx, instance, job, bindings, progress)
	}
	return StatusWaitingForGrab, nil
}

type exactDownloadFailure struct {
	found         bool
	grabFound     bool
	alreadyFailed bool
	downloadID    string
	historyID     int
	failedID      int
	grabRecord    arr.HistoryRecord
}

func prepareExactDownloadFailure(ctx context.Context, instance *arr.Arr, downloadID string) (exactDownloadFailure, error) {
	if downloadID == "" {
		return exactDownloadFailure{}, nil
	}
	failedRecord, alreadyFailed, err := instance.FindDownloadFailedHistoryByDownloadID(ctx, downloadID)
	if err != nil {
		return exactDownloadFailure{}, err
	}
	record, found, err := instance.FindGrabHistoryByDownloadID(ctx, downloadID)
	if err != nil {
		return exactDownloadFailure{}, err
	}
	if alreadyFailed {
		return exactDownloadFailure{
			found:         true,
			grabFound:     found,
			alreadyFailed: true,
			downloadID:    downloadID,
			failedID:      failedRecord.ID,
			grabRecord:    record,
		}, nil
	}
	return exactDownloadFailure{
		found:      found,
		grabFound:  found,
		downloadID: downloadID,
		historyID:  record.ID,
		grabRecord: record,
	}, nil
}

func autoRedownloadsFailure(config arr.DownloadClientConfig, failure exactDownloadFailure) bool {
	if !config.AutoRedownloadFailed {
		return false
	}
	releaseSource, _ := historyDataValue(failure.grabRecord.Data, "releaseSource")
	return !strings.EqualFold(releaseSource, "InteractiveSearch") || config.AutoRedownloadFailedFromInteractiveSearch
}

func executeExactDownloadFailure(
	ctx context.Context,
	instance *arr.Arr,
	job *Job,
	failure exactDownloadFailure,
	progress JobProgress,
) error {
	if !failure.found {
		return nil
	}
	if err := progress.Update(StatusBlocklisting, nil); err != nil {
		return err
	}
	mutation := Mutation{
		Key:        mutationKey(MutationHistoryFailed, failure.downloadID),
		Kind:       MutationHistoryFailed,
		DownloadID: failure.downloadID,
		HistoryID:  failure.historyID,
	}
	mutation, err := ensureMutationIntent(job, progress, StatusBlocklisting, mutation)
	if err != nil {
		return err
	}
	if mutation.State == MutationConfirmed {
		return nil
	}
	if failure.alreadyFailed {
		return confirmMutation(job, progress, StatusBlocklisting, mutation, failure.failedID)
	}
	if mutation.Attempts > 0 {
		record, found, err := instance.FindDownloadFailedHistoryByDownloadID(ctx, failure.downloadID)
		if err != nil {
			return unavailableMutationReconciliation(mutation, fmt.Errorf("reconcile failed-download history: %w", err))
		}
		if found {
			return confirmMutation(job, progress, StatusBlocklisting, mutation, record.ID)
		}
		if err := mutationRedispatchError(mutation); err != nil {
			return err
		}
	}
	mutation, err = recordMutationAttempt(job, progress, StatusBlocklisting, mutation)
	if err != nil {
		return err
	}
	if err := instance.MarkHistoryFailedCtx(ctx, mutation.HistoryID); err != nil {
		if !errors.Is(err, arr.ErrMutationOutcomeUnknown) {
			return err
		}
		record, found, reconcileErr := instance.FindDownloadFailedHistoryByDownloadID(ctx, failure.downloadID)
		if reconcileErr == nil && found {
			return confirmMutation(job, progress, StatusBlocklisting, mutation, record.ID)
		}
		return unresolvedMutation(mutation, err, reconcileErr)
	}
	return confirmMutation(job, progress, StatusBlocklisting, mutation, mutation.HistoryID)
}

func mutationBindings(job Job) ([]Binding, error) {
	bindings := make([]Binding, 0, len(job.Bindings))
	requestedFound := false
	for _, binding := range job.Bindings {
		if binding.ArrName != job.ArrName || binding.ArrType != job.ArrType || !binding.AuthorizesMutation() {
			continue
		}
		if binding.EntryID == job.EntryID && binding.EntryFileID == job.FileID {
			requestedFound = true
		}
		bindings = append(bindings, binding)
	}
	if !requestedFound {
		return nil, fmt.Errorf("requested arr.Arr binding no longer authorizes mutation")
	}
	return bindings, nil
}

func deleteArrFiles(ctx context.Context, instance *arr.Arr, bindings []Binding) error {
	byFileID := make(map[int]Binding, len(bindings))
	for _, binding := range bindings {
		if binding.ArrFileID <= 0 {
			continue
		}
		byFileID[binding.ArrFileID] = binding
	}

	present := make([]int, 0, len(byFileID))
	for fileID, binding := range byFileID {
		current, found, err := instance.ManagedFile(ctx, binding.ArrFileID)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if !bindingMatchesManagedFile(binding, current) {
			return fmt.Errorf("arr file %d no longer matches the indexed media", binding.ArrFileID)
		}
		present = append(present, fileID)
	}

	slices.Sort(present)
	for _, fileID := range present {
		if err := instance.DeleteManagedFile(ctx, fileID); err != nil {
			return err
		}
	}
	return nil
}

func bindingMatchesManagedFile(binding Binding, current arr.LibraryFile) bool {
	if binding.ArrFileID != current.ArrFileID || !sameLibraryPath(binding.LibraryPath, current.Path) {
		return false
	}
	switch binding.ArrType {
	case arr.Sonarr:
		return binding.SeriesID > 0 &&
			binding.SeriesID == current.SeriesID &&
			binding.SeasonNumber == current.SeasonNumber &&
			slices.Equal(binding.EpisodeIDs, current.EpisodeIDs)
	case arr.Radarr:
		return binding.MovieID > 0 && binding.MovieID == current.MovieID
	default:
		return false
	}
}

func validateSearchBindings(instance *arr.Arr, bindings []Binding) error {
	switch instance.Type {
	case arr.Sonarr:
		episodeIDs, seriesID, _ := sonarrTargets(bindings)
		if len(episodeIDs) == 0 && seriesID <= 0 {
			return fmt.Errorf("arr.Sonarr binding has no episode or series target")
		}
	case arr.Radarr:
		if len(movieTargets(bindings)) == 0 {
			return fmt.Errorf("arr.Radarr binding has no movie target")
		}
	default:
		return fmt.Errorf("search unsupported for arr type %q", instance.Type)
	}
	return nil
}

func searchBindings(
	ctx context.Context,
	instance *arr.Arr,
	job *Job,
	bindings []Binding,
	progress JobProgress,
) (Status, error) {
	if err := progress.Update(StatusSearching, nil); err != nil {
		return "", err
	}

	mutation, err := searchMutation(instance, bindings)
	if err != nil {
		return "", err
	}
	mutation, err = ensureMutationIntent(job, progress, StatusSearching, mutation)
	if err != nil {
		return "", err
	}
	if mutation.State == MutationConfirmed {
		return StatusWaitingForGrab, nil
	}
	if mutation.Attempts > 0 {
		command, found, err := reconcileCommandMutation(ctx, instance, mutation)
		if err != nil {
			return "", unavailableMutationReconciliation(mutation, err)
		}
		if found {
			if err := confirmMutation(job, progress, StatusSearching, mutation, command.ID); err != nil {
				return "", err
			}
			return StatusWaitingForGrab, nil
		}
		if err := mutationRedispatchError(mutation); err != nil {
			return "", err
		}
	}
	mutation, err = recordMutationAttempt(job, progress, StatusSearching, mutation)
	if err != nil {
		return "", err
	}
	command, err := dispatchSearchCommand(ctx, instance, mutation)
	if err != nil {
		if !errors.Is(err, arr.ErrMutationOutcomeUnknown) {
			return "", err
		}
		receipt, found, reconcileErr := reconcileCommandMutation(ctx, instance, mutation)
		if reconcileErr == nil && found {
			if err := confirmMutation(job, progress, StatusSearching, mutation, receipt.ID); err != nil {
				return "", err
			}
			return StatusWaitingForGrab, nil
		}
		return "", unresolvedMutation(mutation, err, reconcileErr)
	}
	if err := confirmMutation(job, progress, StatusSearching, mutation, command.ID); err != nil {
		return "", err
	}
	return StatusWaitingForGrab, nil
}

func searchMutation(instance *arr.Arr, bindings []Binding) (Mutation, error) {
	switch instance.Type {
	case arr.Sonarr:
		episodeIDs, seriesID, seasonNumber := sonarrTargets(bindings)
		if len(episodeIDs) > 0 {
			return Mutation{
				Key:         mutationKey(MutationEpisodeSearch, idListKey(episodeIDs)),
				Kind:        MutationEpisodeSearch,
				CommandName: "EpisodeSearch",
				EpisodeIDs:  episodeIDs,
			}, nil
		}
		return Mutation{
			Key:          mutationKey(MutationSeasonSearch, strconv.Itoa(seriesID), strconv.Itoa(seasonNumber)),
			Kind:         MutationSeasonSearch,
			CommandName:  "SeasonSearch",
			SeriesID:     seriesID,
			SeasonNumber: seasonNumber,
		}, nil
	case arr.Radarr:
		movieIDs := movieTargets(bindings)
		return Mutation{
			Key:         mutationKey(MutationMovieSearch, idListKey(movieIDs)),
			Kind:        MutationMovieSearch,
			CommandName: "MoviesSearch",
			MovieIDs:    movieIDs,
		}, nil
	default:
		return Mutation{}, fmt.Errorf("search unsupported for arr type %q", instance.Type)
	}
}

func dispatchSearchCommand(ctx context.Context, instance *arr.Arr, mutation Mutation) (arr.Command, error) {
	switch mutation.Kind {
	case MutationEpisodeSearch:
		return instance.SearchEpisodes(ctx, mutation.EpisodeIDs)
	case MutationSeasonSearch:
		return instance.SearchSeason(ctx, mutation.SeriesID, mutation.SeasonNumber)
	case MutationMovieSearch:
		return instance.SearchMovies(ctx, mutation.MovieIDs)
	default:
		return arr.Command{}, fmt.Errorf("unsupported arr.Arr command mutation %q", mutation.Kind)
	}
}

func reconcileCommandMutation(ctx context.Context, instance *arr.Arr, mutation Mutation) (arr.Command, bool, error) {
	commands, err := instance.Commands(ctx)
	if err != nil {
		return arr.Command{}, false, fmt.Errorf("reconcile arr.Arr command: %w", err)
	}
	command, found := findCommandReceipt(commands, mutation)
	return command, found, nil
}

func grabBestRelease(
	ctx context.Context,
	instance *arr.Arr,
	job *Job,
	bindings []Binding,
	progress JobProgress,
) (Status, error) {
	if err := progress.Update(StatusSearching, nil); err != nil {
		return "", err
	}

	if mutation, found := mutationOfKind(*job, MutationReleaseGrab); found {
		if mutation.State == MutationConfirmed {
			return StatusWaitingForDownload, nil
		}
		if mutation.Attempts > 0 {
			record, found, err := reconcileReleaseMutation(ctx, instance, mutation)
			if err != nil {
				return "", unavailableMutationReconciliation(mutation, err)
			}
			if found {
				if err := confirmMutation(job, progress, StatusSearching, mutation, record.ID); err != nil {
					return "", err
				}
				return StatusWaitingForDownload, nil
			}
			if err := mutationRedispatchError(mutation); err != nil {
				return "", err
			}
		}
		release, err := findPersistedRelease(ctx, instance, bindings, mutation)
		if err != nil {
			return "", err
		}
		return dispatchReleaseMutation(ctx, instance, job, mutation, release, progress)
	}

	releases, err := searchReplacementReleases(ctx, instance, bindings)
	if err != nil {
		return "", err
	}
	for _, release := range releases {
		if !releaseEligible(release) || release.GUID == "" || release.Indexer == "" || release.IndexerID <= 0 {
			continue
		}
		mutation, err := releaseMutation(bindings, release)
		if err != nil {
			return "", err
		}
		mutation, err = ensureMutationIntent(job, progress, StatusSearching, mutation)
		if err != nil {
			return "", err
		}
		return dispatchReleaseMutation(ctx, instance, job, mutation, release, progress)
	}
	return "", fmt.Errorf("arr.Arr returned no downloadable replacement release with reconcilable identity")
}

func searchReplacementReleases(ctx context.Context, instance *arr.Arr, bindings []Binding) ([]arr.Release, error) {
	var (
		releases []arr.Release
		err      error
	)
	switch instance.Type {
	case arr.Sonarr:
		episodeIDs, seriesID, seasonNumber := sonarrTargets(bindings)
		if len(episodeIDs) == 1 {
			releases, err = instance.SearchEpisodeReleases(ctx, episodeIDs[0])
		} else {
			if !singleSonarrSeason(bindings, seriesID, seasonNumber) {
				return nil, fmt.Errorf("interactive arr.Sonarr reacquisition spans multiple seasons")
			}
			releases, err = instance.SearchSeasonReleases(ctx, seriesID, seasonNumber)
		}
	case arr.Radarr:
		movieIDs := movieTargets(bindings)
		if len(movieIDs) == 0 {
			return nil, fmt.Errorf("movie binding has no movie ID")
		}
		releases, err = instance.SearchMovieReleases(ctx, movieIDs[0])
	default:
		return nil, fmt.Errorf("interactive search unsupported for arr type %q", instance.Type)
	}
	if err != nil {
		return nil, err
	}
	return releases, nil
}

func releaseMutation(bindings []Binding, release arr.Release) (Mutation, error) {
	if len(bindings) == 0 {
		return Mutation{}, fmt.Errorf("release mutation requires arr.Arr bindings")
	}
	mutation := Mutation{
		Key:              mutationKey(MutationReleaseGrab, release.GUID, strconv.Itoa(release.IndexerID)),
		Kind:             MutationReleaseGrab,
		ReleaseGUID:      release.GUID,
		ReleaseIndexer:   release.Indexer,
		ReleaseIndexerID: release.IndexerID,
	}
	switch bindings[0].ArrType {
	case arr.Sonarr:
		mutation.EpisodeIDs, mutation.SeriesID, mutation.SeasonNumber = sonarrTargets(bindings)
		if len(mutation.EpisodeIDs) == 0 {
			return Mutation{}, fmt.Errorf("interactive arr.Sonarr reacquisition cannot be reconciled without episode IDs")
		}
	case arr.Radarr:
		movieIDs := movieTargets(bindings)
		if len(movieIDs) == 0 {
			return Mutation{}, fmt.Errorf("interactive arr.Radarr reacquisition cannot be reconciled without a movie ID")
		}
		mutation.MovieIDs = []int{movieIDs[0]}
	default:
		return Mutation{}, fmt.Errorf("interactive search unsupported for arr type %q", bindings[0].ArrType)
	}
	return mutation, nil
}

func findPersistedRelease(
	ctx context.Context,
	instance *arr.Arr,
	bindings []Binding,
	mutation Mutation,
) (arr.Release, error) {
	releases, err := searchReplacementReleases(ctx, instance, bindings)
	if err != nil {
		return arr.Release{}, err
	}
	for _, release := range releases {
		if release.GUID == mutation.ReleaseGUID &&
			release.Indexer == mutation.ReleaseIndexer &&
			release.IndexerID == mutation.ReleaseIndexerID &&
			releaseEligible(release) {
			return release, nil
		}
	}
	return arr.Release{}, fmt.Errorf("previously selected arr.Arr release is no longer available; refusing to substitute another release")
}

func dispatchReleaseMutation(
	ctx context.Context,
	instance *arr.Arr,
	job *Job,
	mutation Mutation,
	release arr.Release,
	progress JobProgress,
) (Status, error) {
	mutation, err := recordMutationAttempt(job, progress, StatusSearching, mutation)
	if err != nil {
		return "", err
	}
	if err := instance.GrabRelease(ctx, release); err != nil {
		if !errors.Is(err, arr.ErrMutationOutcomeUnknown) {
			return "", err
		}
		record, found, reconcileErr := reconcileReleaseMutation(ctx, instance, mutation)
		if reconcileErr == nil && found {
			if err := confirmMutation(job, progress, StatusSearching, mutation, record.ID); err != nil {
				return "", err
			}
			return StatusWaitingForDownload, nil
		}
		return "", unresolvedMutation(mutation, err, reconcileErr)
	}
	if err := confirmMutation(job, progress, StatusSearching, mutation, 0); err != nil {
		return "", err
	}
	return StatusWaitingForDownload, nil
}

func reconcileReleaseMutation(
	ctx context.Context,
	instance *arr.Arr,
	mutation Mutation,
) (arr.HistoryRecord, bool, error) {
	records, err := instance.GrabHistorySince(ctx, mutation.LastDispatchedAt.Add(-mutationClockSkew))
	if err != nil {
		return arr.HistoryRecord{}, false, fmt.Errorf("reconcile release grab: %w", err)
	}
	record, found := findGrabReceipt(records, mutation)
	return record, found, nil
}

func idListKey(ids []int) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(id)
	}
	return strings.Join(parts, ",")
}

func singleSonarrSeason(bindings []Binding, seriesID, seasonNumber int) bool {
	if seriesID <= 0 {
		return false
	}
	for _, binding := range bindings {
		if binding.SeriesID != seriesID || binding.SeasonNumber != seasonNumber {
			return false
		}
	}
	return true
}

func sonarrTargets(bindings []Binding) ([]int, int, int) {
	var episodeIDs []int
	seriesID := 0
	seasonNumber := 0
	for _, binding := range bindings {
		if seriesID == 0 && binding.SeriesID > 0 {
			seriesID = binding.SeriesID
			seasonNumber = binding.SeasonNumber
		}
		episodeIDs = append(episodeIDs, binding.EpisodeIDs...)
	}
	slices.Sort(episodeIDs)
	return slices.Compact(episodeIDs), seriesID, seasonNumber
}

func movieTargets(bindings []Binding) []int {
	movieIDs := make([]int, 0, len(bindings))
	for _, binding := range bindings {
		if binding.MovieID > 0 {
			movieIDs = append(movieIDs, binding.MovieID)
		}
	}
	slices.Sort(movieIDs)
	return slices.Compact(movieIDs)
}

func releaseEligible(release arr.Release) bool {
	return !release.Rejected && !release.TemporarilyRejected && len(release.Rejections) == 0 && (release.DownloadAllowed || release.Approved)
}
