package arr

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type reacquireHandler struct {
	registry    *Storage
	invalidator ReacquireInvalidator
}

type ReacquireInvalidator interface {
	InvalidateReacquire(context.Context, ReacquireJob) error
}

func NewReacquireHandler(registry *Storage, invalidators ...ReacquireInvalidator) ReacquireHandler {
	var invalidator ReacquireInvalidator
	if len(invalidators) > 0 {
		invalidator = invalidators[0]
	}
	return &reacquireHandler{registry: registry, invalidator: invalidator}
}

func (handler *reacquireHandler) Reacquire(ctx context.Context, job ReacquireJob, progress JobProgress) error {
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
		return fmt.Errorf("Arr completed download handling is disabled")
	}

	var failurePlan exactDownloadFailure
	if job.Strategy == ReacquireStrategyHistoryFailed || job.Strategy == ReacquireStrategyInteractiveBest {
		failurePlan, err = prepareExactDownloadFailure(ctx, instance, job.DownloadID)
		if err != nil {
			return err
		}
	}
	if job.Strategy == ReacquireStrategyInteractiveBest {
		if !failurePlan.grabFound {
			return fmt.Errorf("interactive reacquisition requires exact grab history")
		}
		if autoRedownloadsFailure(downloadConfig, failurePlan) {
			return fmt.Errorf("interactive reacquisition requires automatic failed-download redownload to be disabled")
		}
	}

	if err := progress.Update(ReacquireStatusInvalidating, nil); err != nil {
		return err
	}
	if err := deleteArrFiles(ctx, instance, bindings); err != nil {
		return err
	}
	var waitingStatus ReacquireStatus
	switch job.Strategy {
	case ReacquireStrategyHistoryFailed:
		waitingStatus, err = handler.failHistory(ctx, instance, &job, bindings, failurePlan, downloadConfig, progress)
	case ReacquireStrategyCommandSearch:
		waitingStatus, err = searchBindings(ctx, instance, &job, bindings, progress)
	case ReacquireStrategyInteractiveBest:
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

func (handler *reacquireHandler) failHistory(
	ctx context.Context,
	instance *Arr,
	job *ReacquireJob,
	bindings []Binding,
	failure exactDownloadFailure,
	config DownloadClientConfig,
	progress JobProgress,
) (ReacquireStatus, error) {
	if !failure.found {
		return searchBindings(ctx, instance, job, bindings, progress)
	}
	if err := executeExactDownloadFailure(ctx, instance, job, failure, progress); err != nil {
		return "", err
	}
	if failure.alreadyFailed {
		mutation, _ := mutationForKey(*job, mutationKey(ReacquireMutationHistoryFailed, failure.downloadID))
		if mutation.Attempts == 0 {
			return searchBindings(ctx, instance, job, bindings, progress)
		}
	}
	if !autoRedownloadsFailure(config, failure) {
		return searchBindings(ctx, instance, job, bindings, progress)
	}
	return ReacquireStatusWaitingForGrab, nil
}

type exactDownloadFailure struct {
	found         bool
	grabFound     bool
	alreadyFailed bool
	downloadID    string
	historyID     int
	failedID      int
	grabRecord    HistoryRecord
}

func prepareExactDownloadFailure(ctx context.Context, instance *Arr, downloadID string) (exactDownloadFailure, error) {
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

func autoRedownloadsFailure(config DownloadClientConfig, failure exactDownloadFailure) bool {
	if !config.AutoRedownloadFailed {
		return false
	}
	releaseSource, _ := historyDataValue(failure.grabRecord.Data, "releaseSource")
	return !strings.EqualFold(releaseSource, "InteractiveSearch") || config.AutoRedownloadFailedFromInteractiveSearch
}

func executeExactDownloadFailure(
	ctx context.Context,
	instance *Arr,
	job *ReacquireJob,
	failure exactDownloadFailure,
	progress JobProgress,
) error {
	if !failure.found {
		return nil
	}
	if err := progress.Update(ReacquireStatusBlocklisting, nil); err != nil {
		return err
	}
	mutation := ReacquireMutation{
		Key:        mutationKey(ReacquireMutationHistoryFailed, failure.downloadID),
		Kind:       ReacquireMutationHistoryFailed,
		DownloadID: failure.downloadID,
		HistoryID:  failure.historyID,
	}
	mutation, err := ensureMutationIntent(job, progress, ReacquireStatusBlocklisting, mutation)
	if err != nil {
		return err
	}
	if mutation.State == ReacquireMutationConfirmed {
		return nil
	}
	if failure.alreadyFailed {
		return confirmMutation(job, progress, ReacquireStatusBlocklisting, mutation, failure.failedID)
	}
	if mutation.Attempts > 0 {
		record, found, err := instance.FindDownloadFailedHistoryByDownloadID(ctx, failure.downloadID)
		if err != nil {
			return unavailableMutationReconciliation(mutation, fmt.Errorf("reconcile failed-download history: %w", err))
		}
		if found {
			return confirmMutation(job, progress, ReacquireStatusBlocklisting, mutation, record.ID)
		}
		if err := mutationRedispatchError(mutation); err != nil {
			return err
		}
	}
	mutation, err = recordMutationAttempt(job, progress, ReacquireStatusBlocklisting, mutation)
	if err != nil {
		return err
	}
	if err := instance.MarkHistoryFailedCtx(ctx, mutation.HistoryID); err != nil {
		if !errors.Is(err, errMutationOutcomeUnknown) {
			return err
		}
		record, found, reconcileErr := instance.FindDownloadFailedHistoryByDownloadID(ctx, failure.downloadID)
		if reconcileErr == nil && found {
			return confirmMutation(job, progress, ReacquireStatusBlocklisting, mutation, record.ID)
		}
		return unresolvedMutation(mutation, err, reconcileErr)
	}
	return confirmMutation(job, progress, ReacquireStatusBlocklisting, mutation, mutation.HistoryID)
}

func mutationBindings(job ReacquireJob) ([]Binding, error) {
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
		return nil, fmt.Errorf("requested Arr binding no longer authorizes mutation")
	}
	return bindings, nil
}

func deleteArrFiles(ctx context.Context, instance *Arr, bindings []Binding) error {
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

func bindingMatchesManagedFile(binding Binding, current LibraryFile) bool {
	if binding.ArrFileID != current.ArrFileID || !sameLibraryPath(binding.LibraryPath, current.Path) {
		return false
	}
	switch binding.ArrType {
	case Sonarr:
		return binding.SeriesID > 0 &&
			binding.SeriesID == current.SeriesID &&
			binding.SeasonNumber == current.SeasonNumber &&
			slices.Equal(binding.EpisodeIDs, current.EpisodeIDs)
	case Radarr:
		return binding.MovieID > 0 && binding.MovieID == current.MovieID
	default:
		return false
	}
}

func validateSearchBindings(instance *Arr, bindings []Binding) error {
	switch instance.Type {
	case Sonarr:
		episodeIDs, seriesID, _ := sonarrTargets(bindings)
		if len(episodeIDs) == 0 && seriesID <= 0 {
			return fmt.Errorf("Sonarr binding has no episode or series target")
		}
	case Radarr:
		if len(movieTargets(bindings)) == 0 {
			return fmt.Errorf("Radarr binding has no movie target")
		}
	default:
		return fmt.Errorf("search unsupported for arr type %q", instance.Type)
	}
	return nil
}

func searchBindings(
	ctx context.Context,
	instance *Arr,
	job *ReacquireJob,
	bindings []Binding,
	progress JobProgress,
) (ReacquireStatus, error) {
	if err := progress.Update(ReacquireStatusSearching, nil); err != nil {
		return "", err
	}

	mutation, err := searchMutation(instance, bindings)
	if err != nil {
		return "", err
	}
	mutation, err = ensureMutationIntent(job, progress, ReacquireStatusSearching, mutation)
	if err != nil {
		return "", err
	}
	if mutation.State == ReacquireMutationConfirmed {
		return ReacquireStatusWaitingForGrab, nil
	}
	if mutation.Attempts > 0 {
		command, found, err := reconcileCommandMutation(ctx, instance, mutation)
		if err != nil {
			return "", unavailableMutationReconciliation(mutation, err)
		}
		if found {
			if err := confirmMutation(job, progress, ReacquireStatusSearching, mutation, command.ID); err != nil {
				return "", err
			}
			return ReacquireStatusWaitingForGrab, nil
		}
		if err := mutationRedispatchError(mutation); err != nil {
			return "", err
		}
	}
	mutation, err = recordMutationAttempt(job, progress, ReacquireStatusSearching, mutation)
	if err != nil {
		return "", err
	}
	command, err := dispatchSearchCommand(ctx, instance, mutation)
	if err != nil {
		if !errors.Is(err, errMutationOutcomeUnknown) {
			return "", err
		}
		receipt, found, reconcileErr := reconcileCommandMutation(ctx, instance, mutation)
		if reconcileErr == nil && found {
			if err := confirmMutation(job, progress, ReacquireStatusSearching, mutation, receipt.ID); err != nil {
				return "", err
			}
			return ReacquireStatusWaitingForGrab, nil
		}
		return "", unresolvedMutation(mutation, err, reconcileErr)
	}
	if err := confirmMutation(job, progress, ReacquireStatusSearching, mutation, command.ID); err != nil {
		return "", err
	}
	return ReacquireStatusWaitingForGrab, nil
}

func searchMutation(instance *Arr, bindings []Binding) (ReacquireMutation, error) {
	switch instance.Type {
	case Sonarr:
		episodeIDs, seriesID, seasonNumber := sonarrTargets(bindings)
		if len(episodeIDs) > 0 {
			return ReacquireMutation{
				Key:         mutationKey(ReacquireMutationEpisodeSearch, idListKey(episodeIDs)),
				Kind:        ReacquireMutationEpisodeSearch,
				CommandName: "EpisodeSearch",
				EpisodeIDs:  episodeIDs,
			}, nil
		}
		return ReacquireMutation{
			Key:          mutationKey(ReacquireMutationSeasonSearch, strconv.Itoa(seriesID), strconv.Itoa(seasonNumber)),
			Kind:         ReacquireMutationSeasonSearch,
			CommandName:  "SeasonSearch",
			SeriesID:     seriesID,
			SeasonNumber: seasonNumber,
		}, nil
	case Radarr:
		movieIDs := movieTargets(bindings)
		return ReacquireMutation{
			Key:         mutationKey(ReacquireMutationMovieSearch, idListKey(movieIDs)),
			Kind:        ReacquireMutationMovieSearch,
			CommandName: "MoviesSearch",
			MovieIDs:    movieIDs,
		}, nil
	default:
		return ReacquireMutation{}, fmt.Errorf("search unsupported for arr type %q", instance.Type)
	}
}

func dispatchSearchCommand(ctx context.Context, instance *Arr, mutation ReacquireMutation) (Command, error) {
	switch mutation.Kind {
	case ReacquireMutationEpisodeSearch:
		return instance.SearchEpisodes(ctx, mutation.EpisodeIDs)
	case ReacquireMutationSeasonSearch:
		return instance.SearchSeason(ctx, mutation.SeriesID, mutation.SeasonNumber)
	case ReacquireMutationMovieSearch:
		return instance.SearchMovies(ctx, mutation.MovieIDs)
	default:
		return Command{}, fmt.Errorf("unsupported Arr command mutation %q", mutation.Kind)
	}
}

func reconcileCommandMutation(ctx context.Context, instance *Arr, mutation ReacquireMutation) (Command, bool, error) {
	commands, err := instance.Commands(ctx)
	if err != nil {
		return Command{}, false, fmt.Errorf("reconcile Arr command: %w", err)
	}
	command, found := findCommandReceipt(commands, mutation)
	return command, found, nil
}

func grabBestRelease(
	ctx context.Context,
	instance *Arr,
	job *ReacquireJob,
	bindings []Binding,
	progress JobProgress,
) (ReacquireStatus, error) {
	if err := progress.Update(ReacquireStatusSearching, nil); err != nil {
		return "", err
	}

	if mutation, found := mutationOfKind(*job, ReacquireMutationReleaseGrab); found {
		if mutation.State == ReacquireMutationConfirmed {
			return ReacquireStatusWaitingForDownload, nil
		}
		if mutation.Attempts > 0 {
			record, found, err := reconcileReleaseMutation(ctx, instance, mutation)
			if err != nil {
				return "", unavailableMutationReconciliation(mutation, err)
			}
			if found {
				if err := confirmMutation(job, progress, ReacquireStatusSearching, mutation, record.ID); err != nil {
					return "", err
				}
				return ReacquireStatusWaitingForDownload, nil
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
		mutation, err = ensureMutationIntent(job, progress, ReacquireStatusSearching, mutation)
		if err != nil {
			return "", err
		}
		return dispatchReleaseMutation(ctx, instance, job, mutation, release, progress)
	}
	return "", fmt.Errorf("Arr returned no downloadable replacement release with reconcilable identity")
}

func searchReplacementReleases(ctx context.Context, instance *Arr, bindings []Binding) ([]Release, error) {
	var (
		releases []Release
		err      error
	)
	switch instance.Type {
	case Sonarr:
		episodeIDs, seriesID, seasonNumber := sonarrTargets(bindings)
		if len(episodeIDs) == 1 {
			releases, err = instance.SearchEpisodeReleases(ctx, episodeIDs[0])
		} else {
			if !singleSonarrSeason(bindings, seriesID, seasonNumber) {
				return nil, fmt.Errorf("interactive Sonarr reacquisition spans multiple seasons")
			}
			releases, err = instance.SearchSeasonReleases(ctx, seriesID, seasonNumber)
		}
	case Radarr:
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

func releaseMutation(bindings []Binding, release Release) (ReacquireMutation, error) {
	if len(bindings) == 0 {
		return ReacquireMutation{}, fmt.Errorf("release mutation requires Arr bindings")
	}
	mutation := ReacquireMutation{
		Key:              mutationKey(ReacquireMutationReleaseGrab, release.GUID, strconv.Itoa(release.IndexerID)),
		Kind:             ReacquireMutationReleaseGrab,
		ReleaseGUID:      release.GUID,
		ReleaseIndexer:   release.Indexer,
		ReleaseIndexerID: release.IndexerID,
	}
	switch bindings[0].ArrType {
	case Sonarr:
		mutation.EpisodeIDs, mutation.SeriesID, mutation.SeasonNumber = sonarrTargets(bindings)
		if len(mutation.EpisodeIDs) == 0 {
			return ReacquireMutation{}, fmt.Errorf("interactive Sonarr reacquisition cannot be reconciled without episode IDs")
		}
	case Radarr:
		movieIDs := movieTargets(bindings)
		if len(movieIDs) == 0 {
			return ReacquireMutation{}, fmt.Errorf("interactive Radarr reacquisition cannot be reconciled without a movie ID")
		}
		mutation.MovieIDs = []int{movieIDs[0]}
	default:
		return ReacquireMutation{}, fmt.Errorf("interactive search unsupported for arr type %q", bindings[0].ArrType)
	}
	return mutation, nil
}

func findPersistedRelease(
	ctx context.Context,
	instance *Arr,
	bindings []Binding,
	mutation ReacquireMutation,
) (Release, error) {
	releases, err := searchReplacementReleases(ctx, instance, bindings)
	if err != nil {
		return Release{}, err
	}
	for _, release := range releases {
		if release.GUID == mutation.ReleaseGUID &&
			release.Indexer == mutation.ReleaseIndexer &&
			release.IndexerID == mutation.ReleaseIndexerID &&
			releaseEligible(release) {
			return release, nil
		}
	}
	return Release{}, fmt.Errorf("previously selected Arr release is no longer available; refusing to substitute another release")
}

func dispatchReleaseMutation(
	ctx context.Context,
	instance *Arr,
	job *ReacquireJob,
	mutation ReacquireMutation,
	release Release,
	progress JobProgress,
) (ReacquireStatus, error) {
	mutation, err := recordMutationAttempt(job, progress, ReacquireStatusSearching, mutation)
	if err != nil {
		return "", err
	}
	if err := instance.GrabRelease(ctx, release); err != nil {
		if !errors.Is(err, errMutationOutcomeUnknown) {
			return "", err
		}
		record, found, reconcileErr := reconcileReleaseMutation(ctx, instance, mutation)
		if reconcileErr == nil && found {
			if err := confirmMutation(job, progress, ReacquireStatusSearching, mutation, record.ID); err != nil {
				return "", err
			}
			return ReacquireStatusWaitingForDownload, nil
		}
		return "", unresolvedMutation(mutation, err, reconcileErr)
	}
	if err := confirmMutation(job, progress, ReacquireStatusSearching, mutation, 0); err != nil {
		return "", err
	}
	return ReacquireStatusWaitingForDownload, nil
}

func reconcileReleaseMutation(
	ctx context.Context,
	instance *Arr,
	mutation ReacquireMutation,
) (HistoryRecord, bool, error) {
	records, err := instance.GrabHistorySince(ctx, mutation.LastDispatchedAt.Add(-mutationClockSkew))
	if err != nil {
		return HistoryRecord{}, false, fmt.Errorf("reconcile release grab: %w", err)
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

func releaseEligible(release Release) bool {
	return !release.Rejected && !release.TemporarilyRejected && len(release.Rejections) == 0 && (release.DownloadAllowed || release.Approved)
}
