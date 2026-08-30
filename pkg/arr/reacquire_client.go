package arr

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"
)

type DownloadClientConfig struct {
	ID                                        int  `json:"id"`
	EnableCompletedDownloadHandling           bool `json:"enableCompletedDownloadHandling"`
	AutoRedownloadFailed                      bool `json:"autoRedownloadFailed"`
	AutoRedownloadFailedFromInteractiveSearch bool `json:"autoRedownloadFailedFromInteractiveSearch"`
}

type Command struct {
	ID          int         `json:"id"`
	Name        string      `json:"name"`
	CommandName string      `json:"commandName"`
	Status      string      `json:"status"`
	Queued      time.Time   `json:"queued,omitzero"`
	Body        CommandBody `json:"body"`
}

type CommandBody struct {
	Name         string `json:"name"`
	EpisodeIDs   []int  `json:"episodeIds,omitempty"`
	SeriesID     int    `json:"seriesId,omitzero"`
	SeasonNumber int    `json:"seasonNumber,omitzero"`
	MovieIDs     []int  `json:"movieIds,omitempty"`
}

type managedFileResource struct {
	ID           int    `json:"id"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
	SeriesID     int    `json:"seriesId"`
	SeasonNumber int    `json:"seasonNumber"`
	MovieID      int    `json:"movieId"`
}

func (a *Arr) GetDownloadClientConfig(ctx context.Context) (DownloadClientConfig, error) {
	if a == nil {
		return DownloadClientConfig{}, fmt.Errorf("arr not configured")
	}

	var config DownloadClientConfig
	resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/config/downloadclient", nil, &config)
	if err != nil {
		return DownloadClientConfig{}, fmt.Errorf("get download client config: %w", err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return DownloadClientConfig{}, fmt.Errorf("get download client config: %w", err)
	}
	return config, nil
}

func (a *Arr) DeleteManagedFile(ctx context.Context, fileID int) error {
	if a == nil {
		return fmt.Errorf("arr not configured")
	}

	switch a.Type {
	case Sonarr:
		return a.DeleteEpisodeFile(ctx, fileID)
	case Radarr:
		return a.DeleteMovieFile(ctx, fileID)
	default:
		return fmt.Errorf("delete managed file: unsupported arr type %q", a.Type)
	}
}

func (a *Arr) ManagedFile(ctx context.Context, fileID int) (LibraryFile, bool, error) {
	if a == nil {
		return LibraryFile{}, false, fmt.Errorf("arr not configured")
	}
	if fileID <= 0 {
		return LibraryFile{}, false, fmt.Errorf("get managed file: invalid file ID %d", fileID)
	}

	var resource string
	switch a.Type {
	case Sonarr:
		resource = "episodefile"
	case Radarr:
		resource = "moviefile"
	default:
		return LibraryFile{}, false, fmt.Errorf("get managed file: unsupported arr type %q", a.Type)
	}

	var file managedFileResource
	endpoint := fmt.Sprintf("api/v3/%s/%d", resource, fileID)
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, &file)
	if err != nil {
		return LibraryFile{}, false, fmt.Errorf("get %s %d: %w", resource, fileID, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return LibraryFile{}, false, nil
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return LibraryFile{}, false, fmt.Errorf("get %s %d: %w", resource, fileID, err)
	}
	libraryFile := LibraryFile{
		ArrFileID:    file.ID,
		Path:         file.Path,
		Size:         file.Size,
		SeriesID:     file.SeriesID,
		SeasonNumber: file.SeasonNumber,
		MovieID:      file.MovieID,
	}
	if a.Type == Sonarr {
		episodesByFile, err := a.sonarrEpisodeIDsByFile(ctx, file.SeriesID)
		if err != nil {
			return LibraryFile{}, false, err
		}
		libraryFile.EpisodeIDs = slices.Clone(episodesByFile[file.ID])
	}
	return libraryFile, true, nil
}

func (a *Arr) DeleteEpisodeFile(ctx context.Context, fileID int) error {
	if err := a.requireType(Sonarr); err != nil {
		return err
	}
	return a.deleteFile(ctx, "episodefile", fileID)
}

func (a *Arr) DeleteMovieFile(ctx context.Context, fileID int) error {
	if err := a.requireType(Radarr); err != nil {
		return err
	}
	return a.deleteFile(ctx, "moviefile", fileID)
}

func (a *Arr) deleteFile(ctx context.Context, resource string, fileID int) error {
	if fileID <= 0 {
		return fmt.Errorf("delete %s: invalid file ID %d", resource, fileID)
	}

	endpoint := fmt.Sprintf("api/v3/%s/%d", resource, fileID)
	resp, err := a.RequestCtx(ctx, http.MethodDelete, endpoint, nil, nil)
	if err != nil {
		return fmt.Errorf("delete %s %d: %w", resource, fileID, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if err := expectSuccess(resp); err != nil {
		return fmt.Errorf("delete %s %d: %w", resource, fileID, err)
	}
	return nil
}

func (a *Arr) SearchEpisodes(ctx context.Context, episodeIDs []int) (Command, error) {
	if err := a.requireType(Sonarr); err != nil {
		return Command{}, err
	}
	ids, err := normalizedIDs(episodeIDs)
	if err != nil {
		return Command{}, fmt.Errorf("episode search: %w", err)
	}
	return a.postCommand(ctx, struct {
		Name       string `json:"name"`
		EpisodeIDs []int  `json:"episodeIds"`
	}{Name: "EpisodeSearch", EpisodeIDs: ids})
}

func (a *Arr) SearchSeason(ctx context.Context, seriesID, seasonNumber int) (Command, error) {
	if err := a.requireType(Sonarr); err != nil {
		return Command{}, err
	}
	if seriesID <= 0 {
		return Command{}, fmt.Errorf("season search: invalid series ID %d", seriesID)
	}
	if seasonNumber < 0 {
		return Command{}, fmt.Errorf("season search: invalid season number %d", seasonNumber)
	}
	return a.postCommand(ctx, struct {
		Name         string `json:"name"`
		SeriesID     int    `json:"seriesId"`
		SeasonNumber int    `json:"seasonNumber"`
	}{Name: "SeasonSearch", SeriesID: seriesID, SeasonNumber: seasonNumber})
}

func (a *Arr) SearchMovies(ctx context.Context, movieIDs []int) (Command, error) {
	if err := a.requireType(Radarr); err != nil {
		return Command{}, err
	}
	ids, err := normalizedIDs(movieIDs)
	if err != nil {
		return Command{}, fmt.Errorf("movie search: %w", err)
	}
	return a.postCommand(ctx, struct {
		Name     string `json:"name"`
		MovieIDs []int  `json:"movieIds"`
	}{Name: "MoviesSearch", MovieIDs: ids})
}

func (a *Arr) Commands(ctx context.Context) ([]Command, error) {
	if a == nil {
		return nil, fmt.Errorf("arr not configured")
	}
	var commands []Command
	resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/command", nil, &commands)
	if err != nil {
		return nil, fmt.Errorf("list arr commands: %w", err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list arr commands: %w", err)
	}
	return commands, nil
}

func (a *Arr) postCommand(ctx context.Context, payload any) (Command, error) {
	var command Command
	resp, err := a.requestMutationCtx(ctx, http.MethodPost, "api/v3/command", payload, &command)
	if err != nil {
		err = fmt.Errorf("submit arr command: %w", err)
		if ambiguousMutationRequest(resp, err) {
			return Command{}, unknownMutationOutcome(err, 0)
		}
		return Command{}, err
	}
	if err := expectSuccess(resp); err != nil {
		return Command{}, fmt.Errorf("submit arr command: %w", err)
	}
	return command, nil
}

func (a *Arr) requireType(want Type) error {
	if a == nil {
		return fmt.Errorf("arr not configured")
	}
	if a.Type != want {
		return fmt.Errorf("arr %q is %s, want %s", a.Name, a.Type, want)
	}
	return nil
}

func normalizedIDs(ids []int) ([]int, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("at least one ID is required")
	}

	result := make([]int, 0, len(ids))
	seen := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return nil, fmt.Errorf("invalid ID %d", id)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	slices.Sort(result)
	return result, nil
}

func expectStatus(resp *http.Response, allowed ...int) error {
	if resp == nil {
		return fmt.Errorf("arr returned no response")
	}
	if slices.Contains(allowed, resp.StatusCode) {
		return nil
	}
	return fmt.Errorf("arr returned %s", resp.Status)
}

func expectSuccess(resp *http.Response) error {
	if resp == nil {
		return fmt.Errorf("arr returned no response")
	}
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	return fmt.Errorf("arr returned %s", resp.Status)
}
