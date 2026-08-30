package arr

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"golang.org/x/sync/errgroup"
)

// LibraryFile is the Arr-side identity of an imported media file.
type LibraryFile struct {
	ArrFileID    int    `json:"arr_file_id"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
	SeriesID     int    `json:"series_id,omitempty"`
	SeasonNumber int    `json:"season_number,omitempty"`
	EpisodeIDs   []int  `json:"episode_ids,omitempty"`
	MovieID      int    `json:"movie_id,omitempty"`
}

const sonarrLibraryConcurrency = 4

type sonarrSeriesSummary struct {
	ID int `json:"id"`
}

type sonarrLibraryFile struct {
	ID           int    `json:"id"`
	SeriesID     int    `json:"seriesId"`
	SeasonNumber int    `json:"seasonNumber"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
}

type sonarrEpisodeSummary struct {
	ID            int `json:"id"`
	SeriesID      int `json:"seriesId"`
	EpisodeFileID int `json:"episodeFileId"`
}

type radarrMovieSummary struct {
	ID        int `json:"id"`
	MovieFile struct {
		ID      int    `json:"id"`
		MovieID int    `json:"movieId"`
		Path    string `json:"path"`
		Size    int64  `json:"size"`
	} `json:"movieFile"`
}

func (a *Arr) ListLibraryFiles(ctx context.Context) ([]LibraryFile, error) {
	if a == nil {
		return nil, fmt.Errorf("arr not configured")
	}

	switch a.Type {
	case Sonarr:
		return a.listSonarrLibraryFiles(ctx)
	case Radarr:
		return a.listRadarrLibraryFiles(ctx)
	default:
		return nil, fmt.Errorf("list library files: unsupported arr type %q", a.Type)
	}
}

func (a *Arr) listSonarrLibraryFiles(ctx context.Context) ([]LibraryFile, error) {
	var series []sonarrSeriesSummary
	resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/series", nil, &series)
	if err != nil {
		return nil, fmt.Errorf("list sonarr series: %w", err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list sonarr series: %w", err)
	}
	if len(series) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	bySeries := make([][]LibraryFile, len(series))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(sonarrLibraryConcurrency)
	for i, item := range series {
		group.Go(func() error {
			files, err := a.listSonarrSeriesFiles(groupCtx, item.ID)
			if err != nil {
				return err
			}
			bySeries[i] = files
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	files := make([]LibraryFile, 0)
	for _, seriesFiles := range bySeries {
		files = append(files, seriesFiles...)
	}
	return files, nil
}

func (a *Arr) listSonarrSeriesFiles(ctx context.Context, seriesID int) ([]LibraryFile, error) {
	var files []sonarrLibraryFile
	endpoint := fmt.Sprintf("api/v3/episodefile?seriesId=%d", seriesID)
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, &files)
	if err != nil {
		return nil, fmt.Errorf("list episode files for series %d: %w", seriesID, err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list episode files for series %d: %w", seriesID, err)
	}

	episodesByFile, err := a.sonarrEpisodeIDsByFile(ctx, seriesID)
	if err != nil {
		return nil, err
	}

	result := make([]LibraryFile, 0, len(files))
	for _, file := range files {
		if file.ID <= 0 || file.Path == "" {
			continue
		}
		episodeIDs := episodesByFile[file.ID]
		slices.Sort(episodeIDs)
		episodeIDs = slices.Compact(episodeIDs)
		fileSeriesID := file.SeriesID
		if fileSeriesID == 0 {
			fileSeriesID = seriesID
		}
		result = append(result, LibraryFile{
			ArrFileID:    file.ID,
			Path:         file.Path,
			Size:         file.Size,
			SeriesID:     fileSeriesID,
			SeasonNumber: file.SeasonNumber,
			EpisodeIDs:   slices.Clone(episodeIDs),
		})
	}
	return result, nil
}

func (a *Arr) sonarrEpisodeIDsByFile(ctx context.Context, seriesID int) (map[int][]int, error) {
	var episodes []sonarrEpisodeSummary
	endpoint := fmt.Sprintf("api/v3/episode?seriesId=%d", seriesID)
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, &episodes)
	if err != nil {
		return nil, fmt.Errorf("list episodes for series %d: %w", seriesID, err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list episodes for series %d: %w", seriesID, err)
	}

	episodesByFile := make(map[int][]int)
	for _, episode := range episodes {
		if episode.ID <= 0 || episode.EpisodeFileID <= 0 {
			continue
		}
		episodesByFile[episode.EpisodeFileID] = append(episodesByFile[episode.EpisodeFileID], episode.ID)
	}
	for fileID, episodeIDs := range episodesByFile {
		slices.Sort(episodeIDs)
		episodesByFile[fileID] = slices.Compact(episodeIDs)
	}
	return episodesByFile, nil
}

func (a *Arr) listRadarrLibraryFiles(ctx context.Context) ([]LibraryFile, error) {
	var movies []radarrMovieSummary
	resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/movie", nil, &movies)
	if err != nil {
		return nil, fmt.Errorf("list radarr movies: %w", err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list radarr movies: %w", err)
	}

	files := make([]LibraryFile, 0, len(movies))
	for _, movie := range movies {
		if movie.MovieFile.ID <= 0 || movie.MovieFile.Path == "" {
			continue
		}
		movieID := movie.MovieFile.MovieID
		if movieID == 0 {
			movieID = movie.ID
		}
		files = append(files, LibraryFile{
			ArrFileID: movie.MovieFile.ID,
			Path:      movie.MovieFile.Path,
			Size:      movie.MovieFile.Size,
			MovieID:   movieID,
		})
	}
	return files, nil
}

func (a *Arr) ListTargetLibraryFiles(ctx context.Context, records []HistoryRecord) ([]LibraryFile, error) {
	switch a.Type {
	case Sonarr:
		seriesIDs := make(map[int]struct{})
		episodeIDs := make(map[int]struct{})
		for _, record := range records {
			if record.SeriesID > 0 {
				seriesIDs[record.SeriesID] = struct{}{}
			} else if record.EpisodeID > 0 {
				episodeIDs[record.EpisodeID] = struct{}{}
			}
		}
		for episodeID := range episodeIDs {
			seriesID, err := a.sonarrSeriesIDForEpisode(ctx, episodeID)
			if err != nil {
				return nil, err
			}
			if seriesID > 0 {
				seriesIDs[seriesID] = struct{}{}
			}
		}
		files := make([]LibraryFile, 0)
		for seriesID := range seriesIDs {
			seriesFiles, err := a.listSonarrSeriesFiles(ctx, seriesID)
			if err != nil {
				return nil, err
			}
			files = append(files, seriesFiles...)
		}
		return files, nil
	case Radarr:
		movieIDs := make(map[int]struct{})
		for _, record := range records {
			if record.MovieID > 0 {
				movieIDs[record.MovieID] = struct{}{}
			}
		}
		files := make([]LibraryFile, 0, len(movieIDs))
		for movieID := range movieIDs {
			file, found, err := a.radarrMovieFile(ctx, movieID)
			if err != nil {
				return nil, err
			}
			if found {
				files = append(files, file)
			}
		}
		return files, nil
	default:
		return nil, fmt.Errorf("list targeted library files: unsupported arr type %q", a.Type)
	}
}

func (a *Arr) sonarrSeriesIDForEpisode(ctx context.Context, episodeID int) (int, error) {
	var episode sonarrEpisodeSummary
	endpoint := fmt.Sprintf("api/v3/episode/%d", episodeID)
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, &episode)
	if err != nil {
		return 0, fmt.Errorf("get Sonarr episode %d: %w", episodeID, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return 0, fmt.Errorf("get Sonarr episode %d: %w", episodeID, err)
	}
	return episode.SeriesID, nil
}

func (a *Arr) radarrMovieFile(ctx context.Context, movieID int) (LibraryFile, bool, error) {
	var movie radarrMovieSummary
	endpoint := fmt.Sprintf("api/v3/movie/%d", movieID)
	resp, err := a.RequestCtx(ctx, http.MethodGet, endpoint, nil, &movie)
	if err != nil {
		return LibraryFile{}, false, fmt.Errorf("get Radarr movie %d: %w", movieID, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return LibraryFile{}, false, nil
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return LibraryFile{}, false, fmt.Errorf("get Radarr movie %d: %w", movieID, err)
	}
	if movie.MovieFile.ID <= 0 || movie.MovieFile.Path == "" {
		return LibraryFile{}, false, nil
	}
	fileMovieID := movie.MovieFile.MovieID
	if fileMovieID == 0 {
		fileMovieID = movie.ID
	}
	return LibraryFile{
		ArrFileID: movie.MovieFile.ID,
		Path:      movie.MovieFile.Path,
		Size:      movie.MovieFile.Size,
		MovieID:   fileMovieID,
	}, true, nil
}
