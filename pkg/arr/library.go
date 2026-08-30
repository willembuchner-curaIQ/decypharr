package arr

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"slices"

	"golang.org/x/sync/errgroup"
)

const sonarrLibraryConcurrency = 4

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

type sonarrSeries struct {
	ID int `json:"id"`
}

type sonarrEpisodeFile struct {
	ID           int    `json:"id"`
	SeriesID     int    `json:"seriesId"`
	SeasonNumber int    `json:"seasonNumber"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
}

type sonarrEpisode struct {
	ID            int `json:"id"`
	SeriesID      int `json:"seriesId"`
	EpisodeFileID int `json:"episodeFileId"`
}

type radarrMovie struct {
	ID        int `json:"id"`
	MovieFile struct {
		ID      int    `json:"id"`
		MovieID int    `json:"movieId"`
		Path    string `json:"path"`
		Size    int64  `json:"size"`
	} `json:"movieFile"`
}

// LibraryFiles enumerates every imported file an instance owns.
func (s *Service) LibraryFiles(ctx context.Context, name string) ([]LibraryFile, error) {
	instance, err := s.instance(name)
	if err != nil {
		return nil, err
	}
	switch instance.Type {
	case Sonarr:
		return s.sonarrLibraryFiles(ctx, instance)
	case Radarr:
		return s.radarrLibraryFiles(ctx, instance)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, instance.Type)
	}
}

// LibraryFilesForHistory enumerates only the series or movies the given history
// records touch, so a targeted index does not walk the whole library.
func (s *Service) LibraryFilesForHistory(ctx context.Context, name string, records []HistoryRecord) ([]LibraryFile, error) {
	instance, err := s.instance(name)
	if err != nil {
		return nil, err
	}

	switch instance.Type {
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
			seriesID, err := s.sonarrSeriesForEpisode(ctx, instance, episodeID)
			if err != nil {
				return nil, err
			}
			if seriesID > 0 {
				seriesIDs[seriesID] = struct{}{}
			}
		}
		files := make([]LibraryFile, 0, len(seriesIDs))
		for seriesID := range seriesIDs {
			seriesFiles, err := s.sonarrSeriesFiles(ctx, instance, seriesID)
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
			file, found, err := s.radarrMovieFile(ctx, instance, movieID)
			if err != nil {
				return nil, err
			}
			if found {
				files = append(files, file)
			}
		}
		return files, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedType, instance.Type)
	}
}

func (s *Service) sonarrLibraryFiles(ctx context.Context, instance Arr) ([]LibraryFile, error) {
	var series []sonarrSeries
	resp, err := s.get(ctx, instance, "api/v3/series", &series)
	if err != nil {
		return nil, fmt.Errorf("list sonarr series: %w", err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list sonarr series: %w", err)
	}
	if len(series) == 0 {
		return nil, nil
	}

	bySeries := make([][]LibraryFile, len(series))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(sonarrLibraryConcurrency)
	for i, item := range series {
		group.Go(func() error {
			files, err := s.sonarrSeriesFiles(groupCtx, instance, item.ID)
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
	return slices.Concat(bySeries...), nil
}

func (s *Service) sonarrSeriesFiles(ctx context.Context, instance Arr, seriesID int) ([]LibraryFile, error) {
	var episodeFiles []sonarrEpisodeFile
	endpoint := fmt.Sprintf("api/v3/episodefile?seriesId=%d", seriesID)
	resp, err := s.get(ctx, instance, endpoint, &episodeFiles)
	if err != nil {
		return nil, fmt.Errorf("list episode files for series %d: %w", seriesID, err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list episode files for series %d: %w", seriesID, err)
	}

	episodesByFile, err := s.sonarrEpisodeIDs(ctx, instance, seriesID)
	if err != nil {
		return nil, err
	}

	files := make([]LibraryFile, 0, len(episodeFiles))
	for _, file := range episodeFiles {
		if file.ID <= 0 || file.Path == "" {
			continue
		}
		files = append(files, LibraryFile{
			ArrFileID:    file.ID,
			Path:         file.Path,
			Size:         file.Size,
			SeriesID:     cmp.Or(file.SeriesID, seriesID),
			SeasonNumber: file.SeasonNumber,
			EpisodeIDs:   episodesByFile[file.ID],
		})
	}
	return files, nil
}

func (s *Service) sonarrEpisodeIDs(ctx context.Context, instance Arr, seriesID int) (map[int][]int, error) {
	var episodes []sonarrEpisode
	endpoint := fmt.Sprintf("api/v3/episode?seriesId=%d", seriesID)
	resp, err := s.get(ctx, instance, endpoint, &episodes)
	if err != nil {
		return nil, fmt.Errorf("list episodes for series %d: %w", seriesID, err)
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("list episodes for series %d: %w", seriesID, err)
	}

	byFile := make(map[int][]int)
	for _, episode := range episodes {
		if episode.ID <= 0 || episode.EpisodeFileID <= 0 {
			continue
		}
		byFile[episode.EpisodeFileID] = append(byFile[episode.EpisodeFileID], episode.ID)
	}
	for fileID, ids := range byFile {
		slices.Sort(ids)
		byFile[fileID] = slices.Compact(ids)
	}
	return byFile, nil
}

func (s *Service) sonarrSeriesForEpisode(ctx context.Context, instance Arr, episodeID int) (int, error) {
	var episode sonarrEpisode
	endpoint := fmt.Sprintf("api/v3/episode/%d", episodeID)
	resp, err := s.get(ctx, instance, endpoint, &episode)
	if err != nil {
		return 0, fmt.Errorf("get sonarr episode %d: %w", episodeID, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return 0, fmt.Errorf("get sonarr episode %d: %w", episodeID, err)
	}
	return episode.SeriesID, nil
}

func (s *Service) radarrLibraryFiles(ctx context.Context, instance Arr) ([]LibraryFile, error) {
	var movies []radarrMovie
	resp, err := s.get(ctx, instance, "api/v3/movie", &movies)
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
		files = append(files, LibraryFile{
			ArrFileID: movie.MovieFile.ID,
			Path:      movie.MovieFile.Path,
			Size:      movie.MovieFile.Size,
			MovieID:   cmp.Or(movie.MovieFile.MovieID, movie.ID),
		})
	}
	return files, nil
}

func (s *Service) radarrMovieFile(ctx context.Context, instance Arr, movieID int) (LibraryFile, bool, error) {
	var movie radarrMovie
	endpoint := fmt.Sprintf("api/v3/movie/%d", movieID)
	resp, err := s.get(ctx, instance, endpoint, &movie)
	if err != nil {
		return LibraryFile{}, false, fmt.Errorf("get radarr movie %d: %w", movieID, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return LibraryFile{}, false, nil
	}
	if err := expectStatus(resp, http.StatusOK); err != nil {
		return LibraryFile{}, false, fmt.Errorf("get radarr movie %d: %w", movieID, err)
	}
	if movie.MovieFile.ID <= 0 || movie.MovieFile.Path == "" {
		return LibraryFile{}, false, nil
	}
	return LibraryFile{
		ArrFileID: movie.MovieFile.ID,
		Path:      movie.MovieFile.Path,
		Size:      movie.MovieFile.Size,
		MovieID:   cmp.Or(movie.MovieFile.MovieID, movie.ID),
	}, true, nil
}
