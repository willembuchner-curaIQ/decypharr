package qbit

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	json "github.com/bytedance/sonic"
	"github.com/sirrobot01/decypharr/internal/customerror"
)

func TestWriteTorrentAddErrorPreservesSemantics(t *testing.T) {
	tests := map[string]struct {
		err       error
		status    int
		code      string
		retryable bool
		permanent bool
	}{
		"blocked": {
			err:       fmt.Errorf("submit: %w", customerror.TorrentBlockedError),
			status:    http.StatusUnavailableForLegalReasons,
			code:      "torrent_blocked",
			permanent: true,
		},
		"not cached": {
			err:       fmt.Errorf("status: %w", customerror.TorrentNotCachedError),
			status:    http.StatusNotFound,
			code:      "torrent_not_cached",
			retryable: true,
		},
		"invalid input": {
			err:       customerror.NewError(errors.New("bad magnet"), http.StatusBadRequest, "invalid_magnet", false, false).Permanent(),
			status:    http.StatusBadRequest,
			code:      "invalid_magnet",
			permanent: true,
		},
		"provider failure": {
			err:       errors.New("connection reset"),
			status:    http.StatusBadGateway,
			code:      "provider_error",
			retryable: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeTorrentAddError(recorder, tt.err)

			if recorder.Code != tt.status {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.status)
			}
			if got := recorder.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type = %q, want application/json", got)
			}

			var response torrentAddErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response.Code != tt.code || response.Retryable != tt.retryable || response.Permanent != tt.permanent {
				t.Fatalf("response = %#v, want code=%q retryable=%t permanent=%t", response, tt.code, tt.retryable, tt.permanent)
			}
		})
	}
}
