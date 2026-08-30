package server

import (
	"errors"
	"net/http"
	"strings"

	json "github.com/bytedance/sonic"

	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr/reacquire"
)

func (s *Server) handleListArrReacquireJobs(w http.ResponseWriter, _ *http.Request) {
	service := s.manager.ArrService()
	if service == nil {
		s.sendJSONError(w, "Arr reacquisition service is not available", http.StatusServiceUnavailable)
		return
	}
	utils.JSONResponse(w, service.Jobs(), http.StatusOK)
}

func (s *Server) handleGetArrReacquireJob(w http.ResponseWriter, r *http.Request) {
	service := s.manager.ArrService()
	if service == nil {
		s.sendJSONError(w, "Arr reacquisition service is not available", http.StatusServiceUnavailable)
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		s.sendJSONError(w, "Reacquisition job ID is required", http.StatusBadRequest)
		return
	}
	job, ok := service.Job(id)
	if !ok {
		s.sendJSONError(w, "Reacquisition job not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, job, http.StatusOK)
}

func (s *Server) handleArrReacquire(w http.ResponseWriter, r *http.Request) {
	service := s.manager.ArrService()
	if service == nil {
		s.sendJSONError(w, "Arr reacquisition service is not available", http.StatusServiceUnavailable)
		return
	}

	var request reacquire.Request
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&request); err != nil {
		s.sendJSONError(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if request.Cause == "" {
		request.Cause = reacquire.CauseManual
	}
	request.EntryID = strings.TrimSpace(request.EntryID)
	request.FileID = strings.TrimSpace(request.FileID)
	if message := validateArrReacquireRequest(request); message != "" {
		s.sendJSONError(w, message, http.StatusBadRequest)
		return
	}

	job, err := service.Reacquire(request)
	if err != nil {
		s.handleArrReacquireError(w, err)
		return
	}
	utils.JSONResponse(w, job, http.StatusAccepted)
}

func (s *Server) handleRefreshArrIndex(w http.ResponseWriter, _ *http.Request) {
	if s.manager.ArrService() == nil {
		s.sendJSONError(w, "Arr reacquisition service is not available", http.StatusServiceUnavailable)
		return
	}
	if !s.manager.RefreshArrIndex() {
		s.sendJSONError(w, "Arr index refresh could not be queued", http.StatusServiceUnavailable)
		return
	}
	utils.JSONResponse(w, map[string]string{"status": "queued"}, http.StatusAccepted)
}

func (s *Server) handleArrReacquireError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, reacquire.ErrBindingNotFound):
		s.sendJSONError(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, reacquire.ErrBindingUnsafe):
		s.sendJSONError(w, err.Error(), http.StatusConflict)
	case errors.Is(err, reacquire.ErrServiceNotStarted), errors.Is(err, reacquire.ErrServiceClosed):
		s.sendJSONError(w, err.Error(), http.StatusServiceUnavailable)
	default:
		s.logger.Error().Err(err).Msg("Failed to queue Arr reacquisition")
		s.sendJSONError(w, "Failed to queue Arr reacquisition", http.StatusInternalServerError)
	}
}

func validateArrReacquireRequest(request reacquire.Request) string {
	switch {
	case request.EntryID == "":
		return "entryId is required"
	case request.FileID == "":
		return "fileId is required"
	case request.Cause != reacquire.CauseStream && request.Cause != reacquire.CauseRepair && request.Cause != reacquire.CauseManual:
		return "cause must be stream, repair, or manual"
	case request.Strategy != "" && request.Strategy != reacquire.StrategyHistoryFailed && request.Strategy != reacquire.StrategyInteractiveBest && request.Strategy != reacquire.StrategyCommandSearch:
		return "strategy must be history_failed, interactive_best, or command_search"
	default:
		return ""
	}
}
