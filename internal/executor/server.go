package executor

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Server exposes the executor over HTTP.
type Server struct {
	ex  *Executor
	reg *Registry
	mux *http.ServeMux
}

func NewServer(ex *Executor, reg *Registry) *Server {
	s := &Server{ex: ex, reg: reg, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /invoke", s.handleInvoke)
	s.mux.HandleFunc("GET /result/{id}", s.handleResult)
	s.mux.HandleFunc("POST /cancel/{id}", s.handleCancel)
	s.mux.HandleFunc("GET /functions", s.handleFunctions)
	s.mux.HandleFunc("GET /records", s.handleRecords)
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

type invokeRequest struct {
	Function string          `json:"function"`
	Args     json.RawMessage `json:"args"`
}

type invokeResponse struct {
	ID string `json:"id"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) handleInvoke(w http.ResponseWriter, r *http.Request) {
	var req invokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Function == "" {
		writeErr(w, http.StatusBadRequest, "function is required")
		return
	}
	id, err := s.ex.Submit(req.Function, req.Args)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, invokeResponse{ID: id})
	case errors.Is(err, ErrUnknownFunction):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrQueueFull):
		writeErr(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, ErrShuttingDown):
		writeErr(w, http.StatusServiceUnavailable, err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, err.Error())
	}
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	rec, ok := s.ex.Get(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown id")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if s.ex.Cancel(r.PathValue("id")) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "cancellation requested"})
		return
	}
	writeErr(w, http.StatusConflict, "not cancellable (finished or unknown id)")
}

func (s *Server) handleFunctions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"functions": s.reg.Names()})
}

func (s *Server) handleRecords(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"records": s.ex.st.List()})
}
