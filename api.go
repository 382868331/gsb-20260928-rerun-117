package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

func NewRouter(manager *Manager) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/functions", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string][]string{"functions": manager.registry.Names()})
	})
	mux.HandleFunc("GET /v1/capacity", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, manager.Capacity())
	})
	mux.HandleFunc("PUT /v1/capacity", func(w http.ResponseWriter, r *http.Request) {
		var request CapacityRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body"})
			return
		}
		if err := manager.SetCapacity(request.Capacity); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, manager.Capacity())
	})
	mux.HandleFunc("POST /v1/invocations", func(w http.ResponseWriter, r *http.Request) {
		var request InvokeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body"})
			return
		}
		record, err := manager.Submit(request)
		if errors.Is(err, ErrQueueFull) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(struct {
				Invocation Invocation `json:"invocation"`
				Error      string     `json:"error"`
			}{Invocation: record, Error: err.Error()})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, InvokeResponse{ID: record.ID, Status: record.Status})
	})
	mux.HandleFunc("GET /v1/invocations/{id}", func(w http.ResponseWriter, r *http.Request) {
		record, ok := manager.Get(r.PathValue("id"))
		if !ok {
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "invocation not found"})
			return
		}
		writeJSON(w, http.StatusOK, record)
	})
	mux.HandleFunc("POST /v1/invocations/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		record, ok, err := manager.Cancel(r.PathValue("id"))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}
		if !ok {
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "invocation not found"})
			return
		}
		writeJSON(w, http.StatusOK, record)
	})
	mux.HandleFunc("PUT /v1/invocations/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		var request ReplaceResultRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body"})
			return
		}
		record, ok, err := manager.ReplaceResult(r.PathValue("id"), request.Result)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
			return
		}
		if !ok {
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "invocation not found"})
			return
		}
		writeJSON(w, http.StatusOK, record)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/") && r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
