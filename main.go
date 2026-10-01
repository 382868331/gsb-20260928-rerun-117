package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dataPath := flag.String("data", "data/state.json", "path to the durable state file")
	slots := flag.Int("slots", 2, "total capacity units available for running functions")
	queueSize := flag.Int("queue", 4, "maximum number of requests waiting for a slot")
	flag.Parse()

	store, err := OpenStore(*dataPath)
	if err != nil {
		log.Fatalf("open state: %v", err)
	}

	registry := DefaultRegistry()
	manager, err := NewManager(store, registry, *slots, *queueSize)
	if err != nil {
		log.Fatalf("initialize execution manager: %v", err)
	}
	manager.Start()

	server := &http.Server{
		Addr:              *addr,
		Handler:           NewRouter(manager),
		ReadHeaderTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}()

	log.Printf("local function platform listening on %s", *addr)
	err = server.ListenAndServe()
	manager.Stop()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}
