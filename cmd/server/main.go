package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fnexec/internal/executor"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	storePath := flag.String("store", "data/records.jsonl", "result store file")
	slots := flag.Int("slots", 4, "max concurrent executions")
	queue := flag.Int("queue", 16, "max queued requests")
	timeout := flag.Duration("timeout", 30*time.Second, "per-invocation timeout")
	flag.Parse()

	st, err := executor.OpenStore(*storePath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	reg := executor.DefaultRegistry()
	ex := executor.NewExecutor(executor.Config{
		MaxSlots:    *slots,
		MaxQueue:    *queue,
		ExecTimeout: *timeout,
	}, reg, st)

	srv := &http.Server{Addr: *addr, Handler: executor.NewServer(ex, reg).Handler()}

	go func() {
		log.Printf("listening on %s (slots=%d queue=%d timeout=%s)", *addr, *slots, *queue, *timeout)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = ex.Shutdown(ctx)
	log.Println("stopped")
}
