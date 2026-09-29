package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPSmoke(t *testing.T) {
	st, _ := OpenStore("")
	reg := DefaultRegistry()
	ex := NewExecutor(Config{MaxSlots: 1, MaxQueue: 1, ExecTimeout: 200 * time.Millisecond}, reg, st)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ex.Shutdown(ctx)
	}()
	h := NewServer(ex, reg).Handler()

	do := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	// Unknown function -> 404
	if rr := do("POST", "/invoke", `{"function":"evil"}`); rr.Code != http.StatusNotFound {
		t.Fatalf("unknown fn: got %d", rr.Code)
	}

	// Happy path -> 202 then succeeded result
	rr := do("POST", "/invoke", `{"function":"add","args":{"a":1,"b":2}}`)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("invoke: got %d", rr.Code)
	}
	var inv struct{ ID string }
	json.Unmarshal(rr.Body.Bytes(), &inv)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rr = do("GET", "/result/"+inv.ID, "")
		var rec Record
		json.Unmarshal(rr.Body.Bytes(), &rec)
		if rec.Status == StatusSucceeded {
			if rec.Result != float64(3) {
				t.Fatalf("bad result: %v", rec.Result)
			}
			goto ok
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("never succeeded")
ok:

	// Timeout -> record shows timeout, not success
	rr = do("POST", "/invoke", `{"function":"sleep","args":{"ms":5000}}`)
	json.Unmarshal(rr.Body.Bytes(), &inv)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rr = do("GET", "/result/"+inv.ID, "")
		var rec Record
		json.Unmarshal(rr.Body.Bytes(), &rec)
		if rec.Status == StatusTimeout {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("never timed out")
}

func TestHTTPQueueFull(t *testing.T) {
	st, _ := OpenStore("")
	reg := DefaultRegistry()
	ex := NewExecutor(Config{MaxSlots: 1, MaxQueue: 1, ExecTimeout: 5 * time.Second}, reg, st)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ex.Shutdown(ctx)
	}()
	h := NewServer(ex, reg).Handler()
	do := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	do("POST", "/invoke", `{"function":"sleep","args":{"ms":2000}}`)
	time.Sleep(50 * time.Millisecond) // let it start running
	do("POST", "/invoke", `{"function":"sleep","args":{"ms":2000}}`)
	if rr := do("POST", "/invoke", `{"function":"sleep","args":{"ms":2000}}`); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("queue full: got %d", rr.Code)
	}
}
