package main

import (
	"bytes"
	"net/http"
)

func jsonBody(value string) *bytes.Reader {
	return bytes.NewReader([]byte(value))
}

var _ = http.MethodPost
