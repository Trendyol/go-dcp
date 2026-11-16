package logger

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestJSONShape(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{z: newZerolog(&buf, zerolog.InfoLevel)}
	l.Info("connected", "hosts", "localhost", "bucket", "beer")
	l.Debug("hidden")
	l.Trace("also hidden")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d lines: %s", len(lines), buf.String())
	}

	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["message"] != "connected" {
		t.Fatalf("message: %v", rec["message"])
	}
	if rec["level"] != INFO {
		t.Fatalf("level: %v", rec["level"])
	}
	if rec["hosts"] != "localhost" || rec["bucket"] != "beer" {
		t.Fatalf("attrs: %v", rec)
	}
	if _, ok := rec["time"]; !ok {
		t.Fatalf("missing time: %v", rec)
	}
}

func TestTraceLevel(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{z: newZerolog(&buf, zerolog.TraceLevel)}
	l.Trace("observing vb", "vbID", uint16(3))

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["level"] != TRACE {
		t.Fatalf("level: %v", rec["level"])
	}
	if rec["message"] != "observing vb" {
		t.Fatalf("message: %v", rec["message"])
	}
	if rec["vbID"] != float64(3) {
		t.Fatalf("vbID: %v", rec["vbID"])
	}
}

func TestParseLevel(t *testing.T) {
	level, err := ParseLevel(" WARN ")
	if err != nil {
		t.Fatal(err)
	}
	if level != zerolog.WarnLevel {
		t.Fatalf("level: %v", level)
	}

	if _, err = ParseLevel("verbose"); err == nil {
		t.Fatal("expected error")
	}
}

func TestErrorAttr(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{z: newZerolog(&buf, zerolog.ErrorLevel)}
	l.Error("error while connect", "error", errString("dial failed"))

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["error"] != "dial failed" {
		t.Fatalf("error: %v", rec["error"])
	}
	if rec["level"] != ERROR {
		t.Fatalf("level: %v", rec["level"])
	}
}

type errString string

func (e errString) Error() string { return string(e) }
