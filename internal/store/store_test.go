package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"edge-ingest/internal/model"
)

func TestRecoveryIgnoresUncommittedRecord(t *testing.T) {
	dir := t.TempDir()
	storage, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	envelope := validEnvelope("hw-1", "dev-1", "boot-1", 1, "message-1", "obs-1")
	if err := storage.writeRecord(journalRecord{Type: recordPrepare, Txn: "uncommitted", Envelope: &envelope}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(storage.Observations()); got != 0 {
		t.Fatalf("observations from half record = %d, want 0", got)
	}
	if _, err := storage.Ingest(envelope); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	storage, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(storage.Observations()); got != 1 {
		t.Fatalf("committed observations after recovery = %d, want 1", got)
	}
}

func TestRecoveryRemovesCorruptTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ingest.log")
	if err := os.WriteFile(path, []byte("not-a-journal"), 0o644); err != nil {
		t.Fatal(err)
	}
	storage, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(storage.Observations()); got != 0 {
		t.Fatalf("corrupt observations = %d, want 0", got)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSameObservationFromNewDeliveryIsDuplicate(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	first := validEnvelope("hw-2", "dev-2", "boot-a", 1, "message-a", "same-observation")
	second := validEnvelope("hw-2", "dev-2", "boot-b", 1, "message-b", "same-observation")
	result, err := storage.Ingest(first)
	if err != nil || !result.Stored {
		t.Fatalf("first ingest result = %#v, %v", result, err)
	}
	result, err = storage.Ingest(second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Duplicate {
		t.Fatal("same normalized observation must be idempotent")
	}
	if got := len(storage.RawMessages()); got != 1 {
		t.Fatalf("duplicate created raw message, count = %d", got)
	}
}

func validEnvelope(hardwareID, deviceID, bootID string, sequence int64, messageID, observationID string) model.Envelope {
	at := time.Date(2026, 9, 29, 14, 0, 0, 0, time.UTC)
	message := model.RawMessage{
		MessageID:  messageID,
		HardwareID: hardwareID,
		ClaimedID:  deviceID,
		BootID:     bootID,
		Sequence:   sequence,
		Source:     "test",
		Payload:    []byte(`{}`),
		ReceivedAt: at,
	}
	return model.Envelope{
		Message: message,
		Reading: model.Observation{
			ID:         observationID,
			HardwareID: hardwareID,
			DeviceID:   deviceID,
			Metric:     "test",
			Value:      1,
			ObservedAt: at.Add(-time.Minute),
			ReceivedAt: at,
			MessageID:  messageID,
		},
	}
}
