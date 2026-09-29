package link

import (
	"strconv"
	"testing"
	"time"

	"edge-ingest/internal/device"
	"edge-ingest/internal/ingest"
	"edge-ingest/internal/store"
)

func TestDisconnectBackfillAndDeduplicate(t *testing.T) {
	storage := openStore(t)
	service := ingest.NewService(storage)
	link := New(service)
	sensor, err := device.New("temperature", "hw-temp-1", "room-a")
	if err != nil {
		t.Fatal(err)
	}

	link.Connected = false
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	first, err := sensor.Frame(21.1, base)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Send(sensor, first, base.Add(2*time.Minute)); err != nil {
		t.Fatal("message while disconnected must not be acknowledged as sent", err)
	}
	second, err := sensor.Frame(21.2, base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Send(sensor, second, base.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := link.PendingCount(); got != 2 {
		t.Fatalf("pending count = %d, want 2", got)
	}

	flushed, err := link.Reconnect(base.Add(4 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if flushed != 2 || link.PendingCount() != 0 {
		t.Fatalf("flushed = %d pending = %d, want 2 and 0", flushed, link.PendingCount())
	}
	if observations := storage.Observations(); len(observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(observations))
	}

	result, _, err := service.Accept("temperature", sensor.BootID, 1, first, base.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Duplicate {
		t.Fatal("same boot and sequence must be deduplicated")
	}
}

func TestRestartAndIdentityReplacementDoNotDeleteData(t *testing.T) {
	storage := openStore(t)
	service := ingest.NewService(storage)
	link := New(service)
	sensor, err := device.New("temperature", "hw-temp-2", "room-old")
	if err != nil {
		t.Fatal(err)
	}
	oldBoot := sensor.BootID
	at := time.Date(2026, 9, 29, 11, 0, 0, 0, time.UTC)
	oldPayload, err := sensor.Frame(19.5, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Send(sensor, oldPayload, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	if err := sensor.ReplaceIdentity("room-new"); err != nil {
		t.Fatal(err)
	}
	if sensor.BootID == oldBoot {
		t.Fatal("restart must create a new boot sequence")
	}
	newPayload, err := sensor.Frame(20.5, at.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if sensor.Sequence != 1 {
		t.Fatalf("new boot sequence = %d, want 1", sensor.Sequence)
	}
	if err := link.Send(sensor, newPayload, at.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if got := len(storage.RawMessages()); got != 2 {
		t.Fatalf("raw messages = %d, want 2", got)
	}
	if got := len(storage.Observations()); got != 2 {
		t.Fatalf("observations = %d, want 2; replacement must retain history", got)
	}
	binding, ok := storage.CurrentBinding("hw-temp-2")
	if !ok || binding.DeviceID != "room-new" {
		t.Fatalf("current binding = %#v, want room-new", binding)
	}
}

func TestOutOfOrderPreservesObservedAndReceivedTimes(t *testing.T) {
	storage := openStore(t)
	service := ingest.NewService(storage)
	link := New(service)
	sensor, err := device.New("vibration", "hw-vib-1", "motor-a")
	if err != nil {
		t.Fatal(err)
	}
	lateObserved := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	earlyObserved := lateObserved.Add(-time.Minute)
	latePayload, err := sensor.Frame(3.2, lateObserved)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Send(sensor, latePayload, lateObserved.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	earlyPayload, err := sensor.Frame(2.9, earlyObserved)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Send(sensor, earlyPayload, lateObserved.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	observations := storage.Observations()
	if len(observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(observations))
	}
	byID := map[string]time.Time{}
	received := map[string]time.Time{}
	for _, observation := range observations {
		byID[observation.ID] = observation.ObservedAt
		received[observation.ID] = observation.ReceivedAt
	}
	id := "vibration:hw-vib-1:"
	firstID := id + strconv.FormatInt(lateObserved.UnixMilli(), 10)
	secondID := id + strconv.FormatInt(earlyObserved.UnixMilli(), 10)
	if !byID[firstID].Equal(lateObserved) || !received[firstID].Equal(lateObserved.Add(2*time.Minute)) {
		t.Fatalf("first time pair not preserved: %v %v", byID[firstID], received[firstID])
	}
	if !byID[secondID].Equal(earlyObserved) || !received[secondID].Equal(lateObserved.Add(3*time.Minute)) {
		t.Fatalf("second time pair not preserved: %v %v", byID[secondID], received[secondID])
	}
}

func TestStorageFailureIsNotAcknowledged(t *testing.T) {
	storage := openStore(t)
	service := ingest.NewService(storage)
	link := New(service)
	sensor, err := device.New("temperature", "hw-temp-3", "room-c")
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 29, 13, 0, 0, 0, time.UTC)
	payload, err := sensor.Frame(18, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Send(sensor, payload, at.Add(time.Second)); err == nil {
		t.Fatal("failed storage must return an error instead of acknowledging receipt")
	}
	if link.PendingCount() != 1 {
		t.Fatalf("pending after storage failure = %d, want 1", link.PendingCount())
	}
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	storage, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	return storage
}
