package hub_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/device"
	"edge-ingest/internal/hub"
	"edge-ingest/internal/store"
)

func newHub(t *testing.T) (*hub.Hub, *store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	adapters := adapter.NewRegistry(device.TemperatureAdapter{}, device.VibrationAdapter{})
	return hub.New(s, adapters), s, dir
}

func TestBackfillDeduplicateAndTimestamps(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 9, 29, 8, 0, 0, 0, time.UTC)
	h.SetClock(func() time.Time { return base.Add(time.Minute) })
	if err := h.Provision("temp-1", "hw-temp", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	d := device.NewTemperature("temp-1", "hw-temp", "boot-a", 1)
	first := d.Reading(base.Add(2*time.Minute), 21, 40)
	second := d.Reading(base.Add(30*time.Second), 22, 41)
	third := d.Reading(base.Add(3*time.Minute), 23, 42)
	if _, err := h.Receive("temp-1", first); err != nil {
		t.Fatal(err)
	}
	h.SetClock(func() time.Time { return base.Add(5 * time.Minute) })
	if _, err := h.Receive("temp-1", third); err != nil {
		t.Fatal(err)
	}
	gap, err := h.Resume("temp-1", "boot-a")
	if err != nil || !gap.HasProgress || gap.NextSequence != 4 || len(gap.Missing) != 1 || gap.Missing[0] != 2 {
		t.Fatalf("gap resume = %+v, %v", gap, err)
	}
	if _, err := h.Receive("temp-1", second); err != nil {
		t.Fatal(err)
	}
	resume, err := h.Resume("temp-1", "boot-a")
	if err != nil || !resume.HasProgress || resume.NextSequence != 4 || len(resume.Missing) != 0 {
		t.Fatalf("resume = %+v, %v", resume, err)
	}
	ack, err := h.Receive("temp-1", first)
	if err != nil || !ack.Stored || !ack.Duplicate {
		t.Fatalf("duplicate ack = %+v, %v", ack, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := s2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	wantObserved := map[float64]time.Time{21: base.Add(2 * time.Minute), 22: base.Add(30 * time.Second), 23: base.Add(3 * time.Minute)}
	var obsCount, rawCount int
	for _, record := range records {
		if record.Raw != nil {
			rawCount++
		}
		if record.Observation != nil {
			obsCount++
			temperature := record.Observation.Measures["temperature_c"]
			want, ok := wantObserved[temperature]
			if !ok || !record.Observation.ObservedAt.Equal(want) {
				t.Fatalf("observed time changed for %v: %v", temperature, record.Observation.ObservedAt)
			}
			if record.Observation.ReceivedAt.Before(base.Add(time.Minute)) || record.Observation.ReceivedAt.After(base.Add(5*time.Minute)) {
				t.Fatalf("received time not preserved: %v", record.Observation.ReceivedAt)
			}
		}
	}
	if obsCount != 3 || rawCount != 4 {
		t.Fatalf("raw=%d observations=%d", rawCount, obsCount)
	}
}

func TestRebootSequenceDoesNotDeleteNewData(t *testing.T) {
	h, _, _ := newHub(t)
	now := time.Now().UTC()
	if err := h.Provision("vib-1", "hw-vib", device.VibrationType, now); err != nil {
		t.Fatal(err)
	}
	old := device.NewVibration("vib-1", "hw-vib", "boot-old", 100)
	newBoot := device.NewVibration("vib-1", "hw-vib", "boot-new", 1)
	oldMsg := old.Reading(now.Add(time.Second), 1, 0, 0, 1)
	newMsg := newBoot.Reading(now.Add(2*time.Second), 2, 0, 0, 2)
	if _, err := h.Receive("vib-1", oldMsg); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Receive("vib-1", newMsg); err != nil {
		t.Fatal(err)
	}
	newResume, err := h.Resume("vib-1", "boot-new")
	if err != nil || newResume.NextSequence != 2 {
		t.Fatalf("new resume = %+v, %v", newResume, err)
	}
	oldResume, err := h.Resume("vib-1", "boot-old")
	if err != nil || oldResume.NextSequence != 101 {
		t.Fatalf("old resume = %+v, %v", oldResume, err)
	}
}

func TestIdentityReplacementKeepsObservationsSeparate(t *testing.T) {
	h, _, _ := newHub(t)
	now := time.Now().UTC()
	if err := h.Provision("temp-old", "hw-shared", device.TemperatureType, now); err != nil {
		t.Fatal(err)
	}
	d := device.NewTemperature("temp-old", "hw-shared", "boot-x", 1)
	msg := d.Reading(now.Add(time.Second), 20, 50)
	first, err := h.Receive("temp-old", msg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ReplaceIdentity("temp-old", "temp-new", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Receive("temp-old", msg); err == nil {
		t.Fatal("retired identity must not receive")
	}
	d.ReplaceIdentity("temp-new")
	second, err := h.Receive("temp-new", msg)
	if err != nil || second.Duplicate {
		t.Fatalf("same observation ID under new identity must be independently stored: %+v %v", second, err)
	}
	_ = first
}

func TestTornTailRecordIsNotCompleted(t *testing.T) {
	dir := t.TempDir()
	dataDir := dir
	s, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	h := hub.New(s, adapter.NewRegistry(device.TemperatureAdapter{}))
	now := time.Now().UTC()
	if err := h.Provision("temp-1", "hw", device.TemperatureType, now); err != nil {
		t.Fatal(err)
	}
	d := device.NewTemperature("temp-1", "hw", "boot", 1)
	if _, err := h.Receive("temp-1", d.Reading(now.Add(time.Second), 1, 1)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dataDir, "journal.jsonl")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	torn := append(append([]byte(nil), good...), []byte("{\"kind\":\"ingest\",partial")...)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	records, err := s2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var obs int
	for _, record := range records {
		if record.Observation != nil {
			obs++
		}
	}
	if obs != 1 {
		t.Fatalf("observations after torn tail = %d", obs)
	}
}
