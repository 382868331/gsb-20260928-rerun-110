package hub_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/device"
	"edge-ingest/internal/hub"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

func newHub(t *testing.T) (*hub.Hub, *store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	adapters := adapter.NewRegistry(device.TemperatureAdapter{}, device.TemperatureV2Adapter{}, device.VibrationAdapter{})
	return hub.New(s, adapters), s, dir
}

func TestFormatEvolutionPreservesHistoricalInterpretation(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC)
	h.SetClock(func() time.Time { return base })
	if err := h.Provision("temp-1", "hw-temp", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	oldDevice := device.NewTemperature("temp-1", "hw-temp", "boot-1", 1)
	oldMessage := oldDevice.Reading(base.Add(time.Second), 10, 40)
	if _, err := h.Receive("temp-1", oldMessage); err != nil {
		t.Fatal(err)
	}
	if err := h.UpgradeIdentity("temp-1", device.TemperatureV2Type); err != nil {
		t.Fatal(err)
	}
	newDevice := device.NewTemperatureV2("temp-1", "hw-temp", "boot-2", 1)
	newMessage := newDevice.Reading(base.Add(2*time.Second), 20, 41)
	newAck, err := h.Receive("temp-1", newMessage)
	if err != nil || newAck.Pending || newAck.Duplicate {
		t.Fatalf("v2 ack = %+v, %v", newAck, err)
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
	values := make(map[string]float64)
	formats := make(map[string]string)
	for _, record := range records {
		if record.Observation == nil {
			continue
		}
		values[record.Observation.ID] = record.Observation.Measures["temperature_c"]
		formats[record.Observation.ID] = record.Observation.Type
	}
	if values["boot-1-1"] != 10 || formats["boot-1-1"] != device.TemperatureType {
		t.Fatalf("old observation changed: value=%v format=%v", values["boot-1-1"], formats["boot-1-1"])
	}
	if got := values["boot-2-1"]; got < 19.999 || got > 20.001 || formats["boot-2-1"] != device.TemperatureV2Type {
		t.Fatalf("new observation decoded with old format: value=%v format=%v", got, formats["boot-2-1"])
	}
}

func TestUnknownFormatStoredAndReprocessedOnce(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Now().UTC()
	if err := h.Provision("temp-1", "hw", "future-temperature", base); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"observation_id": "future-1",
		"observed_at":    base.Add(time.Second).Format(time.RFC3339Nano),
		"temp_c":         18.5,
		"humidity_pct":   55,
	})
	delivery := model.RawDelivery{BootID: "boot", Sequence: 7, Format: "future-temperature", ContentType: "application/json", Payload: payload}
	ack, err := h.Receive("temp-1", delivery)
	if err != nil || !ack.Stored || !ack.Pending {
		t.Fatalf("unknown format ack = %+v, %v", ack, err)
	}
	repeat, err := h.Receive("temp-1", delivery)
	if err != nil || !repeat.Pending {
		t.Fatalf("repeat pending ack = %+v, %v", repeat, err)
	}
	resume, err := h.Resume("temp-1", "boot")
	if err != nil || len(resume.Pending) != 1 || resume.Pending[0] != 7 || resume.NextConfirmedSequence != 7 {
		t.Fatalf("pending resume = %+v, %v", resume, err)
	}
	h.RegisterAdapter(futureAdapter{format: "future-temperature"})
	result, err := h.ReprocessPending()
	if err != nil || len(result.Processed) != 1 || result.Remaining != 0 || result.Processed[0].Duplicate {
		t.Fatalf("reprocess = %+v, %v", result, err)
	}
	result2, err := h.ReprocessPending()
	if err != nil || len(result2.Processed) != 0 || result2.Remaining != 0 {
		t.Fatalf("second reprocess = %+v, %v", result2, err)
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
	var observations, pendingRaw int
	for _, record := range records {
		if record.Observation != nil {
			if record.Observation.ID == "future-1" {
				observations++
			}
		}
		if record.Raw != nil && record.Raw.Status == "pending" {
			pendingRaw++
		}
	}
	if observations != 1 || pendingRaw != 1 {
		t.Fatalf("future observation records=%d pending raw records=%d", observations, pendingRaw)
	}
}

type futureAdapter struct{ format string }

func (a futureAdapter) Type() string { return a.format }

func (a futureAdapter) Decode(msg model.RawMessage) (model.Observation, error) {
	var p device.TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, err
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, p.ObservedAt)
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       a.format,
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures:   map[string]float64{"temperature_c": p.Celsius, "humidity_pct": p.HumidityPct},
	}, nil
}

func TestSameSequenceDifferentContentRejected(t *testing.T) {
	h, _, _ := newHub(t)
	now := time.Now().UTC()
	if err := h.Provision("temp-1", "hw", device.TemperatureType, now); err != nil {
		t.Fatal(err)
	}
	d := device.NewTemperature("temp-1", "hw", "boot", 1)
	first := d.Reading(now.Add(time.Second), 10, 40)
	second := d.Reading(now.Add(2*time.Second), 11, 41)
	if _, err := h.Receive("temp-1", first); err != nil {
		t.Fatal(err)
	}
	second.Sequence = 1
	if _, err := h.Receive("temp-1", second); err == nil {
		t.Fatal("same sequence with different payload must conflict")
	}
	resume, err := h.Resume("temp-1", "boot")
	if err != nil || resume.NextReceivedSequence != 2 || resume.NextConfirmedSequence != 2 {
		t.Fatalf("failed conflict changed checkpoint: %+v %v", resume, err)
	}
}

func TestTornObservationKeepsPendingCheckpoint(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := hub.New(s, adapter.NewRegistry(device.TemperatureAdapter{}))
	now := time.Now().UTC()
	if err := h.Provision("temp-1", "hw", "future-temperature", now); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"observation_id": "future-1",
		"observed_at":    now.Add(time.Second).Format(time.RFC3339Nano),
		"temp_c":         15,
		"humidity_pct":   50,
	})
	delivery := model.RawDelivery{BootID: "boot", Sequence: 3, Format: "future-temperature", ContentType: "application/json", Payload: payload}
	ack, err := h.Receive("temp-1", delivery)
	if err != nil || !ack.Pending {
		t.Fatalf("pending ack = %+v, %v", ack, err)
	}
	path := filepath.Join(dir, "journal.jsonl")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	torn := append(append([]byte(nil), good...), []byte("{\"kind\":\"observation\",partial")...)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.Pending()) != 1 {
		t.Fatalf("pending after torn observation = %d", len(s2.Pending()))
	}
	if confirmed, ok := s2.Confirmed("temp-1", "boot"); ok {
		t.Fatalf("torn confirmation was recovered: %+v", confirmed)
	}
	h2 := hub.New(s2, adapter.NewRegistry(device.TemperatureAdapter{}, futureAdapter{format: "future-temperature"}))
	result, err := h2.ReprocessPending()
	if err != nil || len(result.Processed) != 1 || result.Remaining != 0 {
		t.Fatalf("reprocess after torn tail = %+v, %v", result, err)
	}
	records, err := s2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var observations int
	for _, record := range records {
		if record.Observation != nil && record.Observation.ID == "future-1" {
			observations++
		}
	}
	if observations != 1 {
		t.Fatalf("confirmed observations = %d", observations)
	}
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
	if err != nil || !resume.HasProgress || resume.NextSequence != 4 || resume.NextConfirmedSequence != 4 || resume.ConfirmedSequence != 3 || len(resume.Missing) != 0 {
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
