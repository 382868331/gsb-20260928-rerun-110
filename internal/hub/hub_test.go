package hub_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
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

// correctedTempRule fixes a calibration offset and renames the measure unit.
type correctedTempRule struct{}

func (correctedTempRule) Type() string { return "temperature_v1_corrected" }

func (correctedTempRule) Decode(msg model.RawMessage) (model.Observation, error) {
	var p device.TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, err
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, p.ObservedAt)
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       "temperature_v1_corrected",
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures: map[string]float64{
			"temperature_c": p.Celsius + 0.5,
			"humidity_pct":  p.HumidityPct,
		},
	}, nil
}

// unitChangeRule reinterprets the same payload in Fahrenheit.
type unitChangeRule struct{}

func (unitChangeRule) Type() string { return "temperature_v1_imperial" }

func (unitChangeRule) Decode(msg model.RawMessage) (model.Observation, error) {
	var p device.TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, err
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, p.ObservedAt)
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       "temperature_v1_imperial",
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures: map[string]float64{
			"temperature_f": p.Celsius*9/5 + 32,
			"humidity_pct":  p.HumidityPct,
		},
	}, nil
}

// annotationRule keeps measures identical but adds explanation attributes.
type annotationRule struct{}

func (annotationRule) Type() string { return "temperature_v1_annotated" }

func (annotationRule) Decode(msg model.RawMessage) (model.Observation, error) {
	var p device.TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, err
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, p.ObservedAt)
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       "temperature_v1_annotated",
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures: map[string]float64{
			"temperature_c": p.Celsius,
			"humidity_pct":  p.HumidityPct,
		},
		Attributes: map[string]any{"quality": "estimated"},
	}, nil
}

func TestReinterpretCorrectedRuleKeepsOldObservation(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	h.SetClock(func() time.Time { return base })
	if err := h.Provision("temp-1", "hw-temp", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	d := device.NewTemperature("temp-1", "hw-temp", "boot-1", 1)
	if _, err := h.Receive("temp-1", d.Reading(base.Add(time.Second), 10, 40)); err != nil {
		t.Fatal(err)
	}
	report, err := h.Reinterpret(correctedTempRule{}, hub.ReinterpretFilter{IdentityID: "temp-1"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Matched != 1 || len(report.Processed) != 1 || report.Skipped != 0 {
		t.Fatalf("report = %+v", report)
	}
	entry := report.Processed[0]
	if entry.Change != model.ChangeValue {
		t.Fatalf("change = %s, want value_changed", entry.Change)
	}
	if len(entry.Comparison.Measures) != 1 || entry.Comparison.Measures[0].OldValue != 10 || entry.Comparison.Measures[0].NewValue != 10.5 {
		t.Fatalf("diff = %+v", entry.Comparison.Measures)
	}
	// Old observation must still be there, unchanged.
	records, err := s.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var original, reinterpreted int
	for _, record := range records {
		if record.Observation == nil {
			continue
		}
		if record.Kind == "reinterpretation" {
			reinterpreted++
			if record.Rule != "temperature_v1_corrected" || record.Comparison == nil || record.Comparison.Change != model.ChangeValue {
				t.Fatalf("reinterpretation record = %+v", record)
			}
			continue
		}
		original++
		if record.Observation.Measures["temperature_c"] != 10 {
			t.Fatalf("original observation overwritten: %v", record.Observation.Measures["temperature_c"])
		}
	}
	if original != 1 || reinterpreted != 1 {
		t.Fatalf("original=%d reinterpreted=%d", original, reinterpreted)
	}
	// Reinterpretation survives a restart and is not duplicated on resume.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	h2 := hub.New(s2, adapter.NewRegistry(device.TemperatureAdapter{}))
	resumed, err := h2.Reinterpret(correctedTempRule{}, hub.ReinterpretFilter{IdentityID: "temp-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Matched != 1 || resumed.Skipped != 1 || len(resumed.Processed) != 0 {
		t.Fatalf("resumed report = %+v", resumed)
	}
	records, err = s2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, record := range records {
		if record.Kind == "reinterpretation" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("reinterpretation records after resume = %d", count)
	}
}

func TestReinterpretClassifiesUnitAndAnnotationChanges(t *testing.T) {
	h, _, _ := newHub(t)
	base := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	h.SetClock(func() time.Time { return base })
	if err := h.Provision("temp-1", "hw-temp", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	d := device.NewTemperature("temp-1", "hw-temp", "boot-1", 1)
	if _, err := h.Receive("temp-1", d.Reading(base.Add(time.Second), 10, 40)); err != nil {
		t.Fatal(err)
	}
	unitReport, err := h.Reinterpret(unitChangeRule{}, hub.ReinterpretFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unitReport.Processed) != 1 || unitReport.Processed[0].Change != model.ChangeUnit {
		t.Fatalf("unit report = %+v", unitReport)
	}
	diff := unitReport.Processed[0].Comparison.Measures
	if len(diff) != 1 || diff[0].Kind != "unit" || diff[0].OldUnit != "c" || diff[0].NewUnit != "f" {
		t.Fatalf("unit diff = %+v", diff)
	}
	annotationReport, err := h.Reinterpret(annotationRule{}, hub.ReinterpretFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(annotationReport.Processed) != 1 || annotationReport.Processed[0].Change != model.ChangeAnnotation {
		t.Fatalf("annotation report = %+v", annotationReport)
	}
	if !annotationReport.Processed[0].Comparison.AttributesChanged {
		t.Fatalf("annotation comparison = %+v", annotationReport.Processed[0].Comparison)
	}
}

func TestReinterpretFilterByDeviceAndTimeRange(t *testing.T) {
	h, _, _ := newHub(t)
	base := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw-1", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	if err := h.Provision("temp-2", "hw-2", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	d1 := device.NewTemperature("temp-1", "hw-1", "boot-1", 1)
	d2 := device.NewTemperature("temp-2", "hw-2", "boot-1", 1)
	if _, err := h.Receive("temp-1", d1.Reading(base.Add(time.Second), 10, 40)); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(time.Hour)
	if _, err := h.Receive("temp-1", d1.Reading(base.Add(time.Hour), 11, 41)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Receive("temp-2", d2.Reading(base.Add(time.Hour), 12, 42)); err != nil {
		t.Fatal(err)
	}
	// Only temp-1 messages received within the first half hour.
	report, err := h.Reinterpret(correctedTempRule{}, hub.ReinterpretFilter{
		IdentityID: "temp-1",
		From:       base,
		To:         base.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Matched != 1 || len(report.Processed) != 1 {
		t.Fatalf("filtered report = %+v", report)
	}
	if report.Processed[0].DeliveryID != "temp-1:boot-1:1" {
		t.Fatalf("filtered delivery = %s", report.Processed[0].DeliveryID)
	}
	// Remaining messages are reinterpreted on a later run.
	rest, err := h.Reinterpret(correctedTempRule{}, hub.ReinterpretFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if rest.Matched != 3 || rest.Skipped != 1 || len(rest.Processed) != 2 {
		t.Fatalf("follow-up report = %+v", rest)
	}
}

func TestConflictEvidencePreserved(t *testing.T) {
	h, s, dir := newHub(t)
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
		t.Fatal("conflict must be rejected")
	}
	conflicts := h.Conflicts()
	if len(conflicts) != 1 {
		t.Fatalf("conflicts = %+v", conflicts)
	}
	evidence := conflicts[0]
	if evidence.Existing.PayloadSHA == "" || evidence.Incoming.Payload == nil {
		t.Fatalf("evidence incomplete: %+v", evidence)
	}
	if evidence.Existing.PayloadSHA == store.PayloadSHA(evidence.Incoming.Payload) {
		t.Fatal("conflicting payloads must have different hashes")
	}
	if evidence.Existing.Sequence != 1 || evidence.Existing.BootID != "boot" {
		t.Fatalf("evidence location = %+v", evidence.Existing)
	}
	// Evidence survives restart; old session data is untouched.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	h2 := hub.New(s2, adapter.NewRegistry(device.TemperatureAdapter{}))
	if got := h2.Conflicts(); len(got) != 1 {
		t.Fatalf("conflicts after restart = %+v", got)
	}
	resume, err := h2.Resume("temp-1", "boot")
	if err != nil || resume.NextReceivedSequence != 2 || resume.NextConfirmedSequence != 2 {
		t.Fatalf("conflict must not advance watermarks: %+v %v", resume, err)
	}
}

func TestReprocessPendingReportsScope(t *testing.T) {
	h, _, _ := newHub(t)
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw", "future-temperature", base); err != nil {
		t.Fatal(err)
	}
	for i, seq := range []int64{3, 4} {
		clock = base.Add(time.Duration(i) * time.Minute)
		payload, _ := json.Marshal(map[string]any{
			"observation_id": "future-" + string(rune('a'+i)),
			"observed_at":    clock.Format(time.RFC3339Nano),
			"temp_c":         18.5,
			"humidity_pct":   55,
		})
		ack, err := h.Receive("temp-1", model.RawDelivery{BootID: "boot", Sequence: seq, Format: "future-temperature", ContentType: "application/json", Payload: payload})
		if err != nil || !ack.Pending {
			t.Fatalf("pending ack = %+v, %v", ack, err)
		}
	}
	h.RegisterAdapter(futureAdapter{format: "future-temperature"})
	result, err := h.ReprocessPending()
	if err != nil || len(result.Processed) != 2 {
		t.Fatalf("reprocess = %+v, %v", result, err)
	}
	if len(result.Scope) != 1 {
		t.Fatalf("scope = %+v", result.Scope)
	}
	scope := result.Scope[0]
	if scope.IdentityID != "temp-1" || scope.BootID != "boot" || scope.FromSequence != 3 || scope.ToSequence != 4 {
		t.Fatalf("scope = %+v", scope)
	}
	if !scope.FirstReceived.Equal(base) || !scope.LastReceived.Equal(base.Add(time.Minute)) {
		t.Fatalf("scope time range = %+v", scope)
	}
}

func TestCorrectionBranchesAdoptionAuditWithdrawalAndRestart(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw-1", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	if err := h.Provision("temp-2", "hw-2", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	d1 := device.NewTemperature("temp-1", "hw-1", "boot-1", 1)
	d2 := device.NewTemperature("temp-2", "hw-2", "boot-1", 1)
	clock = base.Add(time.Minute)
	if _, err := h.Receive("temp-1", d1.Reading(clock, 10, 40)); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(2 * time.Minute)
	if _, err := h.Receive("temp-1", d1.Reading(clock, 11, 41)); err != nil {
		t.Fatal(err)
	}
	clock = base.Add(3 * time.Minute)
	if _, err := h.Receive("temp-2", d2.Reading(clock, 12, 42)); err != nil {
		t.Fatal(err)
	}
	unknownPayload, _ := json.Marshal(map[string]any{
		"observation_id": "unknown-1",
		"observed_at":    base.Add(4 * time.Minute).Format(time.RFC3339Nano),
		"temp_c":         18,
		"humidity_pct":   44,
	})
	clock = base.Add(4 * time.Minute)
	unknown := model.RawDelivery{BootID: "boot-1", Sequence: 3, Format: "future-x", ContentType: "application/json", Payload: unknownPayload}
	ack, err := h.Receive("temp-1", unknown)
	if err != nil || !ack.Pending {
		t.Fatalf("unknown delivery = %+v, %v", ack, err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var buildErr error
	go func() {
		defer wg.Done()
		_, err := h.CreateCorrectionBranch(hub.CorrectionRequest{
			ID: "fahrenheit", Rule: unitChangeRule{}, Operator: "alice",
			Reason: "site reports imperial unit", Scope: model.BranchScope{IdentityID: "temp-1", ToSequence: 2},
		})
		if err != nil {
			buildErr = err
		}
	}()
	go func() {
		defer wg.Done()
		_, err := h.CreateCorrectionBranch(hub.CorrectionRequest{
			ID: "calibration", Rule: correctedTempRule{}, Operator: "bob",
			Reason: "sensor offset", Scope: model.BranchScope{ToSequence: 2},
		})
		if err != nil {
			buildErr = err
		}
	}()
	wg.Wait()
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	unknownBuild, err := h.CreateCorrectionBranch(hub.CorrectionRequest{
		ID: "explain-future", Rule: futureAdapter{format: "future-x"}, Operator: "alice",
		Scope: model.BranchScope{IdentityID: "temp-1", BootID: "boot-1", FromSequence: 3, ToSequence: 3},
	})
	if err != nil || len(unknownBuild.Processed) != 1 || unknownBuild.Pending != 1 {
		t.Fatalf("unknown build = %+v, %v", unknownBuild, err)
	}
	referenceChanged, err := h.RegisterDownstreamReference(hub.ReferenceRequest{
		ID: "dashboard-changed", Name: "changed dashboard",
		DeliveryIDs: []string{"temp-1:boot-1:1", "temp-1:boot-1:2"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(h.BranchEntries("fahrenheit")) != 2 {
		t.Fatalf("fahrenheit entries = %d", len(h.BranchEntries("fahrenheit")))
	}
	fAdopted, err := h.AdoptBranch(hub.AdoptionRequest{BranchID: "fahrenheit", Operator: "reviewer", Reason: "officially imperial"})
	if err != nil || fAdopted.Adoption == nil || len(fAdopted.Adopted) != 2 {
		t.Fatalf("adopt fahrenheit = %+v, %v", fAdopted, err)
	}
	overlap, err := h.AdoptBranch(hub.AdoptionRequest{BranchID: "calibration", Operator: "reviewer", Scope: model.BranchScope{IdentityID: "temp-1"}})
	if err != nil || len(overlap.Conflicts) != 2 || overlap.Adoption != nil {
		t.Fatalf("overlap adoption = %+v, %v", overlap, err)
	}
	if overlap.Conflicts[0].Reason != "incompatible unit interpretation" {
		t.Fatalf("conflict reason = %s", overlap.Conflicts[0].Reason)
	}
	narrowed, err := h.AdoptBranch(hub.AdoptionRequest{
		BranchID: "calibration", Operator: "reviewer",
		Scope: model.BranchScope{IdentityID: "temp-2"},
	})
	if err != nil || len(narrowed.Adopted) != 1 || narrowed.Adopted[0].RawRef.IdentityID != "temp-2" {
		t.Fatalf("narrowed adoption = %+v, %v", narrowed, err)
	}
	futureAdopted, err := h.AdoptBranch(hub.AdoptionRequest{BranchID: "explain-future", Operator: "reviewer"})
	if err != nil || futureAdopted.Adoption == nil {
		t.Fatalf("future adoption = %+v, %v", futureAdopted, err)
	}
	if err := h.AbandonBranch("unused", "operator", "not needed"); err == nil {
		t.Fatal("missing branch must not be abandoned")
	}

	referenceValid, err := h.RegisterDownstreamReference(hub.ReferenceRequest{
		ID: "dashboard-valid", Name: "new dashboard",
		DeliveryIDs: []string{"temp-1:boot-1:3"},
	})
	if err != nil {
		t.Fatal(err)
	}
	impacts := h.AuditReferences()
	impactByID := map[string]model.ReferenceImpact{}
	for _, impact := range impacts {
		impactByID[impact.ReferenceID] = impact
	}
	if impactByID[referenceChanged.ID].Status != model.ReferenceReview || len(impactByID[referenceChanged.ID].DeliveryIDs) != 2 {
		t.Fatalf("changed impact = %+v", impactByID[referenceChanged.ID])
	}
	if impactByID[referenceValid.ID].Status != model.ReferenceValid {
		t.Fatalf("valid impact = %+v", impactByID[referenceValid.ID])
	}

	withdrawal, err := h.WithdrawAdoption(fAdopted.Adoption.ID, "reviewer", "first reading needs original", model.BranchScope{
		IdentityID: "temp-1", FromSequence: 1, ToSequence: 1,
	})
	if err != nil || len(withdrawal.DeliveryIDs) != 1 {
		t.Fatalf("withdrawal = %+v, %v", withdrawal, err)
	}
	current1, ok := h.CurrentValue("temp-1:boot-1:1")
	if !ok || current1.Source != "original" || !current1.Withdrawn || current1.Observation.Measures["temperature_c"] != 10 {
		t.Fatalf("withdrawn current = %+v ok=%v", current1, ok)
	}
	current2, ok := h.CurrentValue("temp-1:boot-1:2")
	if !ok || current2.Source != "branch" || current2.BranchID != "fahrenheit" || current2.Observation.Measures["temperature_f"] == 0 {
		t.Fatalf("remaining adopted current = %+v ok=%v", current2, ok)
	}
	current3, ok := h.CurrentValue("temp-1:boot-1:3")
	if !ok || current3.Source != "branch" || current3.AdoptionID == "" || current3.RawRef.PayloadSHA == "" {
		t.Fatalf("unknown current trace = %+v ok=%v", current3, ok)
	}
	if impact := impactByID[referenceChanged.ID]; len(impact.DeliveryIDs) != 2 {
		t.Fatalf("adoption snapshot changed: %+v", impact)
	}
	postWithdrawal := h.AuditReferences()
	for _, impact := range postWithdrawal {
		if impact.ReferenceID == referenceChanged.ID && impact.Status != model.ReferenceReview {
			t.Fatalf("post-withdrawal impact = %+v", impact)
		}
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	h2 := hub.New(s2, adapter.NewRegistry(device.TemperatureAdapter{}))
	restart1, ok := h2.CurrentValue("temp-1:boot-1:1")
	if !ok || restart1.Source != "original" || !restart1.Withdrawn || restart1.RawRef.DeliveryID != "temp-1:boot-1:1" {
		t.Fatalf("restart current1 = %+v ok=%v", restart1, ok)
	}
	restart2, ok := h2.CurrentValue("temp-1:boot-1:2")
	if !ok || restart2.Source != "branch" || restart2.BranchID != "fahrenheit" || restart2.AdoptionID == "" {
		t.Fatalf("restart current2 = %+v ok=%v", restart2, ok)
	}
	if len(h2.BranchConflicts()) != 2 {
		t.Fatalf("restart branch conflicts = %+v", h2.BranchConflicts())
	}
	resumedBuild, err := h2.CreateCorrectionBranch(hub.CorrectionRequest{
		ID: "explain-future", Rule: futureAdapter{format: "future-x"}, Operator: "alice",
		Scope: model.BranchScope{IdentityID: "temp-1", BootID: "boot-1", FromSequence: 3, ToSequence: 3},
	})
	if err != nil || resumedBuild.Matched != 1 || resumedBuild.Skipped != 1 || len(resumedBuild.Processed) != 0 {
		t.Fatalf("resumed unknown branch = %+v, %v", resumedBuild, err)
	}
}
