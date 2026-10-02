package hub_test

import (
	"errors"
	"testing"
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/device"
	"edge-ingest/internal/hub"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

func provisionTemp(t *testing.T, h *hub.Hub, base time.Time) {
	t.Helper()
	if err := h.Provision("temp-1", "hw-temp", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
}

// receiveReadings ingests one reading per value, advancing the hub clock by
// one second between readings so time-range filters can tell them apart.
func receiveReadings(t *testing.T, h *hub.Hub, now *time.Time, values ...float64) *device.Temperature {
	t.Helper()
	d := device.NewTemperature("temp-1", "hw-temp", "boot-1", 1)
	for _, v := range values {
		*now = now.Add(time.Second)
		if _, err := h.Receive("temp-1", d.Reading(*now, v, 40)); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

func TestCorrectionBranchAdoptionTraceabilityAndDownstream(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	now := base
	h.SetClock(func() time.Time { return now })
	provisionTemp(t, h, base)
	receiveReadings(t, h, &now, 10, 11)

	if err := h.RegisterDownstream("dashboard", "temp-1:boot-1:1", "", "live dashboard"); err != nil {
		t.Fatal(err)
	}
	if err := h.RegisterDownstream("billing", "temp-1:boot-1:2", "temperature_v1_corrected", "already migrated"); err != nil {
		t.Fatal(err)
	}

	branch, err := h.OpenCorrection(correctedTempRule{}, hub.ReinterpretFilter{IdentityID: "temp-1"}, "alice", "calibration offset +0.5")
	if err != nil {
		t.Fatal(err)
	}
	preview, err := h.PreviewCorrection(branch.ID, hub.ReinterpretFilter{})
	if err != nil || preview.Matched != 2 {
		t.Fatalf("preview = %+v, %v", preview, err)
	}
	for _, entry := range preview.Processed {
		if entry.Change != model.ChangeValue {
			t.Fatalf("preview change = %s", entry.Change)
		}
	}

	report, err := h.AdoptBranch(branch.ID, hub.AdoptOptions{Operator: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Adopted) != 2 || report.BranchStatus != model.BranchAdopted {
		t.Fatalf("adopt report = %+v", report)
	}
	if len(report.DownstreamNeedsReview) != 1 || report.DownstreamNeedsReview[0] != "dashboard" {
		t.Fatalf("needs review = %v", report.DownstreamNeedsReview)
	}
	if len(report.DownstreamValid) != 1 || report.DownstreamValid[0] != "billing" {
		t.Fatalf("valid = %v", report.DownstreamValid)
	}

	current, ok := h.CurrentValue("temp-1:boot-1:1")
	if !ok {
		t.Fatal("no current value")
	}
	if current.Rule != "temperature_v1_corrected" || current.AdoptionID == "" {
		t.Fatalf("current = %+v", current)
	}
	if current.Observation.Measures["temperature_c"] != 10.5 {
		t.Fatalf("current value = %v", current.Observation.Measures)
	}
	if current.RawRef.DeliveryID != "temp-1:boot-1:1" || current.RawRef.PayloadSHA == "" {
		t.Fatalf("raw ref = %+v", current.RawRef)
	}
	// The original observation is stored once and never overwritten.
	original, ok := s.ObservationFor("temp-1:boot-1:1")
	if !ok || original.Measures["temperature_c"] != 10 {
		t.Fatalf("original observation changed: %+v", original)
	}

	// Reopen: every current value still traces to raw message and adoption.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	h2 := hub.New(s2, adapter.NewRegistry(device.TemperatureAdapter{}))
	current2, ok := h2.CurrentValue("temp-1:boot-1:1")
	if !ok || current2.Rule != "temperature_v1_corrected" || current2.AdoptionID != current.AdoptionID {
		t.Fatalf("current after reopen = %+v, %v", current2, ok)
	}
	if current2.RawRef.PayloadSHA != current.RawRef.PayloadSHA {
		t.Fatalf("raw ref changed after reopen: %+v", current2.RawRef)
	}
	if _, err := h2.AdoptBranch(branch.ID, hub.AdoptOptions{Operator: "alice"}); err == nil {
		t.Fatal("adopting a closed branch should fail before re-registering the rule or because it is adopted")
	}
}

func TestConcurrentCorrectionsConflictNarrowAndAbandon(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	now := base
	h.SetClock(func() time.Time { return now })
	provisionTemp(t, h, base)
	receiveReadings(t, h, &now, 10, 11, 12)
	t1 := base.Add(1500 * time.Millisecond)

	// Alice corrects the calibration of the first two readings.
	alice, err := h.OpenCorrection(correctedTempRule{}, hub.ReinterpretFilter{IdentityID: "temp-1", To: t1}, "alice", "offset fix")
	if err != nil {
		t.Fatal(err)
	}
	// Bob reinterprets the whole day in imperial units, overlapping Alice.
	bob, err := h.OpenCorrection(unitChangeRule{}, hub.ReinterpretFilter{IdentityID: "temp-1"}, "bob", "imperial units")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.AdoptBranch(alice.ID, hub.AdoptOptions{Operator: "alice"}); err != nil {
		t.Fatal(err)
	}

	// Bob's adoption must not silently overwrite Alice's work.
	report, err := h.AdoptBranch(bob.ID, hub.AdoptOptions{Operator: "bob"})
	if !errors.Is(err, hub.ErrCorrectionConflict) {
		t.Fatalf("expected conflict, got %v (%+v)", err, report)
	}
	if len(report.Conflicts) != 1 || report.Conflicts[0].DeliveryID != "temp-1:boot-1:1" {
		t.Fatalf("conflicts = %+v", report.Conflicts)
	}
	if got := s.Revision(); got != 1 {
		t.Fatalf("conflicted adoption wrote state, revision = %d", got)
	}
	current, _ := h.CurrentValue("temp-1:boot-1:1")
	if current.Rule != "temperature_v1_corrected" {
		t.Fatalf("alice's adoption was overwritten: %+v", current)
	}
	if current, _ := h.CurrentValue("temp-1:boot-1:3"); current.Rule != "" {
		t.Fatalf("conflicted adoption leaked to seq 3: %+v", current)
	}

	// Narrowing adopts only the non-conflicting part; the units of the two
	// interpretations are never mixed into one delivery.
	report, err = h.AdoptBranch(bob.ID, hub.AdoptOptions{Operator: "bob", Narrow: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Adopted) != 2 || report.Adopted[0] != "temp-1:boot-1:2" || report.Adopted[1] != "temp-1:boot-1:3" || report.BranchStatus != model.BranchPartiallyAdopted {
		t.Fatalf("narrow report = %+v", report)
	}
	if len(report.Conflicts) != 1 {
		t.Fatalf("narrow conflicts = %+v", report.Conflicts)
	}
	c1, _ := h.CurrentValue("temp-1:boot-1:1")
	c2, _ := h.CurrentValue("temp-1:boot-1:2")
	c3, _ := h.CurrentValue("temp-1:boot-1:3")
	if c1.Rule != "temperature_v1_corrected" || c1.Observation.Measures["temperature_c"] != 10.5 {
		t.Fatalf("seq1 = %+v", c1)
	}
	if c2.Rule != "temperature_v1_imperial" || c2.Observation.Measures["temperature_f"] != 51.8 {
		t.Fatalf("seq2 = %+v", c2)
	}
	if c3.Rule != "temperature_v1_imperial" || c3.Observation.Measures["temperature_f"] != 53.6 {
		t.Fatalf("seq3 = %+v", c3)
	}

	// The abandoned remainder keeps its source and rationale.
	abandoned, err := h.AbandonBranch(bob.ID, "bob", "defer unit migration for overlapping range")
	if err != nil {
		t.Fatal(err)
	}
	if abandoned.Status != model.BranchAbandoned || abandoned.CloseReason == "" || abandoned.Reason != "imperial units" {
		t.Fatalf("abandoned = %+v", abandoned)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	h2 := hub.New(s2, adapter.NewRegistry(device.TemperatureAdapter{}))
	branches := h2.Branches()
	if len(branches) != 2 || branches[1].Status != model.BranchAbandoned || branches[1].CloseReason != "defer unit migration for overlapping range" {
		t.Fatalf("branches after reopen = %+v", branches)
	}
	c3again, _ := h2.CurrentValue("temp-1:boot-1:3")
	if c3again.Rule != "temperature_v1_imperial" {
		t.Fatalf("seq3 after reopen = %+v", c3again)
	}
}

func TestPartialRetractionRestoresPreviousInterpretation(t *testing.T) {
	h, _, _ := newHub(t)
	base := time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC)
	now := base
	h.SetClock(func() time.Time { return now })
	provisionTemp(t, h, base)
	receiveReadings(t, h, &now, 10, 11, 12)

	branch, err := h.OpenCorrection(correctedTempRule{}, hub.ReinterpretFilter{IdentityID: "temp-1"}, "alice", "offset fix")
	if err != nil {
		t.Fatal(err)
	}
	report, err := h.AdoptBranch(branch.ID, hub.AdoptOptions{Operator: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.RegisterDownstream("dashboard", "temp-1:boot-1:1", "temperature_v1_corrected", ""); err != nil {
		t.Fatal(err)
	}

	// Retract only the first reading (local withdrawal by time range).
	retracted, err := h.RetractAdoption(report.AdoptionID, hub.ReinterpretFilter{To: base.Add(1500 * time.Millisecond)}, "alice", "sensor was being replaced")
	if err != nil {
		t.Fatal(err)
	}
	if len(retracted.Adopted) != 1 || retracted.Adopted[0] != "temp-1:boot-1:1" {
		t.Fatalf("retracted = %+v", retracted)
	}
	if len(retracted.DownstreamNeedsReview) != 1 || retracted.DownstreamNeedsReview[0] != "dashboard" {
		t.Fatalf("retraction downstream = %+v", retracted)
	}
	c1, _ := h.CurrentValue("temp-1:boot-1:1")
	if c1.Rule != "" || c1.AdoptionID != "" || c1.Observation.Measures["temperature_c"] != 10 {
		t.Fatalf("seq1 after retraction = %+v", c1)
	}
	c2, _ := h.CurrentValue("temp-1:boot-1:2")
	if c2.Rule != "temperature_v1_corrected" {
		t.Fatalf("seq2 should stay corrected = %+v", c2)
	}
	// The adoption no longer governs seq 1; retracting it again fails.
	if _, err := h.RetractAdoption(report.AdoptionID, hub.ReinterpretFilter{To: base.Add(1500 * time.Millisecond)}, "alice", "again"); err == nil {
		t.Fatal("second retraction should fail")
	}
}

func TestCorrectionUnknownFormatAndInterruptedAdoptionResume(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	h.SetClock(func() time.Time { return base })
	if err := h.Provision("temp-1", "hw-temp", "future-temperature", base); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"observation_id":"f-1","observed_at":"` + base.Add(time.Second).Format(time.RFC3339Nano) + `","temp_c":18.5,"humidity_pct":55}`)
	delivery := model.RawDelivery{BootID: "boot", Sequence: 1, Format: "future-temperature", ContentType: "application/json", Payload: payload}
	ack, err := h.Receive("temp-1", delivery)
	if err != nil || !ack.Pending {
		t.Fatalf("pending ack = %+v, %v", ack, err)
	}

	// Interruption: process restarts before the format is explained.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	h2 := hub.New(s2, adapter.NewRegistry(device.TemperatureAdapter{}))
	h2.RegisterAdapter(futureAdapter{format: "future-temperature"})
	result, err := h2.ReprocessPending()
	if err != nil || len(result.Processed) != 1 {
		t.Fatalf("reprocess = %+v, %v", result, err)
	}

	// Open a correction branch, then interrupt again before adopting.
	branch, err := h2.OpenCorrection(correctedTempRule{}, hub.ReinterpretFilter{IdentityID: "temp-1"}, "alice", "offset fix")
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	h3 := hub.New(s3, adapter.NewRegistry(device.TemperatureAdapter{}))
	if _, err := h3.AdoptBranch(branch.ID, hub.AdoptOptions{Operator: "alice"}); err == nil {
		t.Fatal("adoption must fail until the rule is re-registered")
	}
	h3.RegisterRule(correctedTempRule{})
	adopted, err := h3.AdoptBranch(branch.ID, hub.AdoptOptions{Operator: "alice"})
	if err != nil || len(adopted.Adopted) != 1 {
		t.Fatalf("resumed adoption = %+v, %v", adopted, err)
	}
	current, ok := h3.CurrentValue("temp-1:boot:1")
	if !ok || current.Rule != "temperature_v1_corrected" || current.AdoptionID != adopted.AdoptionID {
		t.Fatalf("current = %+v, %v", current, ok)
	}
	if current.Observation.Measures["temperature_c"] != 19 {
		t.Fatalf("current measures = %v", current.Observation.Measures)
	}
	if current.RawRef.PayloadSHA != store.PayloadSHA(payload) {
		t.Fatalf("current value does not trace to original message: %+v", current.RawRef)
	}
}
