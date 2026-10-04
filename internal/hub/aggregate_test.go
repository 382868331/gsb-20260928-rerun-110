package hub_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"edge-ingest/internal/device"
	"edge-ingest/internal/hub"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

func receiveTemps(t *testing.T, h *hub.Hub, identity string, clock *time.Time, base time.Time, readings ...struct {
	offsetMin int
	celsius   float64
}) {
	t.Helper()
	d := device.NewTemperature(identity, "hw-"+identity, "boot-1", 1)
	for _, reading := range readings {
		*clock = base.Add(time.Duration(reading.offsetMin) * time.Minute)
		if _, err := h.Receive(identity, d.Reading(*clock, reading.celsius, 40)); err != nil {
			t.Fatal(err)
		}
	}
}

func defineHourly(t *testing.T, h *hub.Hub, id, identity string, from, to time.Time) model.AggregateSpec {
	t.Helper()
	spec, err := h.DefineAggregate(hub.AggregateRequest{
		ID: id, Name: id, Operator: "analyst", IdentityID: identity,
		Measure: "temperature", From: from, To: to, Interval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func previewInterval(t *testing.T, h *hub.Hub, aggregateID string, start time.Time) hub.IntervalPreview {
	t.Helper()
	preview, err := h.PreviewAggregateRevision(aggregateID)
	if err != nil {
		t.Fatal(err)
	}
	for _, interval := range preview.Intervals {
		if interval.Start.Equal(start) {
			return interval
		}
	}
	t.Fatalf("interval %s not found in preview of %s", start, aggregateID)
	return hub.IntervalPreview{}
}

func TestAggregatePreviewUnitChangeAndPartialSignoff(t *testing.T) {
	h, _, _ := newHub(t)
	base := time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw-temp-1", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	receiveTemps(t, h, "temp-1", &clock, base,
		struct {
			offsetMin int
			celsius   float64
		}{10, 10}, struct {
			offsetMin int
			celsius   float64
		}{20, 11}, struct {
			offsetMin int
			celsius   float64
		}{70, 12}, struct {
			offsetMin int
			celsius   float64
		}{80, 13})
	defineHourly(t, h, "hourly-temp", "temp-1", base, base.Add(2*time.Hour))

	run, err := h.ComputeAggregate("hourly-temp")
	if err != nil || len(run.Computed) != 2 {
		t.Fatalf("compute = %+v, %v", run, err)
	}
	first, second := base, base.Add(time.Hour)
	if run.Computed[0].Stats.Count != 2 || run.Computed[0].Stats.Avg != 10.5 || run.Computed[0].Unit != "c" {
		t.Fatalf("first interval = %+v", run.Computed[0])
	}
	if run.Computed[1].Stats.Count != 2 || run.Computed[1].Stats.Avg != 12.5 {
		t.Fatalf("second interval = %+v", run.Computed[1])
	}

	// Partial sign-off: only the first interval is published.
	signoff, err := h.SignOffAggregate(hub.SignoffRequest{
		AggregateID: "hourly-temp", Operator: "alice", Intervals: []time.Time{first},
	})
	if err != nil || len(signoff.Results) != 1 || signoff.Results[0].Status != "signed" {
		t.Fatalf("partial signoff = %+v, %v", signoff, err)
	}
	if got := previewInterval(t, h, "hourly-temp", first); got.Status != model.IntervalPublished {
		t.Fatalf("first interval status = %+v", got)
	}
	if got := previewInterval(t, h, "hourly-temp", second); got.Status != model.IntervalUnpublished || got.Note == "" {
		t.Fatalf("second interval status = %+v", got)
	}

	// Unit-changing correction adopted for the first reading only.
	if _, err := h.CreateCorrectionBranch(hub.CorrectionRequest{
		ID: "fahrenheit", Rule: unitChangeRule{}, Operator: "alice",
		Scope: model.BranchScope{IdentityID: "temp-1", ToSequence: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.CreateCorrectionBranch(hub.CorrectionRequest{
		ID: "calibration", Rule: correctedTempRule{}, Operator: "bob",
		Scope: model.BranchScope{IdentityID: "temp-1", ToSequence: 2},
	}); err != nil {
		t.Fatal(err)
	}
	adopted, err := h.AdoptBranch(hub.AdoptionRequest{BranchID: "fahrenheit", Operator: "reviewer"})
	if err != nil || adopted.Adoption == nil {
		t.Fatalf("adopt = %+v, %v", adopted, err)
	}

	run, err = h.ComputeAggregate("hourly-temp")
	if err != nil || len(run.Computed) != 1 || run.Reused != 1 {
		t.Fatalf("recompute = %+v, %v", run, err)
	}
	revised := run.Computed[0]
	if revised.Revision != 2 || !revised.MixedUnits || len(revised.Contributions) != 1 || len(revised.Skipped) != 1 {
		t.Fatalf("revised interval = %+v", revised)
	}
	if revised.Contributions[0].Unit != "f" || revised.Skipped[0].DeliveryID != "temp-1:boot-1:2" {
		t.Fatalf("mixed unit handling = %+v", revised)
	}

	interval := previewInterval(t, h, "hourly-temp", first)
	if interval.Status != model.IntervalPendingReview || interval.Published == nil || interval.Published.Stats.Avg != 10.5 {
		t.Fatalf("pending interval = %+v", interval)
	}
	if !strings.Contains(interval.Note, "published revision 1 retained") {
		t.Fatalf("pending note = %q", interval.Note)
	}
	changes := map[string]hub.ObservationChange{}
	for _, change := range interval.Changes {
		changes[change.DeliveryID] = change
	}
	if changes["temp-1:boot-1:1"].Change != model.ChangeUnit {
		t.Fatalf("unit change = %+v", changes["temp-1:boot-1:1"])
	}
	if changes["temp-1:boot-1:2"].Change != "removed" {
		t.Fatalf("skipped change = %+v", changes["temp-1:boot-1:2"])
	}
	// The unadopted calibration branch must be reported as blocked, not applied.
	if len(interval.Blocked) != 2 {
		t.Fatalf("blocked = %+v", interval.Blocked)
	}
	blockedReasons := map[string]string{}
	for _, blocked := range interval.Blocked {
		blockedReasons[blocked.DeliveryID] = blocked.Reason
	}
	if blockedReasons["temp-1:boot-1:2"] != "correction branch not adopted" {
		t.Fatalf("blocked reasons = %+v", blockedReasons)
	}

	// Sign off the revision: the new statistic becomes official.
	signoff, err = h.SignOffAggregate(hub.SignoffRequest{
		AggregateID: "hourly-temp", Operator: "bob", Intervals: []time.Time{first},
	})
	if err != nil || signoff.Results[0].Status != "signed" || signoff.Results[0].Revision != 2 {
		t.Fatalf("revision signoff = %+v, %v", signoff, err)
	}
	if got := previewInterval(t, h, "hourly-temp", first); got.Status != model.IntervalPublished || len(got.Changes) != 0 {
		t.Fatalf("post-signoff preview = %+v", got)
	}
}

func TestAggregateUnknownFormatSupplement(t *testing.T) {
	h, _, _ := newHub(t)
	base := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw-temp-1", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	receiveTemps(t, h, "temp-1", &clock, base,
		struct {
			offsetMin int
			celsius   float64
		}{5, 20}, struct {
			offsetMin int
			celsius   float64
		}{15, 22})
	payload, _ := json.Marshal(map[string]any{
		"observation_id": "late-1",
		"observed_at":    base.Add(25 * time.Minute).Format(time.RFC3339Nano),
		"temp_c":         24,
		"humidity_pct":   41,
	})
	clock = base.Add(25 * time.Minute)
	ack, err := h.Receive("temp-1", model.RawDelivery{BootID: "boot-1", Sequence: 3, Format: "future-x", ContentType: "application/json", Payload: payload})
	if err != nil || !ack.Pending {
		t.Fatalf("unknown ack = %+v, %v", ack, err)
	}
	defineHourly(t, h, "hourly-temp", "temp-1", base, base.Add(time.Hour))
	run, err := h.ComputeAggregate("hourly-temp")
	if err != nil || len(run.Computed) != 1 || run.Computed[0].Stats.Count != 2 {
		t.Fatalf("compute = %+v, %v", run, err)
	}
	if _, err := h.SignOffAggregate(hub.SignoffRequest{AggregateID: "hourly-temp", Operator: "alice"}); err != nil {
		t.Fatal(err)
	}

	h.RegisterAdapter(futureAdapter{format: "future-x"})
	reprocessed, err := h.ReprocessPending()
	if err != nil || len(reprocessed.Processed) != 1 {
		t.Fatalf("reprocess = %+v, %v", reprocessed, err)
	}
	run, err = h.ComputeAggregate("hourly-temp")
	if err != nil || len(run.Computed) != 1 || run.Computed[0].Stats.Count != 3 {
		t.Fatalf("recompute = %+v, %v", run, err)
	}
	interval := previewInterval(t, h, "hourly-temp", base)
	if interval.Status != model.IntervalPendingReview || len(interval.Changes) != 1 || interval.Changes[0].Change != "added" {
		t.Fatalf("supplement preview = %+v", interval)
	}
	if interval.Changes[0].DeliveryID != "temp-1:boot-1:3" {
		t.Fatalf("added delivery = %+v", interval.Changes[0])
	}
	signoff, err := h.SignOffAggregate(hub.SignoffRequest{AggregateID: "hourly-temp", Operator: "alice"})
	if err != nil || signoff.Results[0].Revision != 2 {
		t.Fatalf("signoff = %+v, %v", signoff, err)
	}
	if got := previewInterval(t, h, "hourly-temp", base); got.Status != model.IntervalPublished || got.Candidate.Stats.Count != 3 {
		t.Fatalf("final preview = %+v", got)
	}
}

func TestAggregateSharedObservationsConcurrentSignoff(t *testing.T) {
	h, s, dir := newHub(t)
	defer s.Close()
	base := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw-temp-1", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	receiveTemps(t, h, "temp-1", &clock, base,
		struct {
			offsetMin int
			celsius   float64
		}{30, 1}, struct {
			offsetMin int
			celsius   float64
		}{90, 2}, struct {
			offsetMin int
			celsius   float64
		}{150, 3})
	// Two aggregates with overlapping ranges share the middle observation.
	defineHourly(t, h, "agg-a", "temp-1", base, base.Add(2*time.Hour))
	defineHourly(t, h, "agg-b", "temp-1", base.Add(time.Hour), base.Add(3*time.Hour))
	if _, err := h.ComputeAggregate("agg-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ComputeAggregate("agg-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SignOffAggregate(hub.SignoffRequest{AggregateID: "agg-a", Operator: "alice"}); err != nil {
		t.Fatal(err)
	}
	// Signing agg-a must not confirm agg-b even though they share deliveries.
	for _, start := range []time.Time{base.Add(time.Hour), base.Add(2 * time.Hour)} {
		if got := previewInterval(t, h, "agg-b", start); got.Status != model.IntervalUnpublished {
			t.Fatalf("agg-b interval %s = %+v", start, got)
		}
	}

	// Two operators sign agg-b intervals concurrently, overlapping on one.
	shared := base.Add(time.Hour)
	last := base.Add(2 * time.Hour)
	var wg sync.WaitGroup
	results := make([]hub.SignoffResult, 2)
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0], errs[0] = h.SignOffAggregate(hub.SignoffRequest{
			AggregateID: "agg-b", Operator: "alice", Intervals: []time.Time{shared},
		})
	}()
	go func() {
		defer wg.Done()
		results[1], errs[1] = h.SignOffAggregate(hub.SignoffRequest{
			AggregateID: "agg-b", Operator: "bob", Intervals: []time.Time{shared, last},
		})
	}()
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	// The shared interval is published exactly once; the second operator sees
	// the durable decision instead of duplicating it.
	signoffs := h.AggregateSignoffs("agg-b")
	if len(signoffs) != 2 {
		t.Fatalf("agg-b signoffs = %+v", signoffs)
	}
	var sharedStatuses []string
	for _, result := range results {
		for _, entry := range result.Results {
			if entry.Start.Equal(shared) {
				sharedStatuses = append(sharedStatuses, entry.Status)
			}
		}
	}
	sort := map[string]bool{}
	for _, status := range sharedStatuses {
		sort[status] = true
	}
	if !sort["signed"] || !sort["already_current"] || len(sharedStatuses) != 2 {
		t.Fatalf("shared interval statuses = %v", sharedStatuses)
	}
	for _, start := range []time.Time{shared, last} {
		if got := previewInterval(t, h, "agg-b", start); got.Status != model.IntervalPublished {
			t.Fatalf("agg-b interval %s after signoff = %+v", start, got)
		}
	}

	// Replay from disk: published state and sign-off records are consistent.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	h2 := hub.New(s2, hub.DefaultAdapters())
	if got := len(h2.AggregateSignoffs("agg-b")); got != 2 {
		t.Fatalf("replayed signoffs = %d", got)
	}
	for _, start := range []time.Time{shared, last} {
		if got := previewInterval(t, h2, "agg-b", start); got.Status != model.IntervalPublished {
			t.Fatalf("replayed agg-b interval %s = %+v", start, got)
		}
	}
}

func TestAggregateCrashResumeConsistency(t *testing.T) {
	h, s, dir := newHub(t)
	base := time.Date(2026, 10, 2, 11, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw-temp-1", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	receiveTemps(t, h, "temp-1", &clock, base,
		struct {
			offsetMin int
			celsius   float64
		}{10, 1}, struct {
			offsetMin int
			celsius   float64
		}{70, 2}, struct {
			offsetMin int
			celsius   float64
		}{130, 3})
	defineHourly(t, h, "hourly-temp", "temp-1", base, base.Add(3*time.Hour))
	run, err := h.ComputeAggregate("hourly-temp")
	if err != nil || len(run.Computed) != 3 {
		t.Fatalf("compute = %+v, %v", run, err)
	}
	first := base
	if _, err := h.SignOffAggregate(hub.SignoffRequest{
		AggregateID: "hourly-temp", Operator: "alice", Intervals: []time.Time{first},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: recomputation reuses durable revisions, sign-off survives.
	reopen := func() *hub.Hub {
		s2, err := store.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		return hub.New(s2, hub.DefaultAdapters())
	}
	h2 := reopen()
	run, err = h2.ComputeAggregate("hourly-temp")
	if err != nil || run.Reused != 3 || len(run.Computed) != 0 {
		t.Fatalf("resume compute = %+v, %v", run, err)
	}
	resign, err := h2.SignOffAggregate(hub.SignoffRequest{
		AggregateID: "hourly-temp", Operator: "bob", Intervals: []time.Time{first},
	})
	if err != nil || resign.Results[0].Status != "already_current" || resign.Results[0].SignedBy != "alice" {
		t.Fatalf("resign = %+v, %v", resign, err)
	}
	// Partial completion must not masquerade as a fully re-reviewed batch.
	preview, err := h2.PreviewAggregateRevision("hourly-temp")
	if err != nil {
		t.Fatal(err)
	}
	statuses := map[string]int{}
	for _, interval := range preview.Intervals {
		statuses[interval.Status]++
	}
	if statuses[model.IntervalPublished] != 1 || statuses[model.IntervalUnpublished] != 2 {
		t.Fatalf("preview statuses = %v", statuses)
	}

	// Torn tail: a half-written record is dropped, durable state is intact.
	path := filepath.Join(dir, "journal.jsonl")
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	torn := append(append([]byte(nil), good...), []byte("{\"kind\":\"aggregate_interval\",partial")...)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatal(err)
	}
	h3 := reopen()
	run, err = h3.ComputeAggregate("hourly-temp")
	if err != nil || run.Reused != 3 || len(run.Computed) != 0 {
		t.Fatalf("torn resume compute = %+v, %v", run, err)
	}
	if got := len(h3.AggregateSignoffs("hourly-temp")); got != 1 {
		t.Fatalf("torn signoffs = %d", got)
	}

	// A sign-off whose record never reached disk is not reported as published.
	second := base.Add(time.Hour)
	if _, err := h3.SignOffAggregate(hub.SignoffRequest{
		AggregateID: "hourly-temp", Operator: "alice", Intervals: []time.Time{second},
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	truncated := strings.Join(lines[:len(lines)-1], "\n") + "\n"
	if err := os.WriteFile(path, []byte(truncated), 0o644); err != nil {
		t.Fatal(err)
	}
	h4 := reopen()
	if got := previewInterval(t, h4, "hourly-temp", second); got.Status != model.IntervalUnpublished {
		t.Fatalf("lost signoff interval = %+v", got)
	}
	resign, err = h4.SignOffAggregate(hub.SignoffRequest{
		AggregateID: "hourly-temp", Operator: "alice", Intervals: []time.Time{second},
	})
	if err != nil || resign.Results[0].Status != "signed" {
		t.Fatalf("re-sign after loss = %+v, %v", resign, err)
	}
}

func TestAggregateWithdrawalRestoreBasis(t *testing.T) {
	h, s, dir := newHub(t)
	defer s.Close()
	base := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	clock := base
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("temp-1", "hw-temp-1", device.TemperatureType, base); err != nil {
		t.Fatal(err)
	}
	receiveTemps(t, h, "temp-1", &clock, base,
		struct {
			offsetMin int
			celsius   float64
		}{10, 10}, struct {
			offsetMin int
			celsius   float64
		}{20, 20})
	defineHourly(t, h, "hourly-temp", "temp-1", base, base.Add(time.Hour))
	if _, err := h.ComputeAggregate("hourly-temp"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.SignOffAggregate(hub.SignoffRequest{AggregateID: "hourly-temp", Operator: "alice"}); err != nil {
		t.Fatal(err)
	}

	if _, err := h.CreateCorrectionBranch(hub.CorrectionRequest{
		ID: "calibration", Rule: correctedTempRule{}, Operator: "bob",
		Scope: model.BranchScope{IdentityID: "temp-1"},
	}); err != nil {
		t.Fatal(err)
	}
	adopted, err := h.AdoptBranch(hub.AdoptionRequest{BranchID: "calibration", Operator: "reviewer"})
	if err != nil || adopted.Adoption == nil {
		t.Fatalf("adopt = %+v, %v", adopted, err)
	}
	run, err := h.ComputeAggregate("hourly-temp")
	if err != nil || len(run.Computed) != 1 || run.Computed[0].Stats.Avg != 15.5 {
		t.Fatalf("corrected compute = %+v, %v", run, err)
	}
	// Every displayed value traces back to range, adoption and raw message.
	contribution := run.Computed[0].Contributions[0]
	if contribution.AdoptionID != adopted.Adoption.ID || contribution.RawRef.PayloadSHA == "" || contribution.BranchID != "calibration" {
		t.Fatalf("contribution trace = %+v", contribution)
	}
	if _, err := h.SignOffAggregate(hub.SignoffRequest{AggregateID: "hourly-temp", Operator: "alice"}); err != nil {
		t.Fatal(err)
	}

	withdrawal, err := h.WithdrawAdoption(adopted.Adoption.ID, "reviewer", "calibration recalled", model.BranchScope{})
	if err != nil || len(withdrawal.DeliveryIDs) != 2 {
		t.Fatalf("withdrawal = %+v, %v", withdrawal, err)
	}
	run, err = h.ComputeAggregate("hourly-temp")
	if err != nil || len(run.Computed) != 1 || run.Computed[0].Stats.Avg != 15 {
		t.Fatalf("restored compute = %+v, %v", run, err)
	}
	interval := previewInterval(t, h, "hourly-temp", base)
	if interval.Status != model.IntervalPendingReview || interval.Published == nil || interval.Published.Stats.Avg != 15.5 {
		t.Fatalf("restore preview = %+v", interval)
	}
	if len(interval.Changes) != 2 {
		t.Fatalf("restore changes = %+v", interval.Changes)
	}
	for _, change := range interval.Changes {
		if !change.Restored || change.WithdrawalID != withdrawal.ID || !strings.Contains(change.Detail, "calibration recalled") {
			t.Fatalf("restore basis = %+v", change)
		}
	}
	// Published old statistics are retained, never deleted.
	if got := len(h.AggregateSignoffs("hourly-temp")); got != 2 {
		t.Fatalf("signoffs retained = %d", got)
	}
	intervalID := run.Computed[0].ID
	for revision, wantAvg := range map[int]float64{1: 15, 2: 15.5, 3: 15} {
		stored, ok := s.AggregateIntervalRevision("hourly-temp", intervalID, revision)
		if !ok || stored.Stats.Avg != wantAvg {
			t.Fatalf("revision %d = %+v ok=%v", revision, stored, ok)
		}
	}
	// After restart the restore basis is still explainable.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	h2 := hub.New(s2, hub.DefaultAdapters())
	interval = previewInterval(t, h2, "hourly-temp", base)
	if interval.Status != model.IntervalPendingReview || len(interval.Changes) != 2 || !interval.Changes[0].Restored {
		t.Fatalf("restarted restore preview = %+v", interval)
	}
}
