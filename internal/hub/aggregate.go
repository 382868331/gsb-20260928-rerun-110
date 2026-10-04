package hub

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"edge-ingest/internal/model"
)

type AggregateRequest struct {
	ID         string
	Name       string
	Operator   string
	IdentityID string
	Measure    string
	From       time.Time
	To         time.Time
	Interval   time.Duration
}

// DefineAggregate registers a downstream time-bucketed statistic. Defining
// the same ID twice is a no-op so an interrupted setup can be retried.
func (h *Hub) DefineAggregate(req AggregateRequest) (model.AggregateSpec, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if req.Measure == "" {
		return model.AggregateSpec{}, errors.New("aggregate measure is required")
	}
	if req.Interval <= 0 {
		return model.AggregateSpec{}, errors.New("aggregate interval must be positive")
	}
	if !req.To.After(req.From) {
		return model.AggregateSpec{}, errors.New("aggregate range end must be after start")
	}
	id := req.ID
	if id == "" {
		id = "agg-" + strconv.FormatInt(h.now().UTC().UnixNano(), 36)
	}
	if existing, ok := h.store.Aggregate(id); ok {
		return existing, nil
	}
	spec := model.AggregateSpec{
		ID: id, Name: req.Name, Operator: req.Operator, IdentityID: req.IdentityID,
		Measure: req.Measure, From: req.From, To: req.To, Interval: req.Interval,
		CreatedAt: h.now().UTC(),
	}
	return spec, h.store.SaveAggregate(spec)
}

func (h *Hub) Aggregates() []model.AggregateSpec { return h.store.Aggregates() }

func (h *Hub) AggregateIntervals(aggregateID string) []model.AggregateInterval {
	return h.store.AggregateIntervals(aggregateID)
}

func (h *Hub) AggregateSignoffs(aggregateID string) []model.AggregateSignoff {
	return h.store.AggregateSignoffs(aggregateID)
}

type AggregateRunReport struct {
	AggregateID string                    `json:"aggregate_id"`
	Computed    []model.AggregateInterval `json:"computed,omitempty"`
	Reused      int                       `json:"reused"`
}

// ComputeAggregate materializes one revision per interval from the current
// observation values. Each delivery contributes exactly once, taken from its
// single current value, so an old and a new interpretation of the same raw
// message can never be double-counted in one statistic. Intervals whose
// inputs are unchanged reuse the durable revision, which lets an interrupted
// run resume without duplicating already persisted results.
func (h *Hub) ComputeAggregate(id string) (AggregateRunReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	spec, ok := h.store.Aggregate(id)
	if !ok {
		return AggregateRunReport{}, fmt.Errorf("aggregate %s not found", id)
	}
	var currents []model.CurrentValue
	for _, current := range h.store.CurrentValues(model.BranchScope{}) {
		if spec.IdentityID != "" && current.RawRef.IdentityID != spec.IdentityID {
			continue
		}
		observedAt := current.Observation.ObservedAt
		if observedAt.Before(spec.From) || !observedAt.Before(spec.To) {
			continue
		}
		currents = append(currents, current)
	}
	sort.SliceStable(currents, func(i, j int) bool { return currents[i].DeliveryID < currents[j].DeliveryID })
	latest := map[string]model.AggregateInterval{}
	for _, interval := range h.store.AggregateIntervals(id) {
		latest[interval.ID] = interval
	}
	report := AggregateRunReport{AggregateID: id}
	now := h.now().UTC()
	for start := spec.From; start.Before(spec.To); start = start.Add(spec.Interval) {
		end := start.Add(spec.Interval)
		if end.After(spec.To) {
			end = spec.To
		}
		interval := buildInterval(spec, start, end, currents, now)
		previous, exists := latest[interval.ID]
		if exists {
			interval.Revision = previous.Revision
			if previous.Fingerprint == interval.Fingerprint {
				report.Reused++
				continue
			}
			interval.Revision = previous.Revision + 1
		} else {
			interval.Revision = 1
		}
		if err := h.store.SaveAggregateInterval(interval); err != nil {
			return report, err
		}
		report.Computed = append(report.Computed, interval)
	}
	return report, nil
}

func intervalID(aggregateID string, start time.Time) string {
	return aggregateID + ":" + strconv.FormatInt(start.Unix(), 10)
}

func buildInterval(spec model.AggregateSpec, start, end time.Time, currents []model.CurrentValue, now time.Time) model.AggregateInterval {
	interval := model.AggregateInterval{
		ID: intervalID(spec.ID, start), AggregateID: spec.ID,
		Start: start, End: end, ComputedAt: now,
	}
	_, exactUnit := model.SplitMeasureName(spec.Measure)
	stats := model.IntervalStats{}
	for _, current := range currents {
		observedAt := current.Observation.ObservedAt
		if observedAt.Before(start) || !observedAt.Before(end) {
			continue
		}
		var matches []string
		for name := range current.Observation.Measures {
			base, _ := model.SplitMeasureName(name)
			if exactUnit != "" {
				if name == spec.Measure {
					matches = append(matches, name)
				}
			} else if base == spec.Measure {
				matches = append(matches, name)
			}
		}
		sort.Strings(matches)
		switch {
		case len(matches) == 0:
			continue
		case len(matches) > 1:
			interval.Skipped = append(interval.Skipped, model.SkippedContribution{
				DeliveryID: current.DeliveryID,
				Reason:     "ambiguous measures: " + joinNames(matches),
			})
			continue
		}
		name := matches[0]
		_, unit := model.SplitMeasureName(name)
		value := current.Observation.Measures[name]
		if interval.Unit == "" {
			interval.Unit = unit
		} else if interval.Unit != unit {
			interval.MixedUnits = true
			interval.Skipped = append(interval.Skipped, model.SkippedContribution{
				DeliveryID: current.DeliveryID,
				Reason:     fmt.Sprintf("unit %s conflicts with interval unit %s", unit, interval.Unit),
			})
			continue
		}
		interval.Contributions = append(interval.Contributions, model.Contribution{
			DeliveryID: current.DeliveryID, RawRef: current.RawRef,
			Measure: name, Unit: unit, Value: value,
			Source: current.Source, BranchID: current.BranchID,
			AdoptionID: current.AdoptionID, Fingerprint: model.Fingerprint(current.Observation),
		})
		if stats.Count == 0 || value < stats.Min {
			stats.Min = value
		}
		if stats.Count == 0 || value > stats.Max {
			stats.Max = value
		}
		stats.Count++
		stats.Sum += value
	}
	if stats.Count != 0 {
		stats.Avg = stats.Sum / float64(stats.Count)
	}
	interval.Stats = stats
	interval.Fingerprint = interval.ContentFingerprint()
	return interval
}

func joinNames(names []string) string {
	out := ""
	for i, name := range names {
		if i != 0 {
			out += ","
		}
		out += name
	}
	return out
}

// ObservationChange explains how one delivery's contribution differs between
// the published interval revision and the latest computed candidate.
type ObservationChange struct {
	DeliveryID   string              `json:"delivery_id"`
	Change       string              `json:"change"` // value_changed | unit_changed | added | removed
	Old          *model.Contribution `json:"old,omitempty"`
	New          *model.Contribution `json:"new,omitempty"`
	Restored     bool                `json:"restored,omitempty"`
	WithdrawalID string              `json:"withdrawal_id,omitempty"`
	Detail       string              `json:"detail,omitempty"`
}

// BlockedInterpretation is a new explanation that exists on a correction
// branch but cannot enter the official statistic yet, with the reason why.
type BlockedInterpretation struct {
	DeliveryID string `json:"delivery_id"`
	BranchID   string `json:"branch_id"`
	Rule       string `json:"rule"`
	Change     string `json:"change"`
	Reason     string `json:"reason"`
}

// IntervalPreview shows one interval's published statistic, the pending
// candidate revision and every observation-level explanation for the delta.
type IntervalPreview struct {
	IntervalID string                   `json:"interval_id"`
	Start      time.Time                `json:"start"`
	End        time.Time                `json:"end"`
	Status     string                   `json:"status"` // published | pending_review | unpublished
	Published  *model.AggregateInterval `json:"published,omitempty"`
	Candidate  *model.AggregateInterval `json:"candidate,omitempty"`
	Changes    []ObservationChange      `json:"changes,omitempty"`
	Blocked    []BlockedInterpretation  `json:"blocked,omitempty"`
	Note       string                   `json:"note,omitempty"`
}

type AggregatePreview struct {
	AggregateID string              `json:"aggregate_id"`
	Spec        model.AggregateSpec `json:"spec"`
	Intervals   []IntervalPreview   `json:"intervals"`
}

// PreviewAggregateRevision compares, per interval, the published statistic
// with the latest computed candidate. It lists which raw observations changed
// interpretation, which interval statistics are affected, and which new
// explanations are still blocked from the official result. Published old
// revisions are never deleted; they stay visible as the retained baseline.
func (h *Hub) PreviewAggregateRevision(id string) (AggregatePreview, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	spec, ok := h.store.Aggregate(id)
	if !ok {
		return AggregatePreview{}, fmt.Errorf("aggregate %s not found", id)
	}
	published := map[string]model.AggregateInterval{}
	for _, signoff := range h.store.AggregateSignoffs(id) {
		if revision, ok := h.store.AggregateIntervalRevision(id, signoff.IntervalID, signoff.Revision); ok {
			published[signoff.IntervalID] = revision
		}
	}
	blocked := h.blockedInterpretationsLocked(spec)
	preview := AggregatePreview{AggregateID: id, Spec: spec}
	for _, candidate := range h.store.AggregateIntervals(id) {
		interval := IntervalPreview{
			IntervalID: candidate.ID, Start: candidate.Start, End: candidate.End,
			Blocked: blocked[candidate.ID],
		}
		candidateCopy := candidate
		interval.Candidate = &candidateCopy
		base, hasPublished := published[candidate.ID]
		switch {
		case !hasPublished:
			interval.Status = model.IntervalUnpublished
			interval.Note = fmt.Sprintf("revision %d computed but never signed off", candidate.Revision)
		case base.Revision == candidate.Revision:
			interval.Status = model.IntervalPublished
			publishedCopy := base
			interval.Published = &publishedCopy
		default:
			interval.Status = model.IntervalPendingReview
			publishedCopy := base
			interval.Published = &publishedCopy
			interval.Changes = diffContributions(h.store, base, candidate)
			interval.Note = fmt.Sprintf("revision %d awaiting sign-off; published revision %d retained", candidate.Revision, base.Revision)
		}
		preview.Intervals = append(preview.Intervals, interval)
	}
	return preview, nil
}

// diffContributions compares published and candidate contributions per
// delivery. A delivery appears at most once on each side, so an old and a new
// interpretation of the same observation can never both be counted.
func diffContributions(s interface {
	CurrentValue(string) (model.CurrentValue, bool)
}, published, candidate model.AggregateInterval) []ObservationChange {
	oldByDelivery := map[string]model.Contribution{}
	for _, contribution := range published.Contributions {
		oldByDelivery[contribution.DeliveryID] = contribution
	}
	newByDelivery := map[string]model.Contribution{}
	for _, contribution := range candidate.Contributions {
		newByDelivery[contribution.DeliveryID] = contribution
	}
	ids := map[string]struct{}{}
	for id := range oldByDelivery {
		ids[id] = struct{}{}
	}
	for id := range newByDelivery {
		ids[id] = struct{}{}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	var changes []ObservationChange
	for _, deliveryID := range ordered {
		oldContribution, hadOld := oldByDelivery[deliveryID]
		newContribution, hasNew := newByDelivery[deliveryID]
		change := ObservationChange{DeliveryID: deliveryID}
		switch {
		case hadOld && hasNew:
			if oldContribution.Fingerprint == newContribution.Fingerprint {
				continue
			}
			oldC, newC := oldContribution, newContribution
			change.Old, change.New = &oldC, &newC
			if oldContribution.Unit != newContribution.Unit || oldContribution.Measure != newContribution.Measure {
				change.Change = model.ChangeUnit
				change.Detail = fmt.Sprintf("%s %v (%s) -> %s %v (%s)", oldContribution.Measure, oldContribution.Value, oldContribution.Unit, newContribution.Measure, newContribution.Value, newContribution.Unit)
			} else {
				change.Change = model.ChangeValue
				change.Detail = fmt.Sprintf("%s %v -> %v", oldContribution.Measure, oldContribution.Value, newContribution.Value)
			}
		case hasNew:
			newC := newContribution
			change.Change, change.New = "added", &newC
			change.Detail = fmt.Sprintf("%s %v now contributes", newContribution.Measure, newContribution.Value)
		default:
			oldC := oldContribution
			change.Change, change.Old = "removed", &oldC
			change.Detail = fmt.Sprintf("%s %v no longer contributes", oldContribution.Measure, oldContribution.Value)
		}
		if current, ok := s.CurrentValue(deliveryID); ok && current.Withdrawn {
			change.Restored = true
			change.WithdrawalID = current.WithdrawalID
			change.Detail += "; restored by withdrawal " + current.WithdrawalID
			if current.WithdrawalReason != "" {
				change.Detail += " (" + current.WithdrawalReason + ")"
			}
		}
		changes = append(changes, change)
	}
	return changes
}

// blockedInterpretationsLocked lists branch explanations that would change an
// interval but are not in effect, bucketed by interval ID.
func (h *Hub) blockedInterpretationsLocked(spec model.AggregateSpec) map[string][]BlockedInterpretation {
	out := map[string][]BlockedInterpretation{}
	for _, branch := range h.store.Branches() {
		for _, entry := range h.store.BranchEntries(branch.ID) {
			if entry.Status == model.EntryExcluded {
				continue
			}
			ref := entry.RawRef
			if spec.IdentityID != "" && ref.IdentityID != spec.IdentityID {
				continue
			}
			observedAt := entry.Observation.ObservedAt
			if observedAt.Before(spec.From) || !observedAt.Before(spec.To) {
				continue
			}
			current, ok := h.store.CurrentValue(ref.DeliveryID)
			if ok && current.Source == "branch" && current.BranchID == branch.ID && !current.Withdrawn {
				continue // this branch's explanation is already in effect
			}
			if ok && model.Fingerprint(current.Observation) == model.Fingerprint(entry.Observation) {
				continue
			}
			reason := "correction branch not adopted"
			switch {
			case branch.Status == model.BranchAbandoned:
				reason = "correction branch abandoned"
			case ok && current.Source == "branch" && current.BranchID != branch.ID:
				reason = "conflicts with adopted branch " + current.BranchID
			}
			bucket := intervalID(spec.ID, spec.From.Add(observedAt.Sub(spec.From)/spec.Interval*spec.Interval))
			out[bucket] = append(out[bucket], BlockedInterpretation{
				DeliveryID: ref.DeliveryID, BranchID: branch.ID, Rule: entry.Rule,
				Change: entry.Comparison.Change, Reason: reason,
			})
		}
	}
	return out
}

// SignoffRequest signs off candidate interval revisions. Intervals selects
// interval starts; an empty slice signs every computed interval.
type SignoffRequest struct {
	AggregateID string
	Operator    string
	Note        string
	Intervals   []time.Time
}

type IntervalSignoff struct {
	IntervalID string    `json:"interval_id"`
	Start      time.Time `json:"start"`
	Revision   int       `json:"revision"`
	Status     string    `json:"status"` // signed | already_current | no_candidate
	SignedBy   string    `json:"signed_by,omitempty"`
}

type SignoffResult struct {
	AggregateID string            `json:"aggregate_id"`
	Results     []IntervalSignoff `json:"results"`
}

// SignOffAggregate publishes the candidate revision of the selected
// intervals only. Unselected intervals keep their previously published
// revision and stay flagged as pending in the preview. Signing one aggregate
// never confirms another aggregate that shares the same observations. Each
// interval is journaled atomically, so an interrupted batch can be retried
// and already durable sign-offs are reported as already_current instead of
// being duplicated.
func (h *Hub) SignOffAggregate(req SignoffRequest) (SignoffResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.store.Aggregate(req.AggregateID); !ok {
		return SignoffResult{}, fmt.Errorf("aggregate %s not found", req.AggregateID)
	}
	if req.Operator == "" {
		return SignoffResult{}, errors.New("sign-off operator is required")
	}
	candidates := map[string]model.AggregateInterval{}
	var starts []time.Time
	for _, interval := range h.store.AggregateIntervals(req.AggregateID) {
		candidates[interval.ID] = interval
		starts = append(starts, interval.Start)
	}
	if len(req.Intervals) != 0 {
		starts = append([]time.Time(nil), req.Intervals...)
		sort.SliceStable(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	}
	latestSignoff := map[string]model.AggregateSignoff{}
	for _, signoff := range h.store.AggregateSignoffs(req.AggregateID) {
		latestSignoff[signoff.IntervalID] = signoff
	}
	result := SignoffResult{AggregateID: req.AggregateID}
	now := h.now().UTC()
	for _, start := range starts {
		id := intervalID(req.AggregateID, start)
		entry := IntervalSignoff{IntervalID: id, Start: start}
		candidate, ok := candidates[id]
		if !ok {
			entry.Status = "no_candidate"
			result.Results = append(result.Results, entry)
			continue
		}
		entry.Revision = candidate.Revision
		if previous, signed := latestSignoff[id]; signed && previous.Revision == candidate.Revision {
			entry.Status = "already_current"
			entry.SignedBy = previous.Operator
			result.Results = append(result.Results, entry)
			continue
		}
		signoff := model.AggregateSignoff{
			ID:          id + ":sign:" + strconv.Itoa(candidate.Revision),
			AggregateID: req.AggregateID, IntervalID: id, Start: candidate.Start,
			Revision: candidate.Revision, Operator: req.Operator, Note: req.Note,
			SignedAt: now,
		}
		if err := h.store.SaveAggregateSignoff(signoff); err != nil {
			return result, err
		}
		latestSignoff[id] = signoff
		entry.Status = "signed"
		entry.SignedBy = req.Operator
		result.Results = append(result.Results, entry)
	}
	return result, nil
}
