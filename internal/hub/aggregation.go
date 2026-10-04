package hub

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"edge-ingest/internal/model"
)

type AggregationRequest struct {
	ID       string
	Name     string
	Operator string
	Windows  []model.AggregationWindow
}

type AggregationRevisionRequest struct {
	ID            string
	AggregationID string
	BranchID      string
	Operator      string
	Reason        string
}

type AggregationSignoffRequest struct {
	RevisionID string
	WindowID   string
	Operators  []string
	Reason     string
}

func (h *Hub) RegisterAggregation(req AggregationRequest) (model.Aggregation, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if req.ID == "" {
		return model.Aggregation{}, fmt.Errorf("aggregation id is required")
	}
	if len(req.Windows) == 0 {
		return model.Aggregation{}, fmt.Errorf("aggregation %s has no windows", req.ID)
	}
	windowIDs := map[string]struct{}{}
	windows := make([]model.AggregationWindow, 0, len(req.Windows))
	for _, window := range req.Windows {
		if window.ID == "" {
			return model.Aggregation{}, fmt.Errorf("aggregation window id is required")
		}
		if _, exists := windowIDs[window.ID]; exists {
			return model.Aggregation{}, fmt.Errorf("duplicate aggregation window %s", window.ID)
		}
		windowIDs[window.ID] = struct{}{}
		windows = append(windows, window)
	}
	now := h.now().UTC()
	aggregation := model.Aggregation{ID: req.ID, Name: req.Name, Operator: req.Operator, Windows: windows, CreatedAt: now}
	if err := h.store.SaveAggregation(aggregation); err != nil {
		return model.Aggregation{}, err
	}
	revision := model.AggregationRevision{
		ID: req.ID + ":baseline", AggregationID: req.ID, Operator: req.Operator,
		Status: model.AggregationRevisionSigned, CreatedAt: now,
	}
	for _, window := range windows {
		values := h.aggregationValuesLocked(window)
		revision.Windows = append(revision.Windows, model.AggregationWindowRevision{
			Window: window, Status: model.AggregationWindowSigned,
			Values: values, Statistics: aggregate(values),
			Signoffs: []model.AggregationSignoff{{
				ID: req.ID + ":" + window.ID + ":baseline", AggregationID: req.ID,
				RevisionID: req.ID + ":baseline", WindowID: window.ID,
				Operator: req.Operator, SignedAt: now,
			}},
		})
	}
	return aggregation, h.store.SaveRevision(revision)
}

func (h *Hub) PreviewAggregationRevision(req AggregationRevisionRequest) (model.AggregationRevision, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	aggregation, ok := h.store.Aggregation(req.AggregationID)
	if !ok {
		return model.AggregationRevision{}, fmt.Errorf("aggregation %s not found", req.AggregationID)
	}
	now := h.now().UTC()
	id := req.ID
	if id == "" {
		id = req.AggregationID + ":revision:" + strconv.FormatInt(now.UnixNano(), 36)
	}
	candidates := map[string]model.BranchEntry{}
	if req.BranchID != "" {
		if _, ok := h.store.Branch(req.BranchID); !ok {
			return model.AggregationRevision{}, fmt.Errorf("branch %s not found", req.BranchID)
		}
		for _, entry := range h.store.BranchEntries(req.BranchID) {
			if entry.Status == model.EntryExcluded {
				continue
			}
			candidates[entry.RawRef.DeliveryID] = entry
		}
	}
	if existing, ok := h.store.Revision(id); ok {
		if existing.AggregationID != req.AggregationID || existing.BranchID != req.BranchID {
			return model.AggregationRevision{}, fmt.Errorf("revision %s already exists for another aggregation or branch", id)
		}
		refreshed := existing
		refreshed.Windows = nil
		for _, oldWindow := range existing.Windows {
			if oldWindow.Status == model.AggregationWindowSigned {
				refreshed.Windows = append(refreshed.Windows, oldWindow)
				continue
			}
			window := oldWindow.Window
			baseline, _, _ := h.publishedWindowRevisionLocked(aggregation, window.ID)
			values := append([]model.AggregationValue(nil), baseline.Values...)
			byDelivery := map[string]model.AggregationValue{}
			for _, value := range values {
				byDelivery[value.DeliveryID] = value
			}
			var changed, pending []string
			for deliveryID, entry := range candidates {
				if !window.Contains(currentFromEntry(entry)) {
					continue
				}
				candidate := aggregationValueFromEntry(entry)
				current, isCurrent := h.store.CurrentValue(deliveryID)
				if !isCurrent || model.Fingerprint(current.Observation) != candidate.Fingerprint {
					pending = append(pending, deliveryID)
				} else {
					candidate.Source = current.Source
					candidate.AdoptionID = current.AdoptionID
				}
				if old, exists := byDelivery[deliveryID]; !exists || old.Fingerprint != candidate.Fingerprint {
					changed = append(changed, deliveryID)
				}
				byDelivery[deliveryID] = candidate
			}
			values = values[:0]
			for _, value := range byDelivery {
				values = append(values, value)
			}
			sort.Strings(changed)
			sort.Strings(pending)
			sort.SliceStable(values, func(i, j int) bool { return values[i].DeliveryID < values[j].DeliveryID })
			windowRevision := model.AggregationWindowRevision{
				Window: window, Status: model.AggregationWindowPending, Values: values,
				Statistics: aggregate(values), BaselineStatistics: aggregate(baseline.Values),
				ChangedDeliveryIDs: changed, PendingDeliveryIDs: pending,
			}
			if len(pending) != 0 {
				windowRevision.Reason = "candidate interpretation is not the formally adopted current value"
			} else {
				windowRevision.Reason = "awaiting signoff"
			}
			refreshed.Windows = append(refreshed.Windows, windowRevision)
		}
		if err := h.store.UpdateRevision(refreshed); err != nil {
			return model.AggregationRevision{}, err
		}
		return refreshed, nil
	}
	revision := model.AggregationRevision{
		ID: id, AggregationID: req.AggregationID, BranchID: req.BranchID,
		Rule: h.branchRuleLocked(req.BranchID), Operator: req.Operator, Reason: req.Reason,
		Status: model.AggregationRevisionDraft, CreatedAt: now,
	}
	for _, window := range aggregation.Windows {
		baselineWindow, _, _ := h.publishedWindowRevisionLocked(aggregation, window.ID)
		baseline := baselineWindow.Values
		if baseline == nil {
			baseline = h.aggregationValuesLocked(window)
		}
		values := append([]model.AggregationValue(nil), baseline...)
		byDelivery := map[string]model.AggregationValue{}
		for _, value := range values {
			byDelivery[value.DeliveryID] = value
		}
		var changed, pending []string
		for deliveryID, entry := range candidates {
			if !window.Contains(currentFromEntry(entry)) {
				continue
			}
			candidate := aggregationValueFromEntry(entry)
			current, isCurrent := h.store.CurrentValue(deliveryID)
			if !isCurrent || model.Fingerprint(current.Observation) != candidate.Fingerprint {
				pending = append(pending, deliveryID)
			} else {
				candidate.Source = current.Source
				candidate.AdoptionID = current.AdoptionID
			}
			if old, exists := byDelivery[deliveryID]; !exists || old.Fingerprint != candidate.Fingerprint {
				changed = append(changed, deliveryID)
			}
			byDelivery[deliveryID] = candidate
		}
		values = values[:0]
		for _, value := range byDelivery {
			values = append(values, value)
		}
		sort.Strings(changed)
		sort.Strings(pending)
		sort.SliceStable(values, func(i, j int) bool { return values[i].DeliveryID < values[j].DeliveryID })
		windowRevision := model.AggregationWindowRevision{
			Window: window, Values: values, Statistics: aggregate(values),
			BaselineStatistics: aggregate(baseline), ChangedDeliveryIDs: changed,
			PendingDeliveryIDs: pending,
		}
		windowRevision.Status = model.AggregationWindowPending
		if len(pending) == 0 {
			windowRevision.Reason = "awaiting signoff"
		} else {
			windowRevision.Reason = "candidate interpretation is not the formally adopted current value"
		}
		revision.Windows = append(revision.Windows, windowRevision)
	}
	return revision, h.store.SaveRevision(revision)
}

func (h *Hub) SignAggregationWindow(req AggregationSignoffRequest) ([]model.AggregationSignoff, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	revision, ok := h.store.Revision(req.RevisionID)
	if !ok {
		return nil, fmt.Errorf("revision %s not found", req.RevisionID)
	}
	var window *model.AggregationWindowRevision
	for i := range revision.Windows {
		if revision.Windows[i].Window.ID == req.WindowID {
			window = &revision.Windows[i]
			break
		}
	}
	if window == nil {
		return nil, fmt.Errorf("window %s not found in revision %s", req.WindowID, req.RevisionID)
	}
	if len(window.PendingDeliveryIDs) != 0 {
		return nil, fmt.Errorf("window %s contains interpretations that are not formally adopted: %v", req.WindowID, window.PendingDeliveryIDs)
	}
	currentValues := h.aggregationValuesLocked(window.Window)
	if !sameFingerprints(fingerprints(window.Values), fingerprints(currentValues)) {
		return nil, fmt.Errorf("revision %s window %s is stale; refresh the preview before signoff", req.RevisionID, req.WindowID)
	}
	operators := append([]string(nil), req.Operators...)
	if len(operators) == 0 {
		operators = []string{revision.Operator}
	}
	now := h.now().UTC()
	signoffs := make([]model.AggregationSignoff, 0, len(operators))
	existingByOperatorRevision := map[string]model.AggregationSignoff{}
	for _, existing := range h.store.WindowSignoffs(revision.AggregationID, req.WindowID) {
		existingByOperatorRevision[existing.Operator+"\x00"+existing.RevisionID] = existing
	}
	for i, operator := range operators {
		if operator == "" {
			return nil, fmt.Errorf("aggregation signoff operator is required")
		}
		if existing, ok := existingByOperatorRevision[operator+"\x00"+req.RevisionID]; ok {
			signoffs = append(signoffs, existing)
			continue
		}
		signoff := model.AggregationSignoff{
			ID:            req.RevisionID + ":" + req.WindowID + ":" + strconv.Itoa(i) + ":" + strconv.FormatInt(now.UnixNano(), 36),
			AggregationID: revision.AggregationID, RevisionID: revision.ID, WindowID: req.WindowID,
			Operator: operator, Reason: req.Reason, SignedAt: now,
		}
		if err := h.store.SaveAggregationSignoff(signoff); err != nil {
			return signoffs, err
		}
		signoffs = append(signoffs, signoff)
	}
	return signoffs, nil
}

func (h *Hub) AggregationResult(aggregationID string) (model.AggregationResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	aggregation, ok := h.store.Aggregation(aggregationID)
	if !ok {
		return model.AggregationResult{}, fmt.Errorf("aggregation %s not found", aggregationID)
	}
	result := model.AggregationResult{Aggregation: aggregation}
	for _, window := range aggregation.Windows {
		published, publishedRevisionID, _ := h.publishedWindowRevisionLocked(aggregation, window.ID)
		currentValues := h.aggregationValuesLocked(window)
		if publishedRevisionID == "" {
			publishedRevisionID = aggregationID + ":baseline"
		}
		windowResult := model.AggregationWindowResult{
			Window: window, Status: published.Status, RevisionID: publishedRevisionID,
			Statistics: published.Statistics, Values: published.Values,
		}
		if !sameFingerprints(fingerprints(published.Values), fingerprints(currentValues)) {
			windowResult.Status = model.AggregationWindowRestore
			windowResult.Reason = "current interpretation changed after publication; restore values show the available recovery basis"
			windowResult.RestoreFrom = currentValues
		} else if reason := h.pendingAggregationReasonLocked(aggregationID, window.ID); reason != "" {
			windowResult.Status = model.AggregationWindowPending
			windowResult.Reason = reason
		}
		result.Windows = append(result.Windows, windowResult)
	}
	return result, nil
}

func (h *Hub) publishedWindowRevisionLocked(aggregation model.Aggregation, windowID string) (model.AggregationWindowRevision, string, time.Time) {
	published := model.AggregationWindowRevision{}
	publishedRevisionID := aggregation.ID + ":baseline"
	if baseline, ok := h.store.Revision(aggregation.ID + ":baseline"); ok {
		for _, candidate := range baseline.Windows {
			if candidate.Window.ID == windowID {
				published = candidate
			}
		}
	}
	latest := time.Time{}
	for _, signoff := range h.store.WindowSignoffs(aggregation.ID, windowID) {
		signedRevision, ok := h.store.Revision(signoff.RevisionID)
		if !ok {
			continue
		}
		for _, candidate := range signedRevision.Windows {
			if candidate.Window.ID == windowID && signoff.SignedAt.After(latest) {
				published = candidate
				publishedRevisionID = signedRevision.ID
				latest = signoff.SignedAt
			}
		}
	}
	return published, publishedRevisionID, latest
}

func (h *Hub) pendingAggregationReasonLocked(aggregationID, windowID string) string {
	var latest time.Time
	reason := ""
	for _, revision := range h.store.Revisions(aggregationID) {
		if revision.Status == model.AggregationRevisionSigned || revision.ID == aggregationID+":baseline" {
			continue
		}
		for _, window := range revision.Windows {
			if window.Window.ID != windowID {
				continue
			}
			if revision.CreatedAt.After(latest) {
				latest = revision.CreatedAt
				reason = window.Reason
			}
		}
	}
	return reason
}

func (h *Hub) branchRuleLocked(branchID string) string {
	if branchID == "" {
		return ""
	}
	branch, ok := h.store.Branch(branchID)
	if !ok {
		return ""
	}
	return branch.Rule
}

func (h *Hub) aggregationValuesLocked(window model.AggregationWindow) []model.AggregationValue {
	var out []model.AggregationValue
	for _, current := range h.store.CurrentValues(model.BranchScope{IdentityID: window.IdentityID, BootID: window.BootID}) {
		if window.Contains(current) {
			out = append(out, aggregationValueFromCurrent(current))
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].DeliveryID < out[j].DeliveryID })
	return out
}

func aggregationValueFromCurrent(current model.CurrentValue) model.AggregationValue {
	return model.AggregationValue{
		DeliveryID: current.DeliveryID, RawRef: current.RawRef, Observation: current.Observation,
		Source: current.Source, BranchID: current.BranchID, Rule: current.Rule,
		AdoptionID: current.AdoptionID, Fingerprint: model.Fingerprint(current.Observation),
	}
}

func aggregationValueFromEntry(entry model.BranchEntry) model.AggregationValue {
	return model.AggregationValue{
		DeliveryID: entry.RawRef.DeliveryID, RawRef: entry.RawRef, Observation: entry.Observation,
		Source: "branch", BranchID: entry.BranchID, Rule: entry.Rule,
		AdoptionID: "", Fingerprint: model.Fingerprint(entry.Observation),
	}
}

func currentFromEntry(entry model.BranchEntry) model.CurrentValue {
	return model.CurrentValue{RawRef: entry.RawRef, Observation: entry.Observation}
}

func aggregate(values []model.AggregationValue) map[string]model.AggregateMeasure {
	out := map[string]model.AggregateMeasure{}
	for _, value := range values {
		for name, number := range value.Observation.Measures {
			measure := out[name]
			measure.Count++
			measure.Sum += number
			if measure.Count == 1 || number < measure.Min {
				measure.Min = number
			}
			if number > measure.Max {
				measure.Max = number
			}
			measure.Avg = measure.Sum / float64(measure.Count)
			out[name] = measure
		}
	}
	return out
}

func fingerprints(values []model.AggregationValue) map[string]string {
	out := map[string]string{}
	for _, value := range values {
		out[value.DeliveryID] = value.Fingerprint
	}
	return out
}

func sameFingerprints(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
