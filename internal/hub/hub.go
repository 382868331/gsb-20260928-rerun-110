package hub

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/device"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

type Ack struct {
	IdentityID        string `json:"identity_id"`
	BootID            string `json:"boot_id"`
	Sequence          int64  `json:"sequence"`
	Duplicate         bool   `json:"duplicate"`
	Stored            bool   `json:"stored"`
	Pending           bool   `json:"pending"`
	ReceivedSequence  int64  `json:"received_sequence"`
	ConfirmedSequence int64  `json:"confirmed_sequence"`
}

type ResumeState struct {
	BootID                string  `json:"boot_id"`
	NextSequence          int64   `json:"next_sequence"`
	NextReceivedSequence  int64   `json:"next_received_sequence"`
	NextConfirmedSequence int64   `json:"next_confirmed_sequence,omitempty"`
	ConfirmedSequence     int64   `json:"confirmed_sequence,omitempty"`
	HasProgress           bool    `json:"has_progress"`
	HasConfirmedProgress  bool    `json:"has_confirmed_progress"`
	Missing               []int64 `json:"missing,omitempty"`
	Pending               []int64 `json:"pending,omitempty"`
}

type ReprocessResult struct {
	Processed []Ack  `json:"processed"`
	Remaining int    `json:"remaining"`
	FailedAt  *Ack   `json:"failed_at,omitempty"`
	Err       string `json:"error,omitempty"`
}

type ReinterpretSelector struct {
	IdentityID     string     `json:"identity_id,omitempty"`
	BootID         string     `json:"boot_id,omitempty"`
	Format         string     `json:"format,omitempty"`
	ReceivedAfter  *time.Time `json:"received_after,omitempty"`
	ReceivedBefore *time.Time `json:"received_before,omitempty"`
	IncludePending bool       `json:"include_pending,omitempty"`
}

type ReinterpretItem struct {
	IdentityID string `json:"identity_id"`
	BootID     string `json:"boot_id"`
	Sequence   int64  `json:"sequence"`
	DeliveryID string `json:"delivery_id"`
	Format     string `json:"format"`
	Backfill   bool   `json:"backfill"`
	Skipped    bool   `json:"skipped"`
}

type ReinterpretReport struct {
	RunID       string              `json:"run_id"`
	Status      string              `json:"status"`
	Scope       ReinterpretSelector `json:"scope"`
	ScopeNote   string              `json:"scope_note,omitempty"`
	Selected    int                 `json:"selected"`
	Published   []ReinterpretItem   `json:"published,omitempty"`
	AlreadyDone []ReinterpretItem   `json:"already_done,omitempty"`
	Remaining   int                 `json:"remaining"`
	FailedAt    *ReinterpretItem    `json:"failed_at,omitempty"`
	Err         string              `json:"error,omitempty"`
}

type MeasureChange struct {
	Old     float64 `json:"old"`
	New     float64 `json:"new"`
	OldUnit string  `json:"old_unit,omitempty"`
	NewUnit string  `json:"new_unit,omitempty"`
}

type InterpretationDifference struct {
	IdentityID         string                   `json:"identity_id"`
	BootID             string                   `json:"boot_id"`
	Sequence           int64                    `json:"sequence"`
	DeliveryID         string                   `json:"delivery_id"`
	PayloadSHA         string                   `json:"payload_sha256"`
	Kinds              []string                 `json:"kinds"`
	ChangedMeasures    map[string]MeasureChange `json:"changed_measures,omitempty"`
	ChangedUnits       []string                 `json:"changed_units,omitempty"`
	ExplanationChanged bool                     `json:"explanation_changed"`
	Backfill           bool                     `json:"backfill,omitempty"`
	Old                *model.Observation       `json:"old,omitempty"`
	New                model.Observation        `json:"new"`
}

type Hub struct {
	mu       sync.Mutex
	store    *store.Store
	adapters *adapter.Registry
	now      func() time.Time
}

func New(s *store.Store, adapters *adapter.Registry) *Hub {
	return &Hub{store: s, adapters: adapters, now: time.Now}
}

func (h *Hub) SetClock(now func() time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = now
}

func (h *Hub) RegisterAdapter(a adapter.Adapter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.adapters.Register(a)
}

func (h *Hub) Provision(id, hardwareID, deviceType string, now time.Time) error {
	if active, ok := h.store.ActiveHardware(hardwareID); ok && active.ID != id {
		return fmt.Errorf("hardware %s already active as %s", hardwareID, active.ID)
	}
	identity := model.DeviceIdentity{
		ID:         id,
		HardwareID: hardwareID,
		Type:       deviceType,
		Active:     true,
		CreatedAt:  now,
	}
	return h.store.RegisterIdentity(identity)
}

func (h *Hub) UpgradeIdentity(identityID, deviceType string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	identity, err := h.store.GetIdentity(identityID)
	if err != nil {
		return err
	}
	if !identity.Active {
		return fmt.Errorf("identity %s has been replaced", identityID)
	}
	identity.Type = deviceType
	return h.store.UpdateIdentity(identity)
}

func (h *Hub) ReplaceIdentity(oldID, newID string, now time.Time) (model.DeviceIdentity, error) {
	old, err := h.store.GetIdentity(oldID)
	if err != nil {
		return model.DeviceIdentity{}, err
	}
	if !old.Active {
		return model.DeviceIdentity{}, fmt.Errorf("identity %s is not active", oldID)
	}
	return h.store.ReplaceIdentity(oldID, newID, old.HardwareID, old.Type, now)
}

func (h *Hub) Resume(identityID, bootID string) (ResumeState, error) {
	identity, err := h.store.GetIdentity(identityID)
	if err != nil {
		return ResumeState{}, err
	}
	if !identity.Active {
		return ResumeState{}, fmt.Errorf("identity %s has been replaced", identityID)
	}
	progress, ok := h.store.Sequence(identityID, bootID)
	confirmed, hasConfirmed := h.store.Confirmed(identityID, bootID)
	if !ok {
		return ResumeState{BootID: bootID}, nil
	}
	var pending []int64
	for _, raw := range h.store.Pending() {
		if raw.IdentityID == identityID && raw.BootID == bootID {
			pending = append(pending, raw.Sequence)
		}
	}
	nextConfirmed := progress.Start
	confirmedLast := int64(0)
	if hasConfirmed {
		nextConfirmed = confirmed.Last + 1
		confirmedLast = confirmed.Last
	}
	return ResumeState{
		BootID:                bootID,
		NextSequence:          progress.Last + 1,
		NextReceivedSequence:  progress.Last + 1,
		NextConfirmedSequence: nextConfirmed,
		ConfirmedSequence:     confirmedLast,
		HasProgress:           true,
		HasConfirmedProgress:  hasConfirmed,
		Missing:               h.store.Missing(identityID, bootID),
		Pending:               pending,
	}, nil
}

func (h *Hub) Receive(identityID string, delivery model.RawDelivery) (Ack, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	identity, err := h.store.GetIdentity(identityID)
	if err != nil {
		return Ack{}, err
	}
	if !identity.Active {
		return Ack{}, fmt.Errorf("identity %s has been replaced", identityID)
	}
	receivedAt := h.now().UTC()
	format := delivery.Format
	if format == "" {
		format = identity.Type
	}
	raw := model.RawMessage{
		DeliveryID:  fmt.Sprintf("%s:%s:%d", identityID, delivery.BootID, delivery.Sequence),
		IdentityID:  identityID,
		HardwareID:  identity.HardwareID,
		BootID:      delivery.BootID,
		Sequence:    delivery.Sequence,
		Format:      format,
		ContentType: delivery.ContentType,
		Payload:     append([]byte(nil), delivery.Payload...),
		ReceivedAt:  receivedAt,
	}
	codec, err := h.adapters.Get(format)
	if err != nil {
		if !errors.Is(err, adapter.ErrUnknownDeviceType) {
			return Ack{}, err
		}
		result, err := h.store.StorePending(raw)
		if err != nil {
			return Ack{}, err
		}
		return h.rawAck(raw, result), nil
	}
	observation, err := codec.Decode(raw)
	if err != nil {
		return Ack{}, err
	}
	observation = h.normalizeObservation(raw, observation)
	result, err := h.store.Ingest(raw, &observation)
	if err != nil {
		return Ack{}, err
	}
	return h.rawAck(raw, result), nil
}

func (h *Hub) rawAck(raw model.RawMessage, result store.IngestResult) Ack {
	ack := Ack{
		IdentityID: raw.IdentityID,
		BootID:     raw.BootID,
		Sequence:   raw.Sequence,
		Stored:     true,
		Duplicate:  result.Duplicate,
		Pending:    result.Pending,
	}
	confirmed, _ := h.store.Confirmed(raw.IdentityID, raw.BootID)
	progress, _ := h.store.Sequence(raw.IdentityID, raw.BootID)
	ack.ReceivedSequence = progress.Last
	ack.ConfirmedSequence = confirmed.Last
	return ack
}

func (h *Hub) ReprocessPending() (ReprocessResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := ReprocessResult{}
	pending := h.store.Pending()
	for _, raw := range pending {
		identity, err := h.store.GetIdentity(raw.IdentityID)
		if err != nil {
			return result, err
		}
		if raw.HardwareID != "" && raw.HardwareID != identity.HardwareID {
			return result, fmt.Errorf("raw message hardware %s does not match identity %s", raw.HardwareID, identity.HardwareID)
		}
		format := raw.Format
		if format == "" {
			format = identity.Type
			raw.Format = format
		}
		codec, err := h.adapters.Get(format)
		if errors.Is(err, adapter.ErrUnknownDeviceType) {
			result.FailedAt = pendingAck(raw)
			result.Err = err.Error()
			result.Remaining = h.store.PendingCount()
			return result, nil
		}
		if err != nil {
			return result, err
		}
		observation, err := codec.Decode(raw)
		if err != nil {
			result.FailedAt = pendingAck(raw)
			result.Err = err.Error()
			result.Remaining = h.store.PendingCount()
			return result, nil
		}
		observation = h.normalizeObservation(raw, observation)
		confirmed, err := h.store.Confirm(raw, observation)
		if err != nil {
			return result, err
		}
		ack := Ack{
			IdentityID: raw.IdentityID,
			BootID:     raw.BootID,
			Sequence:   raw.Sequence,
			Stored:     true,
			Duplicate:  confirmed.Duplicate,
		}
		result.Processed = append(result.Processed, ack)
	}
	result.Remaining = h.store.PendingCount()
	return result, nil
}

func (h *Hub) normalizeObservation(raw model.RawMessage, observation model.Observation) model.Observation {
	if observation.IdentityID == "" {
		observation.IdentityID = raw.IdentityID
	}
	if observation.HardwareID == "" {
		observation.HardwareID = raw.HardwareID
	}
	if observation.Type == "" {
		observation.Type = raw.Format
	}
	if observation.ReceivedAt.IsZero() {
		observation.ReceivedAt = raw.ReceivedAt
	}
	if observation.InterpretedAt.IsZero() {
		observation.InterpretedAt = h.now().UTC()
	}
	if observation.RuleID == "" {
		observation.RuleID = raw.Format
	}
	return observation
}

func (h *Hub) Reinterpret(runID string, selector ReinterpretSelector) (ReinterpretReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if runID == "" {
		return ReinterpretReport{}, fmt.Errorf("reinterpret run id is required")
	}
	var run store.ReinterpretRun
	if existingRun, ok := h.store.GetReinterpretRun(runID); ok {
		selector = selectorFromRun(existingRun)
		if existingRun.Status == "completed" {
			return h.reinterpretReport(runID, existingRun, selector), nil
		}
		run = existingRun
	} else {
		run = store.ReinterpretRun{
			ID:             runID,
			IdentityID:     selector.IdentityID,
			BootID:         selector.BootID,
			Format:         selector.Format,
			ReceivedAfter:  selector.ReceivedAfter,
			ReceivedBefore: selector.ReceivedBefore,
			IncludePending: selector.IncludePending,
			Status:         "running",
			CreatedAt:      h.now().UTC(),
			ScopeNote:      "reinterpretation is limited to pre-existing raw messages selected by this run; original observations remain unchanged",
		}
		if err := h.store.SaveReinterpretRun(run); err != nil {
			return ReinterpretReport{}, err
		}
	}
	return h.reinterpretLocked(run, selector)
}

func selectorFromRun(run store.ReinterpretRun) ReinterpretSelector {
	return ReinterpretSelector{
		IdentityID:     run.IdentityID,
		BootID:         run.BootID,
		Format:         run.Format,
		ReceivedAfter:  run.ReceivedAfter,
		ReceivedBefore: run.ReceivedBefore,
		IncludePending: run.IncludePending,
	}
}

func (h *Hub) CompareReinterpretation(runID string) ([]InterpretationDifference, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.store.GetReinterpretRun(runID)
	if !ok {
		return nil, fmt.Errorf("reinterpretation run %s not found", runID)
	}
	interpretations := h.store.Interpretations(runID)
	sort.Slice(interpretations, func(i, j int) bool {
		left, right := interpretations[i].Raw, interpretations[j].Raw
		if left.IdentityID != right.IdentityID {
			return left.IdentityID < right.IdentityID
		}
		if left.BootID != right.BootID {
			return left.BootID < right.BootID
		}
		return left.Sequence < right.Sequence
	})
	differences := make([]InterpretationDifference, 0, len(interpretations))
	for _, interpretation := range interpretations {
		old, hasOld := h.store.BaselineObservation(interpretation.Raw.DeliveryID)
		if !hasOld && !interpretation.Backfill {
			continue
		}
		difference := differenceFor(old, interpretation)
		if !hasOld || len(difference.Kinds) > 0 {
			differences = append(differences, difference)
		}
	}
	return differences, nil
}

func (h *Hub) reinterpretLocked(run store.ReinterpretRun, selector ReinterpretSelector) (ReinterpretReport, error) {
	report := ReinterpretReport{RunID: run.ID, Status: "running", Scope: selector, ScopeNote: run.ScopeNote}
	selected := selectedRaws(h.store.RawMessages(), currentPending(h.store.Pending()), selector)
	report.Selected = len(selected)
	for _, stored := range selected {
		raw := stored.RawMessage
		item := ReinterpretItem{
			IdentityID: raw.IdentityID,
			BootID:     raw.BootID,
			Sequence:   raw.Sequence,
			DeliveryID: raw.DeliveryID,
			Format:     raw.Format,
			Backfill:   h.store.IsPending(stored.DeliveryID),
		}
		occurrenceID := raw.DeliveryID + "\x00" + store.PayloadSHA(raw.Payload)
		if h.store.HasInterpretation(run.ID, occurrenceID) {
			item.Skipped = true
			report.AlreadyDone = append(report.AlreadyDone, item)
			continue
		}
		identity, err := h.store.GetIdentity(raw.IdentityID)
		if err != nil {
			report.FailedAt = &item
			report.Err = err.Error()
			report.Remaining = len(selected) - len(report.Published) - len(report.AlreadyDone)
			return report, nil
		}
		format := selector.Format
		if format == "" {
			format = raw.Format
		}
		if format == "" {
			format = identity.Type
		}
		codec, err := h.adapters.Get(format)
		if err != nil {
			item.Format = format
			report.FailedAt = &item
			report.Err = err.Error()
			report.Remaining = len(selected) - len(report.Published) - len(report.AlreadyDone)
			return report, nil
		}
		interpretedRaw := raw
		interpretedRaw.Format = format
		observation, err := codec.Decode(interpretedRaw)
		if err != nil {
			report.FailedAt = &item
			report.Err = err.Error()
			report.Remaining = len(selected) - len(report.Published) - len(report.AlreadyDone)
			return report, nil
		}
		observation = h.normalizeObservation(interpretedRaw, observation)
		observation.InterpretedAt = h.now().UTC()
		if observation.RuleID == "" {
			observation.RuleID = format
		}
		interpretation := store.Interpretation{
			RunID: run.ID,
			Raw: model.RawReference{
				DeliveryID:  raw.DeliveryID,
				IdentityID:  raw.IdentityID,
				BootID:      raw.BootID,
				Sequence:    raw.Sequence,
				Format:      format,
				ContentType: raw.ContentType,
				PayloadSHA:  store.PayloadSHA(raw.Payload),
			},
			Observation:   observation,
			InterpretedAt: observation.InterpretedAt,
			Backfill:      h.store.IsPending(stored.DeliveryID),
		}
		if err := h.store.StoreInterpretation(interpretation); err != nil {
			return report, err
		}
		report.Published = append(report.Published, item)
	}
	completedAt := h.now().UTC()
	run.Status = "completed"
	run.CompletedAt = &completedAt
	if err := h.store.UpdateReinterpretRun(run); err != nil {
		return report, err
	}
	report.Status = "completed"
	return report, nil
}

func (h *Hub) reinterpretReport(runID string, run store.ReinterpretRun, selector ReinterpretSelector) ReinterpretReport {
	report := ReinterpretReport{RunID: runID, Status: run.Status, Scope: selector, ScopeNote: run.ScopeNote}
	for _, interpretation := range h.store.Interpretations(runID) {
		report.Published = append(report.Published, ReinterpretItem{
			IdentityID: interpretation.Raw.IdentityID,
			BootID:     interpretation.Raw.BootID,
			Sequence:   interpretation.Raw.Sequence,
			DeliveryID: interpretation.Raw.DeliveryID,
			Format:     interpretation.Raw.Format,
			Backfill:   interpretation.Backfill,
		})
	}
	sortItems(report.Published)
	report.Selected = len(report.Published)
	return report
}

func currentPending(messages []model.RawMessage) map[string]struct{} {
	pending := make(map[string]struct{}, len(messages))
	for _, message := range messages {
		pending[message.DeliveryID] = struct{}{}
	}
	return pending
}

func selectedRaws(messages []store.StoredRawMessage, pending map[string]struct{}, selector ReinterpretSelector) []store.StoredRawMessage {
	seen := make(map[string]struct{})
	selected := make([]store.StoredRawMessage, 0)
	for _, stored := range messages {
		raw := stored.RawMessage
		if stored.Status == "conflict" {
			continue
		}
		if selector.IdentityID != "" && raw.IdentityID != selector.IdentityID {
			continue
		}
		if selector.BootID != "" && raw.BootID != selector.BootID {
			continue
		}
		_, isPending := pending[raw.DeliveryID]
		if !selector.IncludePending && isPending {
			continue
		}
		if selector.ReceivedAfter != nil && raw.ReceivedAt.Before(*selector.ReceivedAfter) {
			continue
		}
		if selector.ReceivedBefore != nil && !raw.ReceivedAt.Before(*selector.ReceivedBefore) {
			continue
		}
		if selector.Format != "" && raw.Format != selector.Format {
			continue
		}
		id := raw.DeliveryID + "\x00" + store.PayloadSHA(raw.Payload)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		selected = append(selected, stored)
	}
	sort.Slice(selected, func(i, j int) bool {
		left, right := selected[i].RawMessage, selected[j].RawMessage
		if left.IdentityID != right.IdentityID {
			return left.IdentityID < right.IdentityID
		}
		if left.BootID != right.BootID {
			return left.BootID < right.BootID
		}
		if left.Sequence != right.Sequence {
			return left.Sequence < right.Sequence
		}
		return store.PayloadSHA(left.Payload) < store.PayloadSHA(right.Payload)
	})
	return selected
}

func differenceFor(old model.Observation, interpretation store.Interpretation) InterpretationDifference {
	newObservation := interpretation.Observation
	difference := InterpretationDifference{
		IdentityID:      interpretation.Raw.IdentityID,
		BootID:          interpretation.Raw.BootID,
		Sequence:        interpretation.Raw.Sequence,
		DeliveryID:      interpretation.Raw.DeliveryID,
		PayloadSHA:      interpretation.Raw.PayloadSHA,
		Backfill:        interpretation.Backfill,
		New:             newObservation,
		ChangedMeasures: make(map[string]MeasureChange),
	}
	if old.ID == "" {
		difference.Kinds = []string{"backfilled"}
		return difference
	}
	oldCopy := old
	difference.Old = &oldCopy
	names := make(map[string]struct{})
	for name := range old.Measures {
		names[name] = struct{}{}
	}
	for name := range newObservation.Measures {
		names[name] = struct{}{}
	}
	for name := range names {
		oldValue, oldOK := old.Measures[name]
		newValue, newOK := newObservation.Measures[name]
		oldUnit := old.Units[name]
		newUnit := newObservation.Units[name]
		if oldOK != newOK || oldValue != newValue || oldUnit != newUnit {
			difference.ChangedMeasures[name] = MeasureChange{Old: oldValue, New: newValue, OldUnit: oldUnit, NewUnit: newUnit}
		}
		if oldUnit != newUnit {
			difference.ChangedUnits = append(difference.ChangedUnits, name)
		}
	}
	if !reflect.DeepEqual(old.Measures, newObservation.Measures) {
		difference.Kinds = append(difference.Kinds, "value")
	}
	if len(difference.ChangedUnits) > 0 {
		difference.Kinds = append(difference.Kinds, "unit")
		sort.Strings(difference.ChangedUnits)
	}
	changedExplanation := old.Explanation != newObservation.Explanation || old.RuleID != newObservation.RuleID || old.RuleVersion != newObservation.RuleVersion || old.Type != newObservation.Type
	if !reflect.DeepEqual(old.Attributes, newObservation.Attributes) {
		changedExplanation = true
	}
	if changedExplanation {
		difference.Kinds = append(difference.Kinds, "explanation")
		difference.ExplanationChanged = true
	}
	sort.Strings(difference.Kinds)
	return difference
}

func sortItems(items []ReinterpretItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].IdentityID != items[j].IdentityID {
			return items[i].IdentityID < items[j].IdentityID
		}
		if items[i].BootID != items[j].BootID {
			return items[i].BootID < items[j].BootID
		}
		return items[i].Sequence < items[j].Sequence
	})
}

func pendingAck(raw model.RawMessage) *Ack {
	return &Ack{IdentityID: raw.IdentityID, BootID: raw.BootID, Sequence: raw.Sequence, Pending: true, Stored: true}
}

func DefaultAdapters() *adapter.Registry {
	return adapter.NewRegistry(
		device.TemperatureAdapter{},
		device.TemperatureV2Adapter{},
		device.VibrationAdapter{},
	)
}
