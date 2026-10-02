package hub

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	Processed []Ack            `json:"processed"`
	Remaining int              `json:"remaining"`
	FailedAt  *Ack             `json:"failed_at,omitempty"`
	Err       string           `json:"error,omitempty"`
	Scope     []ReprocessScope `json:"scope,omitempty"`
}

// ReprocessScope describes the device and sequence/time range covered by a
// backfill interpretation run over previously unknown formats.
type ReprocessScope struct {
	IdentityID    string    `json:"identity_id"`
	BootID        string    `json:"boot_id"`
	FromSequence  int64     `json:"from_sequence"`
	ToSequence    int64     `json:"to_sequence"`
	FirstReceived time.Time `json:"first_received"`
	LastReceived  time.Time `json:"last_received"`
}

// ReinterpretFilter selects stored raw messages for reinterpretation by
// device identity and receive-time range. Zero values mean no restriction.
type ReinterpretFilter struct {
	IdentityID string
	From       time.Time
	To         time.Time
}

type ReinterpretEntry struct {
	DeliveryID string           `json:"delivery_id"`
	IdentityID string           `json:"identity_id"`
	BootID     string           `json:"boot_id"`
	Sequence   int64            `json:"sequence"`
	Rule       string           `json:"rule"`
	Change     string           `json:"change"`
	Comparison model.Comparison `json:"comparison"`
}

type ReinterpretReport struct {
	Rule      string             `json:"rule"`
	Matched   int                `json:"matched"`
	Skipped   int                `json:"skipped"` // already reinterpreted with this rule
	Pending   int                `json:"pending"` // matched but not yet interpretable
	Processed []ReinterpretEntry `json:"processed"`
}

type CorrectionRequest struct {
	ID       string
	Rule     adapter.Adapter
	Operator string
	Reason   string
	Scope    model.BranchScope
}

type BranchBuildReport struct {
	BranchID  string              `json:"branch_id"`
	Rule      string              `json:"rule"`
	Matched   int                 `json:"matched"`
	Processed []model.BranchEntry `json:"processed"`
	Pending   int                 `json:"pending"`
	Skipped   int                 `json:"skipped"`
}

type AdoptionRequest struct {
	BranchID string
	Operator string
	Reason   string
	Scope    model.BranchScope
}

type AdoptionConflict struct {
	DeliveryID         string `json:"delivery_id"`
	ExistingBranchID   string `json:"existing_branch_id"`
	ExistingAdoptionID string `json:"existing_adoption_id"`
	Reason             string `json:"reason"`
}

type AdoptionResult struct {
	Adoption  *model.Adoption     `json:"adoption,omitempty"`
	Adopted   []model.BranchEntry `json:"adopted,omitempty"`
	Conflicts []AdoptionConflict  `json:"conflicts,omitempty"`
}

type ReferenceRequest struct {
	ID          string
	Name        string
	Operator    string
	DeliveryIDs []string
	Baseline    map[string]string
}

type BranchComparisonEntry struct {
	DeliveryID string             `json:"delivery_id"`
	RawRef     model.RawReference `json:"raw_ref"`
	Change     string             `json:"change"`
	Comparison model.Comparison   `json:"comparison"`
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
		result.Scope = extendScope(result.Scope, raw)
	}
	result.Remaining = h.store.PendingCount()
	return result, nil
}

func extendScope(scope []ReprocessScope, raw model.RawMessage) []ReprocessScope {
	for i := range scope {
		s := &scope[i]
		if s.IdentityID != raw.IdentityID || s.BootID != raw.BootID {
			continue
		}
		if raw.Sequence < s.FromSequence {
			s.FromSequence = raw.Sequence
		}
		if raw.Sequence > s.ToSequence {
			s.ToSequence = raw.Sequence
		}
		if raw.ReceivedAt.Before(s.FirstReceived) {
			s.FirstReceived = raw.ReceivedAt
		}
		if raw.ReceivedAt.After(s.LastReceived) {
			s.LastReceived = raw.ReceivedAt
		}
		return scope
	}
	return append(scope, ReprocessScope{
		IdentityID:    raw.IdentityID,
		BootID:        raw.BootID,
		FromSequence:  raw.Sequence,
		ToSequence:    raw.Sequence,
		FirstReceived: raw.ReceivedAt,
		LastReceived:  raw.ReceivedAt,
	})
}

// Reinterpret re-decodes stored raw messages with a corrected rule and
// records the new observations alongside a comparison against the original
// interpretation. Original observations are never modified or deleted.
// Already-reinterpreted deliveries are skipped, so an interrupted run can
// be resumed by calling Reinterpret again with the same rule.
func (h *Hub) Reinterpret(rule adapter.Adapter, filter ReinterpretFilter) (ReinterpretReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	report := ReinterpretReport{Rule: rule.Type()}
	raws := h.store.RawMessages(filter.IdentityID, filter.From, filter.To)
	for _, stored := range raws {
		report.Matched++
		if h.store.Reinterpreted(stored.DeliveryID, rule.Type()) {
			report.Skipped++
			continue
		}
		old, ok := h.store.ObservationFor(stored.DeliveryID)
		if !ok {
			// Still pending (unknown format): nothing to compare against yet.
			report.Pending++
			continue
		}
		observation, err := rule.Decode(stored.RawMessage)
		if err != nil {
			return report, fmt.Errorf("reinterpret %s: %w", stored.DeliveryID, err)
		}
		comparison := model.Compare(old, observation)
		if err := h.store.Reinterpret(stored.RawMessage, rule.Type(), observation, comparison); err != nil {
			return report, err
		}
		report.Processed = append(report.Processed, ReinterpretEntry{
			DeliveryID: stored.DeliveryID,
			IdentityID: stored.IdentityID,
			BootID:     stored.BootID,
			Sequence:   stored.Sequence,
			Rule:       rule.Type(),
			Change:     comparison.Change,
			Comparison: comparison,
		})
	}
	return report, nil
}

// CreateCorrectionBranch materializes a correction as an independently
// comparable branch without changing any current value. Re-running with the
// same branch ID resumes an interrupted run and skips durable entries.
func (h *Hub) CreateCorrectionBranch(req CorrectionRequest) (BranchBuildReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if req.Rule == nil {
		return BranchBuildReport{}, errors.New("correction rule is required")
	}
	now := h.now().UTC()
	branchID := req.ID
	if branchID == "" {
		branchID = req.Rule.Type() + "-" + strconv.FormatInt(now.UnixNano(), 36)
	}
	branch, exists := h.store.Branch(branchID)
	if !exists {
		branch = model.CorrectionBranch{
			ID: branchID, Rule: req.Rule.Type(), Operator: req.Operator,
			Reason: req.Reason, Scope: req.Scope, Status: model.BranchActive,
			CreatedAt: now, UpdatedAt: now,
		}
		if err := h.store.SaveBranch(branch); err != nil {
			return BranchBuildReport{}, err
		}
	} else if branch.Rule != req.Rule.Type() {
		return BranchBuildReport{}, fmt.Errorf("branch %s already uses rule %s", branchID, branch.Rule)
	}
	report := BranchBuildReport{BranchID: branchID, Rule: req.Rule.Type()}
	for _, stored := range h.store.RawMessages(req.Scope.IdentityID, req.Scope.From, req.Scope.To) {
		if !req.Scope.Matches(stored.IdentityID, stored.BootID, stored.Sequence, stored.ReceivedAt) {
			continue
		}
		report.Matched++
		if h.store.BranchEntryExists(branchID, stored.DeliveryID) {
			report.Skipped++
			continue
		}
		newObservation, err := req.Rule.Decode(stored.RawMessage)
		if err != nil {
			return report, fmt.Errorf("build branch %s for %s: %w", branchID, stored.DeliveryID, err)
		}
		oldObservation, hasOld := h.store.ObservationFor(stored.DeliveryID)
		comparison := model.Comparison{Change: model.ChangeValue}
		if hasOld {
			comparison = model.Compare(oldObservation, newObservation)
		}
		entry := model.BranchEntry{
			ID:       branchID + ":" + stored.DeliveryID,
			BranchID: branchID, Rule: req.Rule.Type(),
			RawRef: stored.RawReference(), Observation: newObservation,
			Comparison: comparison, Status: model.EntryIncluded,
			CreatedAt: now,
		}
		if !hasOld {
			report.Pending++
		}
		if err := h.store.SaveBranchEntry(entry); err != nil {
			return report, err
		}
		report.Processed = append(report.Processed, entry)
	}
	return report, nil
}

func (h *Hub) Branches() []model.CorrectionBranch { return h.store.Branches() }

func (h *Hub) BranchEntries(branchID string) []model.BranchEntry {
	return h.store.BranchEntries(branchID)
}

// CompareBranches compares candidate interpretations for the same stored
// deliveries. Scope can select different devices, boots, sequences or time
// windows; only deliveries present in both branches are compared.
func (h *Hub) CompareBranches(leftID, rightID string, scope model.BranchScope) ([]BranchComparisonEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.store.Branch(leftID); !ok {
		return nil, fmt.Errorf("branch %s not found", leftID)
	}
	if _, ok := h.store.Branch(rightID); !ok {
		return nil, fmt.Errorf("branch %s not found", rightID)
	}
	left := map[string]model.BranchEntry{}
	for _, entry := range h.store.BranchEntries(leftID) {
		if scope.Matches(entry.RawRef.IdentityID, entry.RawRef.BootID, entry.RawRef.Sequence, entry.Observation.ReceivedAt) {
			left[entry.RawRef.DeliveryID] = entry
		}
	}
	var out []BranchComparisonEntry
	for _, rightEntry := range h.store.BranchEntries(rightID) {
		ref := rightEntry.RawRef
		if !scope.Matches(ref.IdentityID, ref.BootID, ref.Sequence, rightEntry.Observation.ReceivedAt) {
			continue
		}
		leftEntry, ok := left[ref.DeliveryID]
		if !ok {
			continue
		}
		comparison := model.Compare(leftEntry.Observation, rightEntry.Observation)
		out = append(out, BranchComparisonEntry{
			DeliveryID: ref.DeliveryID, RawRef: ref,
			Change: comparison.Change, Comparison: comparison,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].DeliveryID < out[j].DeliveryID })
	return out, nil
}

// AdoptBranch makes non-overlapping branch entries current. Any overlap with
// a different active adoption is returned and excluded, so callers can narrow
// the scope and adopt the remaining compatible subset.
func (h *Hub) AdoptBranch(req AdoptionRequest) (AdoptionResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	branch, ok := h.store.Branch(req.BranchID)
	if !ok {
		return AdoptionResult{}, fmt.Errorf("branch %s not found", req.BranchID)
	}
	if branch.Status == model.BranchAbandoned {
		return AdoptionResult{}, fmt.Errorf("branch %s was abandoned", req.BranchID)
	}
	result := AdoptionResult{}
	var candidates []model.BranchEntry
	for _, entry := range h.store.BranchEntries(req.BranchID) {
		if entry.Status == model.EntryExcluded {
			continue
		}
		ref := entry.RawRef
		if !req.Scope.Matches(ref.IdentityID, ref.BootID, ref.Sequence, entry.Observation.ReceivedAt) {
			continue
		}
		if existing, exists := h.store.CurrentAdoption(ref.DeliveryID); exists {
			if h.store.Withdrawn(existing.ID, ref.DeliveryID) {
				candidates = append(candidates, entry)
				continue
			}
			if existing.BranchID == req.BranchID {
				continue
			}
			conflict := AdoptionConflict{
				DeliveryID: ref.DeliveryID, ExistingBranchID: existing.BranchID,
				ExistingAdoptionID: existing.ID,
				Reason:             "overlapping active adoption",
			}
			current, _ := h.store.CurrentValue(ref.DeliveryID)
			if unitConflict(current.Observation, entry.Observation) {
				conflict.Reason = "incompatible unit interpretation"
			}
			result.Conflicts = append(result.Conflicts, conflict)
			if err := h.store.RecordBranchConflict(model.BranchConflict{
				BranchID: req.BranchID, ExistingBranchID: existing.BranchID,
				DeliveryIDs: []string{ref.DeliveryID}, Reason: conflict.Reason,
				RecordedAt: h.now().UTC(),
			}); err != nil {
				return AdoptionResult{}, err
			}
			continue
		}
		candidates = append(candidates, entry)
	}
	if len(result.Conflicts) != 0 {
		return result, nil
	}
	if len(candidates) == 0 {
		return AdoptionResult{}, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].RawRef.DeliveryID < candidates[j].RawRef.DeliveryID })
	deliveryIDs := make([]string, 0, len(candidates))
	for _, entry := range candidates {
		deliveryIDs = append(deliveryIDs, entry.RawRef.DeliveryID)
	}
	adoption := model.Adoption{
		ID:       req.BranchID + ":adopt:" + strconv.FormatInt(h.now().UTC().UnixNano(), 36),
		BranchID: req.BranchID, Rule: branch.Rule, Operator: req.Operator,
		Reason: req.Reason, Scope: req.Scope, DeliveryIDs: deliveryIDs,
		AdoptedAt: h.now().UTC(),
	}
	adoption.References = h.referenceImpactsLocked(deliveryIDs)
	if err := h.store.Adopt(adoption); err != nil {
		return AdoptionResult{}, err
	}
	updated := branch
	updated.Status = model.BranchAdopted
	updated.UpdatedAt = adoption.AdoptedAt
	_ = h.store.UpdateBranch(updated)
	result.Adoption = &adoption
	result.Adopted = candidates
	return result, nil
}

func unitConflict(oldObs, candidate model.Observation) bool {
	oldUnits := map[string]string{}
	for name := range oldObs.Measures {
		base, unit := model.SplitMeasureName(name)
		if unit != "" {
			oldUnits[base] = unit
		}
	}
	for name := range candidate.Measures {
		base, unit := model.SplitMeasureName(name)
		if unit != "" && oldUnits[base] != "" && oldUnits[base] != unit {
			return true
		}
	}
	return false
}

func (h *Hub) AbandonBranch(branchID, operator, reason string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	branch, ok := h.store.Branch(branchID)
	if !ok {
		return fmt.Errorf("branch %s not found", branchID)
	}
	branch.Status = model.BranchAbandoned
	branch.AbandonReason = reason
	branch.Operator = operator
	branch.UpdatedAt = h.now().UTC()
	return h.store.UpdateBranch(branch)
}

func (h *Hub) WithdrawAdoption(adoptionID, operator, reason string, scope model.BranchScope) (model.AdoptionWithdrawal, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	adoption, ok := h.store.Adoption(adoptionID)
	if !ok {
		return model.AdoptionWithdrawal{}, fmt.Errorf("adoption %s not found", adoptionID)
	}
	var ids []string
	for _, id := range adoption.DeliveryIDs {
		if current, exists := h.store.CurrentValue(id); exists && current.AdoptionID == adoptionID && scope.Matches(current.RawRef.IdentityID, current.RawRef.BootID, current.RawRef.Sequence, current.Observation.ReceivedAt) {
			ids = append(ids, id)
		}
	}
	withdrawal := model.AdoptionWithdrawal{
		ID:         adoptionID + ":withdraw:" + strconv.FormatInt(h.now().UTC().UnixNano(), 36),
		AdoptionID: adoptionID, BranchID: adoption.BranchID, Operator: operator,
		Reason: reason, DeliveryIDs: ids, WithdrawnAt: h.now().UTC(),
	}
	if err := h.store.WithdrawAdoption(withdrawal); err != nil {
		return model.AdoptionWithdrawal{}, err
	}
	return withdrawal, nil
}

func (h *Hub) RegisterDownstreamReference(req ReferenceRequest) (model.DownstreamReference, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	deliveryIDs := append([]string(nil), req.DeliveryIDs...)
	sort.Strings(deliveryIDs)
	baseline := map[string]string{}
	for key, value := range req.Baseline {
		baseline[key] = value
	}
	for _, deliveryID := range deliveryIDs {
		if _, known := baseline[deliveryID]; !known {
			if current, ok := h.store.CurrentValue(deliveryID); ok {
				baseline[deliveryID] = model.Fingerprint(current.Observation)
			}
		}
	}
	id := req.ID
	if id == "" {
		id = "ref-" + strconv.FormatInt(h.now().UTC().UnixNano(), 36)
	}
	reference := model.DownstreamReference{
		ID: id, Name: req.Name, Operator: req.Operator,
		DeliveryIDs: deliveryIDs, Baseline: baseline, CreatedAt: h.now().UTC(),
	}
	return reference, h.store.RegisterReference(reference)
}

func (h *Hub) AuditReferences() []model.ReferenceImpact {
	h.mu.Lock()
	defer h.mu.Unlock()
	all := map[string]struct{}{}
	for _, reference := range h.store.References() {
		for _, id := range reference.DeliveryIDs {
			all[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return h.referenceImpactsLocked(ids)
}

func (h *Hub) referenceImpactsLocked(deliveryIDs []string) []model.ReferenceImpact {
	var impacts []model.ReferenceImpact
	for _, reference := range h.store.References() {
		impact := model.ReferenceImpact{ReferenceID: reference.ID, Name: reference.Name, Status: model.ReferenceValid}
		for _, deliveryID := range reference.DeliveryIDs {
			current, ok := h.store.CurrentValue(deliveryID)
			if !ok {
				impact.Status = model.ReferenceReview
				impact.Reason = "referenced delivery is not currently available"
				impact.DeliveryIDs = append(impact.DeliveryIDs, deliveryID)
				continue
			}
			want, hasBaseline := reference.Baseline[deliveryID]
			if !hasBaseline {
				impact.Status = model.ReferenceIncomplete
				impact.Reason = "baseline fingerprint is missing"
				impact.DeliveryIDs = append(impact.DeliveryIDs, deliveryID)
				continue
			}
			if model.Fingerprint(current.Observation) != want {
				impact.Status = model.ReferenceReview
				impact.Reason = "adopted interpretation differs from referenced value"
				impact.DeliveryIDs = append(impact.DeliveryIDs, deliveryID)
			}
		}
		if len(impact.DeliveryIDs) == 0 {
			impact.DeliveryIDs = append([]string(nil), reference.DeliveryIDs...)
		}
		impacts = append(impacts, impact)
	}
	return impacts
}

func (h *Hub) CurrentValues(scope model.BranchScope) []model.CurrentValue {
	return h.store.CurrentValues(scope)
}

func (h *Hub) CurrentValue(deliveryID string) (model.CurrentValue, bool) {
	return h.store.CurrentValue(deliveryID)
}

func (h *Hub) BranchConflicts() []model.BranchConflict {
	return h.store.BranchConflicts()
}

// Conflicts returns the preserved evidence of same-sequence content conflicts.
func (h *Hub) Conflicts() []store.ConflictEvidence {
	return h.store.Conflicts()
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
