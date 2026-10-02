package hub

import (
	"errors"
	"fmt"
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

// ErrCorrectionConflict is returned when adopting a branch whose scope
// overlaps deliveries that another operator already re-interpreted after
// this branch was opened. Nothing is written; the caller can inspect the
// report's Conflicts and retry with AdoptOptions.Narrow to adopt only the
// non-conflicting part.
var ErrCorrectionConflict = errors.New("correction branch conflicts with a newer adoption")

type AdoptOptions struct {
	Operator string
	// Narrow adopts only the non-conflicting deliveries when a concurrent
	// adoption overlaps; the branch stays partially_adopted for the rest.
	Narrow bool
}

// AdoptionConflict describes one delivery where a newer adoption and this
// branch disagree (e.g. incompatible unit interpretations).
type AdoptionConflict struct {
	DeliveryID         string           `json:"delivery_id"`
	ExistingRule       string           `json:"existing_rule"`
	ExistingAdoptionID string           `json:"existing_adoption_id"`
	IncomingRule       string           `json:"incoming_rule"`
	Comparison         model.Comparison `json:"comparison"`
}

type AdoptionReport struct {
	AdoptionID            string             `json:"adoption_id,omitempty"`
	Adopted               []string           `json:"adopted,omitempty"`
	Skipped               []string           `json:"skipped,omitempty"`
	Pending               []string           `json:"pending,omitempty"`
	Conflicts             []AdoptionConflict `json:"conflicts,omitempty"`
	DownstreamValid       []string           `json:"downstream_valid,omitempty"`
	DownstreamNeedsReview []string           `json:"downstream_needs_review,omitempty"`
	BranchStatus          string             `json:"branch_status"`
}

// RegisterRule makes a correction rule's adapter available to this hub.
// Rules must be re-registered after a restart before their branches can be
// previewed or adopted; all branch and adoption state is recovered from
// the journal automatically.
func (h *Hub) RegisterRule(rule adapter.Adapter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rules[rule.Type()] = rule
}

// OpenCorrection creates a correction branch: an alternative
// interpretation rule over a device/time scope. The original observations
// are stored once and never modified; the branch only takes effect through
// a formal adoption.
func (h *Hub) OpenCorrection(rule adapter.Adapter, filter ReinterpretFilter, operator, reason string) (model.CorrectionBranch, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rules[rule.Type()] = rule
	branch := model.CorrectionBranch{
		ID:       h.store.NextBranchID(),
		Rule:     rule.Type(),
		Operator: operator,
		Reason:   reason,
		Status:   model.BranchOpen,
		Filter: model.BranchFilter{
			IdentityID: filter.IdentityID,
			From:       filter.From,
			To:         filter.To,
		},
		BaseRevision: h.store.Revision(),
		CreatedAt:    h.now().UTC(),
	}
	if err := h.store.SaveBranch(branch); err != nil {
		return model.CorrectionBranch{}, err
	}
	return branch, nil
}

func (h *Hub) Branches() []model.CorrectionBranch {
	return h.store.Branches()
}

// PreviewCorrection compares a branch's interpretation against the
// currently effective values over an optional additional device/time
// filter, without persisting anything.
func (h *Hub) PreviewCorrection(branchID string, filter ReinterpretFilter) (ReinterpretReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	branch, err := h.store.GetBranch(branchID)
	if err != nil {
		return ReinterpretReport{}, err
	}
	rule, err := h.branchRule(branch)
	if err != nil {
		return ReinterpretReport{}, err
	}
	report := ReinterpretReport{Rule: branch.Rule}
	for _, stored := range h.branchRaws(branch, filter) {
		report.Matched++
		current, ok := h.currentObservation(stored.DeliveryID)
		if !ok {
			report.Pending++
			continue
		}
		observation, err := rule.Decode(stored.RawMessage)
		if err != nil {
			return report, fmt.Errorf("preview %s: %w", stored.DeliveryID, err)
		}
		comparison := model.Compare(current, observation)
		report.Processed = append(report.Processed, ReinterpretEntry{
			DeliveryID: stored.DeliveryID,
			IdentityID: stored.IdentityID,
			BootID:     stored.BootID,
			Sequence:   stored.Sequence,
			Rule:       branch.Rule,
			Change:     comparison.Change,
			Comparison: comparison,
		})
	}
	return report, nil
}

// AdoptBranch formally adopts a correction branch. Concurrent overlap is
// never resolved by last-save-wins: deliveries re-interpreted by a newer
// adoption after this branch opened are reported as conflicts and nothing
// is written unless AdoptOptions.Narrow is set, in which case only the
// non-conflicting deliveries are adopted and the branch stays
// partially_adopted.
func (h *Hub) AdoptBranch(branchID string, opts AdoptOptions) (AdoptionReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	branch, err := h.store.GetBranch(branchID)
	if err != nil {
		return AdoptionReport{}, err
	}
	if branch.Status != model.BranchOpen && branch.Status != model.BranchPartiallyAdopted {
		return AdoptionReport{}, fmt.Errorf("branch %s is %s", branchID, branch.Status)
	}
	rule, err := h.branchRule(branch)
	if err != nil {
		return AdoptionReport{}, err
	}
	report := AdoptionReport{}
	var entries []model.AdoptionEntry
	for _, stored := range h.branchRaws(branch, ReinterpretFilter{}) {
		deliveryID := stored.DeliveryID
		currentRule, currentAdoptionID := h.store.CurrentInterp(deliveryID)
		if currentRule == branch.Rule && currentAdoptionID != "" {
			// Already adopted under this branch (e.g. resumed run).
			report.Skipped = append(report.Skipped, deliveryID)
			continue
		}
		current, ok := h.currentObservation(deliveryID)
		if !ok {
			report.Pending = append(report.Pending, deliveryID)
			continue
		}
		observation, err := rule.Decode(stored.RawMessage)
		if err != nil {
			return report, fmt.Errorf("adopt %s: %w", deliveryID, err)
		}
		comparison := model.Compare(current, observation)
		if currentAdoptionID != "" && h.store.InterpRevision(deliveryID) > branch.BaseRevision && comparison.Change != model.ChangeUnchanged {
			report.Conflicts = append(report.Conflicts, AdoptionConflict{
				DeliveryID:         deliveryID,
				ExistingRule:       currentRule,
				ExistingAdoptionID: currentAdoptionID,
				IncomingRule:       branch.Rule,
				Comparison:         comparison,
			})
			continue
		}
		entries = append(entries, model.AdoptionEntry{
			RawRef:             rawReference(stored.RawMessage),
			Observation:        observation,
			Comparison:         comparison,
			PreviousRule:       currentRule,
			PreviousAdoptionID: currentAdoptionID,
		})
		report.Adopted = append(report.Adopted, deliveryID)
	}
	if len(report.Conflicts) > 0 && !opts.Narrow {
		report.BranchStatus = branch.Status
		return report, ErrCorrectionConflict
	}
	now := h.now().UTC()
	if len(entries) > 0 {
		adoption := model.Adoption{
			ID:        fmt.Sprintf("adoption-%d", h.store.Revision()+1),
			BranchID:  branch.ID,
			Rule:      branch.Rule,
			Operator:  opts.Operator,
			Revision:  h.store.Revision() + 1,
			Entries:   entries,
			CreatedAt: now,
		}
		if len(report.Conflicts) > 0 || len(report.Pending) > 0 {
			branch.Status = model.BranchPartiallyAdopted
		} else {
			branch.Status = model.BranchAdopted
			branch.ClosedAt = now
		}
		if err := h.store.ApplyAdoption(branch, adoption); err != nil {
			return report, err
		}
		report.AdoptionID = adoption.ID
	} else if len(report.Conflicts) == 0 && len(report.Pending) == 0 {
		branch.Status = model.BranchAdopted
		branch.ClosedAt = now
		if err := h.store.SaveBranch(branch); err != nil {
			return report, err
		}
	}
	report.BranchStatus = branch.Status
	report.DownstreamValid, report.DownstreamNeedsReview = h.downstreamImpact(report.Adopted)
	return report, nil
}

// AbandonBranch closes a branch without adopting it. The branch record,
// including its operator, rule and rationale, is preserved.
func (h *Hub) AbandonBranch(branchID, operator, reason string) (model.CorrectionBranch, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	branch, err := h.store.GetBranch(branchID)
	if err != nil {
		return model.CorrectionBranch{}, err
	}
	if branch.Status == model.BranchAdopted || branch.Status == model.BranchAbandoned {
		return branch, fmt.Errorf("branch %s is already %s", branchID, branch.Status)
	}
	branch.Status = model.BranchAbandoned
	branch.CloseReason = reason
	branch.ClosedAt = h.now().UTC()
	if err := h.store.SaveBranch(branch); err != nil {
		return model.CorrectionBranch{}, err
	}
	return branch, nil
}

// RetractAdoption formally withdraws the deliveries of an adoption that
// match the filter (partial retraction). Affected deliveries revert to the
// interpretation that preceded the adoption.
func (h *Hub) RetractAdoption(adoptionID string, filter ReinterpretFilter, operator, reason string) (AdoptionReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	adoption, err := h.store.GetAdoption(adoptionID)
	if err != nil {
		return AdoptionReport{}, err
	}
	var deliveryIDs []string
	for _, entry := range adoption.Entries {
		deliveryID := entry.RawRef.DeliveryID
		if filter.IdentityID != "" && entry.RawRef.IdentityID != filter.IdentityID {
			continue
		}
		if !filter.From.IsZero() || !filter.To.IsZero() {
			matched := false
			for _, raw := range h.store.RawMessages(entry.RawRef.IdentityID, filter.From, filter.To) {
				if raw.DeliveryID == deliveryID {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		rule, currentAdoptionID := h.store.CurrentInterp(deliveryID)
		if currentAdoptionID != adoptionID {
			return AdoptionReport{}, fmt.Errorf("delivery %s is now governed by adoption %s (%s); retract that first", deliveryID, currentAdoptionID, rule)
		}
		deliveryIDs = append(deliveryIDs, deliveryID)
	}
	if len(deliveryIDs) == 0 {
		return AdoptionReport{}, fmt.Errorf("adoption %s has no deliveries matching the filter", adoptionID)
	}
	retraction := model.Retraction{
		ID:          fmt.Sprintf("retraction-%d", h.store.Revision()+1),
		AdoptionID:  adoptionID,
		Operator:    operator,
		Reason:      reason,
		DeliveryIDs: deliveryIDs,
		Revision:    h.store.Revision() + 1,
		CreatedAt:   h.now().UTC(),
	}
	if err := h.store.ApplyRetraction(retraction); err != nil {
		return AdoptionReport{}, err
	}
	report := AdoptionReport{AdoptionID: retraction.ID, Adopted: deliveryIDs}
	report.DownstreamValid, report.DownstreamNeedsReview = h.downstreamImpact(deliveryIDs)
	return report, nil
}

// RegisterDownstream records that an external consumer referenced the
// effective interpretation of a delivery, so later adoptions can report
// which references stay valid and which need re-review.
func (h *Hub) RegisterDownstream(id, deliveryID, rule, note string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.store.AddDownstreamRef(model.DownstreamRef{
		ID:         id,
		DeliveryID: deliveryID,
		Rule:       rule,
		Note:       note,
		CreatedAt:  h.now().UTC(),
	})
}

// CurrentValue resolves the effective interpretation of one delivery with
// its full provenance: the stored raw message reference and the adoption
// decision (empty AdoptionID = original ingest interpretation).
func (h *Hub) CurrentValue(deliveryID string) (model.CurrentValue, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, raw := range h.store.RawMessages("", time.Time{}, time.Time{}) {
		if raw.DeliveryID != deliveryID {
			continue
		}
		observation, ok := h.currentObservation(deliveryID)
		if !ok {
			return model.CurrentValue{}, false
		}
		rule, adoptionID := h.store.CurrentInterp(deliveryID)
		return model.CurrentValue{
			DeliveryID:  deliveryID,
			Observation: observation,
			Rule:        rule,
			AdoptionID:  adoptionID,
			RawRef:      rawReference(raw.RawMessage),
		}, true
	}
	return model.CurrentValue{}, false
}

// CurrentValues lists the effective interpretation of every interpreted
// delivery in the given device/time scope.
func (h *Hub) CurrentValues(filter ReinterpretFilter) []model.CurrentValue {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []model.CurrentValue
	for _, raw := range h.store.RawMessages(filter.IdentityID, filter.From, filter.To) {
		observation, ok := h.currentObservation(raw.DeliveryID)
		if !ok {
			continue
		}
		rule, adoptionID := h.store.CurrentInterp(raw.DeliveryID)
		out = append(out, model.CurrentValue{
			DeliveryID:  raw.DeliveryID,
			Observation: observation,
			Rule:        rule,
			AdoptionID:  adoptionID,
			RawRef:      rawReference(raw.RawMessage),
		})
	}
	return out
}

func (h *Hub) branchRule(branch model.CorrectionBranch) (adapter.Adapter, error) {
	rule, ok := h.rules[branch.Rule]
	if !ok {
		return nil, fmt.Errorf("rule %s is not registered with this hub", branch.Rule)
	}
	return rule, nil
}

// branchRaws selects the stored raw messages covered by a branch, further
// restricted by an optional extra device/time filter.
func (h *Hub) branchRaws(branch model.CorrectionBranch, extra ReinterpretFilter) []store.StoredRawMessage {
	identityID := branch.Filter.IdentityID
	if extra.IdentityID != "" {
		identityID = extra.IdentityID
	}
	from, to := branch.Filter.From, branch.Filter.To
	if !extra.From.IsZero() && (from.IsZero() || extra.From.After(from)) {
		from = extra.From
	}
	if !extra.To.IsZero() && (to.IsZero() || extra.To.Before(to)) {
		to = extra.To
	}
	return h.store.RawMessages(identityID, from, to)
}

func (h *Hub) currentObservation(deliveryID string) (model.Observation, bool) {
	_, adoptionID := h.store.CurrentInterp(deliveryID)
	if adoptionID == "" {
		return h.store.ObservationFor(deliveryID)
	}
	return h.store.AdoptedObservation(deliveryID, adoptionID)
}

// downstreamImpact splits registered downstream references into those
// still valid after the given deliveries changed interpretation and those
// that referenced a superseded interpretation and need re-review.
func (h *Hub) downstreamImpact(changed []string) (valid, needsReview []string) {
	affected := make(map[string]bool, len(changed))
	for _, id := range changed {
		affected[id] = true
	}
	for _, ref := range h.store.DownstreamRefs() {
		rule, _ := h.store.CurrentInterp(ref.DeliveryID)
		if !affected[ref.DeliveryID] || ref.Rule == rule {
			valid = append(valid, ref.ID)
		} else {
			needsReview = append(needsReview, ref.ID)
		}
	}
	return valid, needsReview
}

func rawReference(raw model.RawMessage) model.RawReference {
	return model.RawReference{
		DeliveryID:  raw.DeliveryID,
		IdentityID:  raw.IdentityID,
		BootID:      raw.BootID,
		Sequence:    raw.Sequence,
		Format:      raw.Format,
		ContentType: raw.ContentType,
		PayloadSHA:  store.PayloadSHA(raw.Payload),
	}
}
