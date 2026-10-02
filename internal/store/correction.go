package store

import (
	"fmt"

	"edge-ingest/internal/model"
)

// interpState identifies the interpretation currently in effect for a
// delivery: the original ingest (Rule "", AdoptionID "") or an adoption.
type interpState struct {
	Rule       string
	AdoptionID string
}

// Revision returns the current adoption revision. Correction branches
// capture it as their base so concurrent overlapping adoptions can be
// detected instead of silently overwriting each other.
func (s *Store) Revision() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}

// SaveBranch persists a correction branch record (create or status update).
func (s *Store) SaveBranch(branch model.CorrectionBranch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "correction_branch", Branch: &branch}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) GetBranch(id string) (model.CorrectionBranch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	branch, ok := s.branches[id]
	if !ok {
		return model.CorrectionBranch{}, ErrNotFound
	}
	return branch, nil
}

func (s *Store) Branches() []model.CorrectionBranch {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.CorrectionBranch, 0, len(s.branchOrder))
	for _, id := range s.branchOrder {
		out = append(out, s.branches[id])
	}
	return out
}

// NextBranchID returns a stable human-readable id for a new branch.
func (s *Store) NextBranchID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("branch-%d", len(s.branchOrder)+1)
}

// ApplyAdoption atomically records the formal adoption and the resulting
// branch status in a single journal record.
func (s *Store) ApplyAdoption(branch model.CorrectionBranch, adoption model.Adoption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "adoption", Branch: &branch, Adoption: &adoption}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

// ApplyRetraction atomically records a (possibly partial) retraction.
func (s *Store) ApplyRetraction(retraction model.Retraction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "retraction", Retraction: &retraction}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

// CurrentInterp reports which interpretation is effective for a delivery.
// An empty AdoptionID means the original ingest interpretation.
func (s *Store) CurrentInterp(deliveryID string) (rule, adoptionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stack := s.interpStack[deliveryID]
	if len(stack) == 0 {
		return "", ""
	}
	top := stack[len(stack)-1]
	return top.Rule, top.AdoptionID
}

// InterpRevision returns the revision at which the current interpretation
// of a delivery was adopted (0 = original ingest).
func (s *Store) InterpRevision(deliveryID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, adoptionID := currentInterp(s.interpStack, deliveryID)
	if adoptionID == "" {
		return 0
	}
	return s.adoptions[adoptionID].Revision
}

func currentInterp(stacks map[string][]interpState, deliveryID string) (string, string) {
	stack := stacks[deliveryID]
	if len(stack) == 0 {
		return "", ""
	}
	top := stack[len(stack)-1]
	return top.Rule, top.AdoptionID
}

// AdoptedObservation returns the observation an adoption produced for a
// delivery.
func (s *Store) AdoptedObservation(deliveryID, adoptionID string) (model.Observation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	adoption, ok := s.adoptions[adoptionID]
	if !ok {
		return model.Observation{}, false
	}
	for _, entry := range adoption.Entries {
		if entry.RawRef.DeliveryID == deliveryID {
			return entry.Observation, true
		}
	}
	return model.Observation{}, false
}

func (s *Store) GetAdoption(id string) (model.Adoption, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	adoption, ok := s.adoptions[id]
	if !ok {
		return model.Adoption{}, ErrNotFound
	}
	return adoption, nil
}

func (s *Store) AddDownstreamRef(ref model.DownstreamRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "downstream_ref", Downstream: &ref}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) DownstreamRefs() []model.DownstreamRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.DownstreamRef(nil), s.downstream...)
}
