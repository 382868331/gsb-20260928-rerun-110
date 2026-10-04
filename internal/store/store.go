package store

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"edge-ingest/internal/model"
)

var ErrNotFound = errors.New("not found")

type JournalRecord struct {
	Kind           string                     `json:"kind"`
	Identity       *model.DeviceIdentity      `json:"identity,omitempty"`
	Raw            *StoredRawMessage          `json:"raw,omitempty"`
	RawRef         *model.RawReference        `json:"raw_ref,omitempty"`
	Observation    *model.Observation         `json:"observation,omitempty"`
	Sequence       *SequenceProgress          `json:"sequence,omitempty"`
	Confirmed      *ConfirmedSequence         `json:"confirmed,omitempty"`
	Rule           string                     `json:"rule,omitempty"`
	Comparison     *model.Comparison          `json:"comparison,omitempty"`
	Conflict       *ConflictEvidence          `json:"conflict,omitempty"`
	Branch         *model.CorrectionBranch    `json:"branch,omitempty"`
	BranchEntry    *model.BranchEntry         `json:"branch_entry,omitempty"`
	Adoption       *model.Adoption            `json:"adoption,omitempty"`
	Withdrawal     *model.AdoptionWithdrawal  `json:"withdrawal,omitempty"`
	BranchConflict *model.BranchConflict      `json:"branch_conflict,omitempty"`
	Reference      *model.DownstreamReference `json:"reference,omitempty"`
	Aggregation    *model.Aggregation         `json:"aggregation,omitempty"`
	Revision       *model.AggregationRevision `json:"revision,omitempty"`
	Signoff        *model.AggregationSignoff  `json:"signoff,omitempty"`
	Checksum       string                     `json:"checksum,omitempty"`
}

// ConflictEvidence preserves both sides of a same-sequence conflict:
// the previously stored raw reference and the rejected incoming raw.
type ConflictEvidence struct {
	Existing   model.RawReference `json:"existing"`
	Incoming   StoredRawMessage   `json:"incoming"`
	RecordedAt time.Time          `json:"recorded_at"`
}

type StoredRawMessage struct {
	model.RawMessage
	Status      string `json:"status,omitempty"`
	DuplicateOf string `json:"duplicate_of,omitempty"`
}

func (r StoredRawMessage) RawReference() model.RawReference {
	return model.RawReference{
		DeliveryID:  r.DeliveryID,
		IdentityID:  r.IdentityID,
		BootID:      r.BootID,
		Sequence:    r.Sequence,
		Format:      r.Format,
		ContentType: r.ContentType,
		PayloadSHA:  PayloadSHA(r.Payload),
	}
}

type SequenceProgress struct {
	IdentityID string `json:"identity_id"`
	BootID     string `json:"boot_id"`
	Start      int64  `json:"start"`
	Last       int64  `json:"last"`
}

type ConfirmedSequence struct {
	IdentityID string `json:"identity_id"`
	BootID     string `json:"boot_id"`
	Last       int64  `json:"last"`
}

type Store struct {
	mu                 sync.Mutex
	path               string
	f                  *os.File
	writer             *bufio.Writer
	identities         map[string]model.DeviceIdentity
	byHardware         map[string]string
	seenObs            map[string]struct{}
	sequences          map[string]SequenceProgress
	delivered          map[string]map[int64]struct{}
	payloads           map[string]map[int64]string
	confirmed          map[string]map[int64]struct{}
	pending            map[string]model.RawMessage
	pendingIDs         []string
	raws               map[string]StoredRawMessage
	rawOrder           []string
	obsByDelivery      map[string]model.Observation
	reinterpreted      map[string]struct{}
	reinterpretRecords []JournalRecord
	conflicts          []ConflictEvidence
	branches           map[string]model.CorrectionBranch
	branchOrder        []string
	branchEntries      map[string]map[string]model.BranchEntry
	branchEntryOrder   []string
	currentByDelivery  map[string]model.CurrentValue
	currentOrder       []string
	adoptionByID       map[string]model.Adoption
	adoptionByDelivery map[string]model.Adoption
	withdrawals        map[string]map[string]struct{}
	branchConflicts    []model.BranchConflict
	references         map[string]model.DownstreamReference
	referenceOrder     []string
	aggregations       map[string]model.Aggregation
	aggregationOrder   []string
	revisions          map[string]model.AggregationRevision
	revisionOrder      []string
	signoffsByWindow   map[string]map[string]model.AggregationSignoff
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "journal.jsonl")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{
		path:               path,
		f:                  f,
		writer:             bufio.NewWriter(f),
		identities:         make(map[string]model.DeviceIdentity),
		byHardware:         make(map[string]string),
		seenObs:            make(map[string]struct{}),
		sequences:          make(map[string]SequenceProgress),
		delivered:          make(map[string]map[int64]struct{}),
		payloads:           make(map[string]map[int64]string),
		confirmed:          make(map[string]map[int64]struct{}),
		pending:            make(map[string]model.RawMessage),
		raws:               make(map[string]StoredRawMessage),
		obsByDelivery:      make(map[string]model.Observation),
		reinterpreted:      make(map[string]struct{}),
		branches:           make(map[string]model.CorrectionBranch),
		branchEntries:      make(map[string]map[string]model.BranchEntry),
		currentByDelivery:  make(map[string]model.CurrentValue),
		adoptionByID:       make(map[string]model.Adoption),
		adoptionByDelivery: make(map[string]model.Adoption),
		withdrawals:        make(map[string]map[string]struct{}),
		references:         make(map[string]model.DownstreamReference),
		aggregations:       make(map[string]model.Aggregation),
		revisions:          make(map[string]model.AggregationRevision),
		signoffsByWindow:   make(map[string]map[string]model.AggregationSignoff),
	}
	if err := s.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writer.Flush(); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	return s.f.Close()
}

func (s *Store) RegisterIdentity(identity model.DeviceIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.identities[identity.ID]; exists {
		return nil
	}
	record := JournalRecord{Kind: "identity", Identity: &identity}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) UpdateIdentity(identity model.DeviceIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.identities[identity.ID]; !exists {
		return ErrNotFound
	}
	record := JournalRecord{Kind: "identity", Identity: &identity}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) GetIdentity(id string) (model.DeviceIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, ok := s.identities[id]
	if !ok {
		return model.DeviceIdentity{}, ErrNotFound
	}
	return identity, nil
}

func (s *Store) ActiveHardware(hardwareID string) (model.DeviceIdentity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byHardware[hardwareID]
	if !ok {
		return model.DeviceIdentity{}, false
	}
	return s.identities[id], true
}

func (s *Store) ReplaceIdentity(oldID, newID, hardwareID, deviceType string, now time.Time) (model.DeviceIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if currentID, ok := s.byHardware[hardwareID]; ok && currentID != oldID {
		return model.DeviceIdentity{}, fmt.Errorf("hardware %s currently belongs to %s", hardwareID, currentID)
	}
	old := s.identities[oldID]
	old.Active = false
	old.ReplacedBy = newID
	next := model.DeviceIdentity{ID: newID, HardwareID: hardwareID, Type: deviceType, Active: true, CreatedAt: now}
	oldRecord := JournalRecord{Kind: "identity", Identity: &old}
	if err := s.append(oldRecord); err != nil {
		return model.DeviceIdentity{}, err
	}
	newRecord := JournalRecord{Kind: "identity", Identity: &next}
	if err := s.append(newRecord); err != nil {
		return model.DeviceIdentity{}, err
	}
	s.apply(oldRecord)
	s.apply(newRecord)
	return next, nil
}

func (s *Store) HasObservation(identityID, observationID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.seenObs[observationKey(identityID, observationID)]
	return ok
}

func (s *Store) Sequence(identityID, bootID string) (SequenceProgress, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.sequences[sequenceKey(identityID, bootID)]
	return p, ok
}

func (s *Store) Confirmed(identityID, bootID string) (ConfirmedSequence, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	confirmed, ok := s.confirmed[sequenceKey(identityID, bootID)]
	if !ok {
		return ConfirmedSequence{}, false
	}
	progress := s.sequences[sequenceKey(identityID, bootID)]
	last, ok := highestContiguous(confirmed, progress.Start)
	return ConfirmedSequence{IdentityID: identityID, BootID: bootID, Last: last}, ok
}

func (s *Store) Missing(identityID, bootID string) []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	progress, ok := s.sequences[sequenceKey(identityID, bootID)]
	if !ok {
		return nil
	}
	have := s.delivered[sequenceKey(identityID, bootID)]
	var missing []int64
	for seq := progress.Start; seq <= progress.Last; seq++ {
		if have == nil {
			missing = append(missing, seq)
			continue
		}
		if _, ok := have[seq]; !ok {
			missing = append(missing, seq)
		}
	}
	return missing
}

func (s *Store) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

func highestContiguous(set map[int64]struct{}, start int64) (int64, bool) {
	last := start - 1
	found := false
	for {
		if _, ok := set[start]; !ok {
			return last, found
		}
		found = true
		last = start
		start++
	}
}

type IngestResult struct {
	Duplicate   bool
	DuplicateOf string
	Pending     bool
	Conflict    bool
	Observation *model.Observation
}

func (s *Store) Ingest(raw model.RawMessage, observation *model.Observation) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ingestLocked(raw, observation, observation == nil)
}

func (s *Store) StorePending(raw model.RawMessage) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ingestLocked(raw, nil, true)
}

func (s *Store) Pending() []model.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := make([]model.RawMessage, 0, len(s.pendingIDs))
	for _, id := range s.pendingIDs {
		if raw, ok := s.pending[id]; ok {
			pending = append(pending, cloneRaw(raw))
		}
	}
	return pending
}

func (s *Store) Confirm(raw model.RawMessage, observation model.Observation) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deliveryKey := sequenceKey(raw.IdentityID, raw.BootID)
	existingPayload, rawExists := s.payloads[deliveryKey][raw.Sequence]
	if rawExists && existingPayload != PayloadSHA(raw.Payload) {
		s.recordConflictLocked(raw, existingPayload)
		return IngestResult{Conflict: true}, fmt.Errorf("sequence %d in boot %s already contains different raw content", raw.Sequence, raw.BootID)
	}
	storedRaw, pendingExists := s.pending[raw.DeliveryID]
	if rawExists && (!pendingExists || PayloadSHA(storedRaw.Payload) != PayloadSHA(raw.Payload)) {
		return IngestResult{}, fmt.Errorf("delivery %s was not stored as a pending raw message", raw.DeliveryID)
	}
	obKey := observationKey(observation.IdentityID, observation.ID)
	if _, duplicate := s.seenObs[obKey]; duplicate {
		record := JournalRecord{
			Kind: "duplicate_observation",
			RawRef: &model.RawReference{
				DeliveryID:  raw.DeliveryID,
				IdentityID:  raw.IdentityID,
				BootID:      raw.BootID,
				Sequence:    raw.Sequence,
				Format:      raw.Format,
				ContentType: raw.ContentType,
				PayloadSHA:  PayloadSHA(raw.Payload),
			},
			Confirmed: &ConfirmedSequence{IdentityID: raw.IdentityID, BootID: raw.BootID, Last: raw.Sequence},
		}
		if err := s.append(record); err != nil {
			return IngestResult{}, err
		}
		s.apply(record)
		return IngestResult{Duplicate: true, DuplicateOf: obKey, Observation: nil}, nil
	}
	reference := model.RawReference{
		DeliveryID:  raw.DeliveryID,
		IdentityID:  raw.IdentityID,
		BootID:      raw.BootID,
		Sequence:    raw.Sequence,
		Format:      raw.Format,
		ContentType: raw.ContentType,
		PayloadSHA:  PayloadSHA(raw.Payload),
	}
	confirmed := ConfirmedSequence{IdentityID: raw.IdentityID, BootID: raw.BootID, Last: raw.Sequence}
	record := JournalRecord{Kind: "observation", RawRef: &reference, Observation: &observation, Confirmed: &confirmed}
	if err := s.append(record); err != nil {
		return IngestResult{}, err
	}
	s.apply(record)
	return IngestResult{Observation: &observation}, nil
}

// recordConflictLocked appends a conflict evidence record. The sequence
// watermarks are not touched: the rejected raw never advances progress.
func (s *Store) recordConflictLocked(raw model.RawMessage, existingSHA string) error {
	evidence := ConflictEvidence{
		Existing: model.RawReference{
			DeliveryID: raw.DeliveryID,
			IdentityID: raw.IdentityID,
			BootID:     raw.BootID,
			Sequence:   raw.Sequence,
			PayloadSHA: existingSHA,
		},
		Incoming:   StoredRawMessage{RawMessage: cloneRaw(raw), Status: "conflict"},
		RecordedAt: time.Now().UTC(),
	}
	record := JournalRecord{Kind: "conflict", Conflict: &evidence}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) Conflicts() []ConflictEvidence {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ConflictEvidence(nil), s.conflicts...)
}

// RawMessages returns stored raw messages, optionally filtered by identity
// and receive-time range (zero bounds are ignored). Pending and confirmed
// raws are both included so reinterpretation can cover every stored message.
func (s *Store) RawMessages(identityID string, from, to time.Time) []StoredRawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []StoredRawMessage
	for _, id := range s.rawOrder {
		raw := s.raws[id]
		if identityID != "" && raw.IdentityID != identityID {
			continue
		}
		if !from.IsZero() && raw.ReceivedAt.Before(from) {
			continue
		}
		if !to.IsZero() && raw.ReceivedAt.After(to) {
			continue
		}
		out = append(out, StoredRawMessage{RawMessage: cloneRaw(raw.RawMessage), Status: raw.Status, DuplicateOf: raw.DuplicateOf})
	}
	return out
}

// ObservationFor returns the confirmed observation produced for a delivery.
func (s *Store) ObservationFor(deliveryID string) (model.Observation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obs, ok := s.obsByDelivery[deliveryID]
	return obs, ok
}

// Reinterpreted reports whether a delivery was already reinterpreted with
// the given rule, which keeps reprocessing idempotent across restarts.
func (s *Store) Reinterpreted(deliveryID, rule string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.reinterpreted[reinterpretKey(deliveryID, rule)]
	return ok
}

// Reinterpret atomically stores a reinterpretation of a stored raw message
// under a corrected rule. The original observation is never modified.
func (s *Store) Reinterpret(raw model.RawMessage, rule string, observation model.Observation, comparison model.Comparison) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.reinterpreted[reinterpretKey(raw.DeliveryID, rule)]; ok {
		return nil
	}
	reference := model.RawReference{
		DeliveryID:  raw.DeliveryID,
		IdentityID:  raw.IdentityID,
		BootID:      raw.BootID,
		Sequence:    raw.Sequence,
		Format:      raw.Format,
		ContentType: raw.ContentType,
		PayloadSHA:  PayloadSHA(raw.Payload),
	}
	record := JournalRecord{
		Kind:        "reinterpretation",
		RawRef:      &reference,
		Observation: &observation,
		Rule:        rule,
		Comparison:  &comparison,
	}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

// Reinterpretations lists stored reinterpretation records for comparison output.
func (s *Store) Reinterpretations() []JournalRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []JournalRecord
	for _, record := range s.reinterpretRecords {
		out = append(out, record)
	}
	return out
}

func (s *Store) SaveBranch(branch model.CorrectionBranch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.branches[branch.ID]; exists {
		return nil
	}
	record := JournalRecord{Kind: "correction_branch", Branch: &branch}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) UpdateBranch(branch model.CorrectionBranch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.branches[branch.ID]; !exists {
		return ErrNotFound
	}
	record := JournalRecord{Kind: "correction_branch", Branch: &branch}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) Branch(id string) (model.CorrectionBranch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	branch, ok := s.branches[id]
	return branch, ok
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

func (s *Store) BranchEntryExists(branchID, deliveryID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.branchEntries[branchID][deliveryID]
	return ok
}

func (s *Store) SaveBranchEntry(entry model.BranchEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entries := s.branchEntries[entry.BranchID]; entries != nil {
		if _, exists := entries[entry.RawRef.DeliveryID]; exists {
			return nil
		}
	}
	record := JournalRecord{Kind: "branch_entry", BranchEntry: &entry}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) UpdateBranchEntry(entry model.BranchEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "branch_entry", BranchEntry: &entry}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) BranchEntries(branchID string) []model.BranchEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.BranchEntry
	for _, key := range s.branchEntryOrder {
		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) == 2 && parts[0] == branchID {
			out = append(out, s.branchEntries[branchID][parts[1]])
		}
	}
	return out
}

func (s *Store) CurrentAdoption(deliveryID string) (model.Adoption, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	adoption, ok := s.adoptionByDelivery[deliveryID]
	return adoption, ok
}

func (s *Store) Withdrawn(adoptionID, deliveryID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.withdrawals[adoptionID][deliveryID]
	return ok
}

func (s *Store) Adopt(adoption model.Adoption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "adoption", Adoption: &adoption}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) Adoption(id string) (model.Adoption, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	adoption, ok := s.adoptionByID[id]
	return adoption, ok
}

func (s *Store) WithdrawAdoption(withdrawal model.AdoptionWithdrawal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "adoption_withdrawal", Withdrawal: &withdrawal}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) RecordBranchConflict(conflict model.BranchConflict) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := JournalRecord{Kind: "branch_conflict", BranchConflict: &conflict}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) BranchConflicts() []model.BranchConflict {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.BranchConflict(nil), s.branchConflicts...)
}

func (s *Store) RegisterReference(reference model.DownstreamReference) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.references[reference.ID]; exists {
		return nil
	}
	record := JournalRecord{Kind: "downstream_reference", Reference: &reference}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) References() []model.DownstreamReference {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.DownstreamReference, 0, len(s.referenceOrder))
	for _, id := range s.referenceOrder {
		out = append(out, s.references[id])
	}
	return out
}

func (s *Store) CurrentValues(scope model.BranchScope) []model.CurrentValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.CurrentValue, 0, len(s.currentOrder))
	for _, id := range s.currentOrder {
		current := s.currentByDelivery[id]
		if scope.Matches(current.RawRef.IdentityID, current.RawRef.BootID, current.RawRef.Sequence, current.Observation.ReceivedAt) {
			out = append(out, current)
		}
	}
	return out
}

func (s *Store) CurrentValue(deliveryID string) (model.CurrentValue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.currentByDelivery[deliveryID]
	return current, ok
}

func (s *Store) SaveAggregation(aggregation model.Aggregation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.aggregations[aggregation.ID]; exists {
		return nil
	}
	record := JournalRecord{Kind: "aggregation", Aggregation: &aggregation}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) Aggregation(id string) (model.Aggregation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	aggregation, ok := s.aggregations[id]
	return aggregation, ok
}

func (s *Store) Aggregations() []model.Aggregation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Aggregation, 0, len(s.aggregationOrder))
	for _, id := range s.aggregationOrder {
		out = append(out, s.aggregations[id])
	}
	return out
}

func (s *Store) SaveRevision(revision model.AggregationRevision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.revisions[revision.ID]; exists {
		return nil
	}
	record := JournalRecord{Kind: "aggregation_revision", Revision: &revision}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) UpdateRevision(revision model.AggregationRevision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.revisions[revision.ID]; !exists {
		return ErrNotFound
	}
	record := JournalRecord{Kind: "aggregation_revision", Revision: &revision}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) Revision(id string) (model.AggregationRevision, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	revision, ok := s.revisions[id]
	return revision, ok
}

func (s *Store) Revisions(aggregationID string) []model.AggregationRevision {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.AggregationRevision
	for _, id := range s.revisionOrder {
		revision := s.revisions[id]
		if aggregationID == "" || revision.AggregationID == aggregationID {
			out = append(out, revision)
		}
	}
	return out
}

func (s *Store) SaveAggregationSignoff(signoff model.AggregationSignoff) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := signoff.AggregationID + "\x00" + signoff.WindowID
	if _, ok := s.signoffsByWindow[key][signoffKey(signoff.Operator, signoff.RevisionID)]; ok {
		return nil
	}
	record := JournalRecord{Kind: "aggregation_signoff", Signoff: &signoff}
	if err := s.append(record); err != nil {
		return err
	}
	s.apply(record)
	return nil
}

func (s *Store) WindowSignoffs(aggregationID, windowID string) []model.AggregationSignoff {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.AggregationSignoff
	for _, signoff := range s.signoffsByWindow[aggregationID+"\x00"+windowID] {
		out = append(out, signoff)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].SignedAt.Before(out[j].SignedAt) })
	return out
}

func signoffKey(operator, revisionID string) string {
	return operator + "\x00" + revisionID
}

func reinterpretKey(deliveryID, rule string) string {
	return deliveryID + "\x00" + rule
}

func PayloadSHA(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (s *Store) ingestLocked(raw model.RawMessage, observation *model.Observation, pending bool) (IngestResult, error) {
	result := IngestResult{Observation: observation, Pending: pending}
	key := sequenceKey(raw.IdentityID, raw.BootID)
	if existing, ok := s.payloads[key][raw.Sequence]; ok {
		if existing != PayloadSHA(raw.Payload) {
			s.recordConflictLocked(raw, existing)
			return IngestResult{Conflict: true}, fmt.Errorf("sequence %d in boot %s already contains different raw content", raw.Sequence, raw.BootID)
		}
		if pending {
			if _, isPending := s.pending[raw.DeliveryID]; isPending {
				return IngestResult{Pending: true}, nil
			}
		}
	}
	progress, exists := s.sequences[key]
	storedRaw := StoredRawMessage{RawMessage: raw}
	if pending {
		storedRaw.Status = "pending"
	} else {
		storedRaw.Status = "confirmed"
	}
	if observation != nil {
		obKey := observationKey(observation.IdentityID, observation.ID)
		if _, duplicate := s.seenObs[obKey]; duplicate {
			storedRaw.Status = "duplicate"
			storedRaw.DuplicateOf = obKey
			result.Duplicate = true
			result.DuplicateOf = obKey
			result.Pending = false
			result.Observation = nil
		}
	}
	if !exists {
		progress = SequenceProgress{IdentityID: raw.IdentityID, BootID: raw.BootID, Start: raw.Sequence, Last: raw.Sequence}
	} else if raw.Sequence > progress.Last {
		progress.Last = raw.Sequence
	}
	record := JournalRecord{Kind: "ingest", Raw: &storedRaw, Sequence: &progress}
	if observation != nil && !result.Duplicate {
		record.Observation = observation
		record.Confirmed = &ConfirmedSequence{IdentityID: raw.IdentityID, BootID: raw.BootID, Last: raw.Sequence}
	}
	if err := s.append(record); err != nil {
		return IngestResult{}, err
	}
	s.apply(record)
	return result, nil
}

func cloneRaw(raw model.RawMessage) model.RawMessage {
	raw.Payload = append([]byte(nil), raw.Payload...)
	return raw
}

func (s *Store) ReadAll() ([]JournalRecord, error) {
	s.mu.Lock()
	path := s.path
	s.mu.Unlock()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readJournal(f)
}

func (s *Store) replay() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(s.f)
	if err != nil {
		return err
	}
	records, validBytes, err := parseJournal(data)
	if err != nil {
		return err
	}
	if validBytes < len(data) {
		if err := s.f.Truncate(int64(validBytes)); err != nil {
			return err
		}
	}
	for _, record := range records {
		s.apply(record)
	}
	if _, err := s.f.Seek(int64(validBytes), io.SeekStart); err != nil {
		return err
	}
	s.writer.Reset(s.f)
	return nil
}

func (s *Store) append(record JournalRecord) error {
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	record.Checksum = hex.EncodeToString(sum[:])
	b, err = json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := s.writer.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *Store) apply(record JournalRecord) {
	switch record.Kind {
	case "ingest":
		if record.Raw != nil {
			if _, exists := s.raws[record.Raw.DeliveryID]; !exists {
				s.rawOrder = append(s.rawOrder, record.Raw.DeliveryID)
			}
			s.raws[record.Raw.DeliveryID] = *record.Raw
			key := sequenceKey(record.Raw.IdentityID, record.Raw.BootID)
			if s.delivered[key] == nil {
				s.delivered[key] = make(map[int64]struct{})
			}
			s.delivered[key][record.Raw.Sequence] = struct{}{}
			if s.payloads[key] == nil {
				s.payloads[key] = make(map[int64]string)
			}
			s.payloads[key][record.Raw.Sequence] = PayloadSHA(record.Raw.Payload)
			if record.Raw.Status == "pending" {
				if _, exists := s.pending[record.Raw.DeliveryID]; !exists {
					s.pending[record.Raw.DeliveryID] = cloneRaw(record.Raw.RawMessage)
					s.pendingIDs = append(s.pendingIDs, record.Raw.DeliveryID)
				}
			} else {
				delete(s.pending, record.Raw.DeliveryID)
			}
			if s.confirmed[key] == nil {
				s.confirmed[key] = make(map[int64]struct{})
			}
			if record.Raw.Status != "pending" && record.Observation != nil {
				s.confirmed[key][record.Raw.Sequence] = struct{}{}
			}
		}
		if record.Raw != nil && record.Raw.DuplicateOf != "" {
			s.seenObs[record.Raw.DuplicateOf] = struct{}{}
		}
		if record.Observation != nil {
			s.seenObs[observationKey(record.Observation.IdentityID, record.Observation.ID)] = struct{}{}
			if record.Raw != nil {
				s.obsByDelivery[record.Raw.DeliveryID] = *record.Observation
				s.setCurrentOriginalLocked(record.Raw.RawReference(), *record.Observation)
			}
		}
		if record.Sequence != nil {
			s.sequences[sequenceKey(record.Sequence.IdentityID, record.Sequence.BootID)] = *record.Sequence
		}
	case "reinterpretation":
		if record.RawRef != nil {
			s.reinterpreted[reinterpretKey(record.RawRef.DeliveryID, record.Rule)] = struct{}{}
		}
		s.reinterpretRecords = append(s.reinterpretRecords, record)
	case "correction_branch":
		if record.Branch != nil {
			if _, exists := s.branches[record.Branch.ID]; !exists {
				s.branchOrder = append(s.branchOrder, record.Branch.ID)
			}
			s.branches[record.Branch.ID] = *record.Branch
		}
	case "branch_entry":
		if record.BranchEntry != nil {
			entry := *record.BranchEntry
			if s.branchEntries[entry.BranchID] == nil {
				s.branchEntries[entry.BranchID] = make(map[string]model.BranchEntry)
			}
			deliveryID := entry.RawRef.DeliveryID
			if _, exists := s.branchEntries[entry.BranchID][deliveryID]; !exists {
				s.branchEntryOrder = append(s.branchEntryOrder, entry.BranchID+"\x00"+deliveryID)
			}
			s.branchEntries[entry.BranchID][deliveryID] = entry
		}
	case "adoption":
		if record.Adoption != nil {
			adoption := *record.Adoption
			s.adoptionByID[adoption.ID] = adoption
			entries := s.branchEntries[adoption.BranchID]
			for _, deliveryID := range adoption.DeliveryIDs {
				entry, ok := entries[deliveryID]
				if !ok {
					continue
				}
				s.seenObs[observationKey(entry.Observation.IdentityID, entry.Observation.ID)] = struct{}{}
				s.adoptionByDelivery[deliveryID] = adoption
				if s.withdrawals[adoption.ID] != nil {
					if _, withdrawn := s.withdrawals[adoption.ID][deliveryID]; withdrawn {
						continue
					}
				}
				entry.Status = model.EntryAdopted
				s.branchEntries[adoption.BranchID][deliveryID] = entry
				key := sequenceKey(entry.RawRef.IdentityID, entry.RawRef.BootID)
				if s.confirmed[key] == nil {
					s.confirmed[key] = make(map[int64]struct{})
				}
				s.confirmed[key][entry.RawRef.Sequence] = struct{}{}
				delete(s.pending, deliveryID)
				s.setCurrentBranchLocked(entry, adoption)
			}
		}
	case "adoption_withdrawal":
		if record.Withdrawal != nil {
			withdrawal := *record.Withdrawal
			if s.withdrawals[withdrawal.AdoptionID] == nil {
				s.withdrawals[withdrawal.AdoptionID] = make(map[string]struct{})
			}
			for _, deliveryID := range withdrawal.DeliveryIDs {
				s.withdrawals[withdrawal.AdoptionID][deliveryID] = struct{}{}
				if adoption, ok := s.adoptionByID[withdrawal.AdoptionID]; ok {
					_ = adoption
				}
				if stored, ok := s.raws[deliveryID]; ok {
					if original, exists := s.obsByDelivery[deliveryID]; exists {
						current := model.CurrentValue{
							DeliveryID:       deliveryID,
							RawRef:           stored.RawReference(),
							Observation:      original,
							Source:           "original",
							Withdrawn:        true,
							WithdrawalID:     withdrawal.ID,
							WithdrawnAt:      withdrawal.WithdrawnAt,
							WithdrawalReason: withdrawal.Reason,
						}
						s.setCurrentLocked(current)
					}
				}
			}
		}
	case "branch_conflict":
		if record.BranchConflict != nil {
			s.branchConflicts = append(s.branchConflicts, *record.BranchConflict)
		}
	case "downstream_reference":
		if record.Reference != nil {
			if _, exists := s.references[record.Reference.ID]; !exists {
				s.referenceOrder = append(s.referenceOrder, record.Reference.ID)
			}
			s.references[record.Reference.ID] = *record.Reference
		}
	case "aggregation":
		if record.Aggregation != nil {
			if _, exists := s.aggregations[record.Aggregation.ID]; !exists {
				s.aggregationOrder = append(s.aggregationOrder, record.Aggregation.ID)
			}
			s.aggregations[record.Aggregation.ID] = *record.Aggregation
		}
	case "aggregation_revision":
		if record.Revision != nil {
			if _, exists := s.revisions[record.Revision.ID]; !exists {
				s.revisionOrder = append(s.revisionOrder, record.Revision.ID)
			}
			revision := *record.Revision
			for i := range revision.Windows {
				key := revision.AggregationID + "\x00" + revision.Windows[i].Window.ID
				for _, signoff := range s.signoffsByWindow[key] {
					if signoff.RevisionID == revision.ID {
						revision.Windows[i].Status = model.AggregationWindowSigned
					}
				}
			}
			s.revisions[record.Revision.ID] = revision
		}
	case "aggregation_signoff":
		if record.Signoff != nil {
			signoff := *record.Signoff
			key := signoff.AggregationID + "\x00" + signoff.WindowID
			if s.signoffsByWindow[key] == nil {
				s.signoffsByWindow[key] = make(map[string]model.AggregationSignoff)
			}
			s.signoffsByWindow[key][signoffKey(signoff.Operator, signoff.RevisionID)] = signoff
			revision := s.revisions[signoff.RevisionID]
			for i := range revision.Windows {
				if revision.Windows[i].Window.ID != signoff.WindowID {
					continue
				}
				hasSignoff := false
				for _, prior := range revision.Windows[i].Signoffs {
					if prior.ID == signoff.ID {
						hasSignoff = true
					}
				}
				if !hasSignoff {
					revision.Windows[i].Status = model.AggregationWindowSigned
					revision.Windows[i].Signoffs = append(revision.Windows[i].Signoffs, signoff)
				}
			}
			signed := 0
			for _, window := range revision.Windows {
				if window.Status == model.AggregationWindowSigned {
					signed++
				}
			}
			if signed > 0 {
				revision.Status = model.AggregationRevisionPartial
			}
			if signed == len(revision.Windows) {
				revision.Status = model.AggregationRevisionSigned
			}
			s.revisions[revision.ID] = revision
		}
	case "conflict":
		if record.Conflict != nil {
			s.conflicts = append(s.conflicts, *record.Conflict)
		}
	case "identity":
		if record.Identity == nil {
			return
		}
		s.identities[record.Identity.ID] = *record.Identity
		if record.Identity.Active {
			s.byHardware[record.Identity.HardwareID] = record.Identity.ID
		} else if current := s.byHardware[record.Identity.HardwareID]; current == record.Identity.ID {
			delete(s.byHardware, record.Identity.HardwareID)
		}
	case "raw":
		if record.Raw != nil && record.Raw.DuplicateOf != "" {
			s.seenObs[record.Raw.DuplicateOf] = struct{}{}
		}
	case "observation", "duplicate_observation":
		s.applyObservation(record)
	case "sequence":
		if record.Sequence != nil {
			s.sequences[sequenceKey(record.Sequence.IdentityID, record.Sequence.BootID)] = *record.Sequence
		}
	}
}

func (s *Store) applyObservation(record JournalRecord) {
	if record.RawRef != nil {
		key := sequenceKey(record.RawRef.IdentityID, record.RawRef.BootID)
		if s.confirmed[key] == nil {
			s.confirmed[key] = make(map[int64]struct{})
		}
		s.confirmed[key][record.RawRef.Sequence] = struct{}{}
		deliveryID := record.RawRef.DeliveryID
		if deliveryID == "" {
			deliveryID = sequenceKey(record.RawRef.IdentityID, record.RawRef.BootID) + ":" + fmt.Sprint(record.RawRef.Sequence)
		}
		delete(s.pending, deliveryID)
	}
	if record.Observation != nil {
		s.seenObs[observationKey(record.Observation.IdentityID, record.Observation.ID)] = struct{}{}
		if record.RawRef != nil {
			s.obsByDelivery[record.RawRef.DeliveryID] = *record.Observation
			if record.Kind == "observation" {
				if stored, ok := s.raws[record.RawRef.DeliveryID]; ok {
					s.setCurrentOriginalLocked(stored.RawReference(), *record.Observation)
				}
			}
		}
	}
	if record.Confirmed != nil {
		key := sequenceKey(record.Confirmed.IdentityID, record.Confirmed.BootID)
		if s.confirmed[key] == nil {
			s.confirmed[key] = make(map[int64]struct{})
		}
		s.confirmed[key][record.Confirmed.Last] = struct{}{}
	}
}

func (s *Store) setCurrentOriginalLocked(reference model.RawReference, observation model.Observation) {
	s.setCurrentLocked(model.CurrentValue{
		DeliveryID:  reference.DeliveryID,
		RawRef:      reference,
		Observation: observation,
		Source:      "original",
	})
}

func (s *Store) setCurrentBranchLocked(entry model.BranchEntry, adoption model.Adoption) {
	s.setCurrentLocked(model.CurrentValue{
		DeliveryID:  entry.RawRef.DeliveryID,
		RawRef:      entry.RawRef,
		Observation: entry.Observation,
		Source:      "branch",
		BranchID:    entry.BranchID,
		Rule:        entry.Rule,
		AdoptionID:  adoption.ID,
		AdoptedAt:   adoption.AdoptedAt,
	})
}

func (s *Store) setCurrentLocked(current model.CurrentValue) {
	if _, exists := s.currentByDelivery[current.DeliveryID]; !exists {
		s.currentOrder = append(s.currentOrder, current.DeliveryID)
	}
	s.currentByDelivery[current.DeliveryID] = current
}

func readJournal(r io.Reader) ([]JournalRecord, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	records, _, err := parseJournal(data)
	return records, err
}

func parseJournal(data []byte) ([]JournalRecord, int, error) {
	var records []JournalRecord
	lineStart := 0
	lineNo := 0
	for lineStart < len(data) {
		lineNo++
		next := bytes.IndexByte(data[lineStart:], '\n')
		lineEnd := len(data)
		if next >= 0 {
			lineEnd = lineStart + next
		}
		line := bytes.TrimSpace(data[lineStart:lineEnd])
		isLast := next < 0
		if len(line) != 0 {
			var record JournalRecord
			if err := json.Unmarshal(line, &record); err != nil {
				if isLast {
					return records, lineStart, nil
				}
				return nil, 0, fmt.Errorf("journal line %d: %w", lineNo, err)
			}
			got := record.Checksum
			record.Checksum = ""
			b, err := json.Marshal(record)
			if err != nil {
				return nil, 0, fmt.Errorf("journal line %d: %w", lineNo, err)
			}
			sum := sha256.Sum256(b)
			if got != hex.EncodeToString(sum[:]) {
				if isLast {
					return records, lineStart, nil
				}
				return nil, 0, fmt.Errorf("journal line %d: checksum mismatch", lineNo)
			}
			record.Checksum = got
			records = append(records, record)
		}
		if next < 0 {
			return records, len(data), nil
		}
		lineStart = lineEnd + 1
	}
	return records, len(data), nil
}

func observationKey(identityID, observationID string) string {
	return identityID + "\\x00" + observationID
}

func sequenceKey(identityID, bootID string) string {
	return identityID + "\\x00" + bootID
}
