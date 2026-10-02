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
	"sync"
	"time"

	"edge-ingest/internal/model"
)

var ErrNotFound = errors.New("not found")

type JournalRecord struct {
	Kind        string                  `json:"kind"`
	Identity    *model.DeviceIdentity   `json:"identity,omitempty"`
	Raw         *StoredRawMessage       `json:"raw,omitempty"`
	RawRef      *model.RawReference     `json:"raw_ref,omitempty"`
	Observation *model.Observation      `json:"observation,omitempty"`
	Sequence    *SequenceProgress       `json:"sequence,omitempty"`
	Confirmed   *ConfirmedSequence      `json:"confirmed,omitempty"`
	Rule        string                  `json:"rule,omitempty"`
	Comparison  *model.Comparison       `json:"comparison,omitempty"`
	Conflict    *ConflictEvidence       `json:"conflict,omitempty"`
	Branch      *model.CorrectionBranch `json:"branch,omitempty"`
	Adoption    *model.Adoption         `json:"adoption,omitempty"`
	Retraction  *model.Retraction       `json:"retraction,omitempty"`
	Downstream  *model.DownstreamRef    `json:"downstream,omitempty"`
	Checksum    string                  `json:"checksum,omitempty"`
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
	adoptions          map[string]model.Adoption
	interpStack        map[string][]interpState
	downstream         []model.DownstreamRef
	revision           int64
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
		path:          path,
		f:             f,
		writer:        bufio.NewWriter(f),
		identities:    make(map[string]model.DeviceIdentity),
		byHardware:    make(map[string]string),
		seenObs:       make(map[string]struct{}),
		sequences:     make(map[string]SequenceProgress),
		delivered:     make(map[string]map[int64]struct{}),
		payloads:      make(map[string]map[int64]string),
		confirmed:     make(map[string]map[int64]struct{}),
		pending:       make(map[string]model.RawMessage),
		raws:          make(map[string]StoredRawMessage),
		obsByDelivery: make(map[string]model.Observation),
		reinterpreted: make(map[string]struct{}),
		branches:      make(map[string]model.CorrectionBranch),
		adoptions:     make(map[string]model.Adoption),
		interpStack:   make(map[string][]interpState),
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
	case "conflict":
		if record.Conflict != nil {
			s.conflicts = append(s.conflicts, *record.Conflict)
		}
	case "correction_branch":
		s.applyBranch(record.Branch)
	case "adoption":
		s.applyBranch(record.Branch)
		if record.Adoption != nil {
			s.adoptions[record.Adoption.ID] = *record.Adoption
			if record.Adoption.Revision > s.revision {
				s.revision = record.Adoption.Revision
			}
			for _, entry := range record.Adoption.Entries {
				deliveryID := entry.RawRef.DeliveryID
				s.interpStack[deliveryID] = append(s.interpStack[deliveryID], interpState{Rule: record.Adoption.Rule, AdoptionID: record.Adoption.ID})
			}
		}
	case "retraction":
		if record.Retraction != nil {
			if record.Retraction.Revision > s.revision {
				s.revision = record.Retraction.Revision
			}
			for _, deliveryID := range record.Retraction.DeliveryIDs {
				stack := s.interpStack[deliveryID]
				if n := len(stack); n > 0 && stack[n-1].AdoptionID == record.Retraction.AdoptionID {
					s.interpStack[deliveryID] = stack[:n-1]
				}
			}
		}
	case "downstream_ref":
		if record.Downstream != nil {
			s.downstream = append(s.downstream, *record.Downstream)
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
	case "observation":
		s.applyObservation(record)
	case "duplicate_observation":
		s.applyObservation(record)
	case "sequence":
		if record.Sequence != nil {
			s.sequences[sequenceKey(record.Sequence.IdentityID, record.Sequence.BootID)] = *record.Sequence
		}
	}
}

func (s *Store) applyBranch(branch *model.CorrectionBranch) {
	if branch == nil {
		return
	}
	if _, exists := s.branches[branch.ID]; !exists {
		s.branchOrder = append(s.branchOrder, branch.ID)
	}
	s.branches[branch.ID] = *branch
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
