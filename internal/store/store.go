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

var (
	ErrSequenceConflict = errors.New("sequence conflict")
	ErrPendingNotFound  = errors.New("pending message not found")
)

type JournalRecord struct {
	Kind                string                `json:"kind"`
	Identity            *model.DeviceIdentity `json:"identity,omitempty"`
	Raw                 *StoredRawMessage     `json:"raw,omitempty"`
	Observation         *model.Observation    `json:"observation,omitempty"`
	Sequence            *SequenceProgress     `json:"sequence,omitempty"`
	PendingID           string                `json:"pending_id,omitempty"`
	ResolvedDuplicateOf string                `json:"resolved_duplicate_of,omitempty"`
	Checksum            string                `json:"checksum,omitempty"`
}

type StoredRawMessage struct {
	model.RawMessage
	DuplicateOf string `json:"duplicate_of,omitempty"`
}

type SequenceProgress struct {
	IdentityID string `json:"identity_id"`
	BootID     string `json:"boot_id"`
	Start      int64  `json:"start"`
	Last       int64  `json:"last"`
}

type Store struct {
	mu         sync.Mutex
	path       string
	f          *os.File
	writer     *bufio.Writer
	identities map[string]model.DeviceIdentity
	byHardware map[string]string
	seenObs    map[string]struct{}
	sequences  map[string]SequenceProgress
	delivered  map[string]map[int64]struct{}
	rawBySeq   map[string]map[int64]StoredRawMessage
	pending    map[string]StoredRawMessage
	resolved   map[string]string
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
		path:       path,
		f:          f,
		writer:     bufio.NewWriter(f),
		identities: make(map[string]model.DeviceIdentity),
		byHardware: make(map[string]string),
		seenObs:    make(map[string]struct{}),
		sequences:  make(map[string]SequenceProgress),
		delivered:  make(map[string]map[int64]struct{}),
		rawBySeq:   make(map[string]map[int64]StoredRawMessage),
		pending:    make(map[string]StoredRawMessage),
		resolved:   make(map[string]string),
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

type IngestResult struct {
	Duplicate   bool
	DuplicateOf string
	Pending     bool
	PendingID   string
	Observation *model.Observation
}

func (s *Store) Ingest(raw model.RawMessage, observation *model.Observation) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := IngestResult{Observation: observation}
	storedRaw := StoredRawMessage{RawMessage: raw}
	key := sequenceKey(raw.IdentityID, raw.BootID)
	progress, exists := s.sequences[key]
	if !exists {
		progress = SequenceProgress{IdentityID: raw.IdentityID, BootID: raw.BootID, Start: raw.Sequence, Last: raw.Sequence - 1}
	}
	if observation != nil {
		obKey := observationKey(observation.IdentityID, observation.ID)
		if _, duplicate := s.seenObs[obKey]; duplicate {
			storedRaw.DuplicateOf = obKey
			result.Duplicate = true
			result.DuplicateOf = obKey
			result.Observation = nil
		}
	}
	var progressUpdate *SequenceProgress
	if !exists || raw.Sequence > progress.Last {
		progress.Last = raw.Sequence
		progressUpdate = &progress
	}
	record := JournalRecord{Kind: "ingest", Raw: &storedRaw, Sequence: progressUpdate}
	if observation != nil && !result.Duplicate {
		record.Observation = observation
	}
	if existing, conflict := s.conflictingRaw(raw); conflict {
		if !sameRaw(existing.RawMessage, raw) {
			return IngestResult{}, fmt.Errorf("%w: %s %s sequence %d", ErrSequenceConflict, raw.IdentityID, raw.BootID, raw.Sequence)
		}
		result.Observation = nil
	}
	if err := s.append(record); err != nil {
		return IngestResult{}, err
	}
	s.apply(record)
	return result, nil
}

func (s *Store) IngestPending(raw model.RawMessage) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.pending[raw.DeliveryID]; ok {
		if sameRaw(existing.RawMessage, raw) {
			return IngestResult{Pending: true, PendingID: raw.DeliveryID}, nil
		}
		return IngestResult{}, fmt.Errorf("%w: pending delivery %s reused", ErrSequenceConflict, raw.DeliveryID)
	}
	if existing, ok := s.conflictingRaw(raw); ok {
		if sameRaw(existing.RawMessage, raw) {
			return IngestResult{Pending: true, PendingID: raw.DeliveryID}, nil
		}
		return IngestResult{}, fmt.Errorf("%w: %s %s sequence %d", ErrSequenceConflict, raw.IdentityID, raw.BootID, raw.Sequence)
	}
	storedRaw := StoredRawMessage{RawMessage: raw}
	key := sequenceKey(raw.IdentityID, raw.BootID)
	progress, exists := s.sequences[key]
	if !exists {
		progress = SequenceProgress{IdentityID: raw.IdentityID, BootID: raw.BootID, Start: raw.Sequence, Last: raw.Sequence - 1}
	}
	var progressUpdate *SequenceProgress
	if !exists || raw.Sequence > progress.Last {
		progress.Last = raw.Sequence
		progressUpdate = &progress
	}
	record := JournalRecord{Kind: "pending", Raw: &storedRaw, PendingID: raw.DeliveryID, Sequence: progressUpdate}
	if err := s.append(record); err != nil {
		return IngestResult{}, err
	}
	s.apply(record)
	return IngestResult{Pending: true, PendingID: raw.DeliveryID}, nil
}

func (s *Store) PendingMessages() []StoredRawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	messages := make([]StoredRawMessage, 0, len(s.pending))
	for _, raw := range s.pending {
		m := raw
		m.Payload = append([]byte(nil), raw.Payload...)
		messages = append(messages, m)
	}
	return messages
}

func (s *Store) ResolvePending(pendingID string, observation model.Observation) (IngestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pending[pendingID]
	if !ok {
		if duplicateOf, resolved := s.resolved[pendingID]; resolved {
			return IngestResult{Duplicate: true, DuplicateOf: duplicateOf}, nil
		}
		return IngestResult{}, fmt.Errorf("%w: %s", ErrPendingNotFound, pendingID)
	}
	obKey := observationKey(observation.IdentityID, observation.ID)
	if _, duplicate := s.seenObs[obKey]; duplicate {
		record := JournalRecord{Kind: "resolved", PendingID: pendingID, ResolvedDuplicateOf: obKey}
		if err := s.append(record); err != nil {
			return IngestResult{}, err
		}
		s.apply(record)
		return IngestResult{Duplicate: true, DuplicateOf: obKey}, nil
	}
	record := JournalRecord{Kind: "observation", PendingID: pendingID, Observation: &observation}
	if err := s.append(record); err != nil {
		return IngestResult{}, err
	}
	s.apply(record)
	return IngestResult{Observation: &observation}, nil
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
	seekMode := io.SeekStart
	if validBytes < len(data) {
		seekMode = io.SeekEnd
	}
	seekOffset := int64(validBytes)
	if seekMode == io.SeekEnd {
		seekOffset = 0
	}
	if _, err := s.f.Seek(seekOffset, seekMode); err != nil {
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
	case "ingest", "pending":
		if record.Raw != nil {
			key := sequenceKey(record.Raw.IdentityID, record.Raw.BootID)
			if s.delivered[key] == nil {
				s.delivered[key] = make(map[int64]struct{})
			}
			s.delivered[key][record.Raw.Sequence] = struct{}{}
			if s.rawBySeq[key] == nil {
				s.rawBySeq[key] = make(map[int64]StoredRawMessage)
			}
			s.rawBySeq[key][record.Raw.Sequence] = *record.Raw
		}
		if record.Kind == "pending" && record.PendingID != "" {
			s.pending[record.PendingID] = *record.Raw
		}
		if record.Raw != nil && record.Raw.DuplicateOf != "" {
			s.seenObs[record.Raw.DuplicateOf] = struct{}{}
		}
		if record.Observation != nil {
			s.seenObs[observationKey(record.Observation.IdentityID, record.Observation.ID)] = struct{}{}
		}
		if record.Sequence != nil {
			s.sequences[sequenceKey(record.Sequence.IdentityID, record.Sequence.BootID)] = *record.Sequence
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
		if record.Observation != nil {
			s.seenObs[observationKey(record.Observation.IdentityID, record.Observation.ID)] = struct{}{}
		}
		if record.PendingID != "" {
			delete(s.pending, record.PendingID)
		}
	case "resolved":
		if record.PendingID != "" {
			s.resolved[record.PendingID] = record.ResolvedDuplicateOf
			delete(s.pending, record.PendingID)
		}
	case "sequence":
		if record.Sequence != nil {
			s.sequences[sequenceKey(record.Sequence.IdentityID, record.Sequence.BootID)] = *record.Sequence
		}
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
	return identityID + "\x00" + observationID
}

func (s *Store) conflictingRaw(raw model.RawMessage) (StoredRawMessage, bool) {
	bySeq := s.rawBySeq[sequenceKey(raw.IdentityID, raw.BootID)]
	existing, ok := bySeq[raw.Sequence]
	return existing, ok
}

func sameRaw(a, b model.RawMessage) bool {
	return a.IdentityID == b.IdentityID &&
		a.HardwareID == b.HardwareID &&
		a.BootID == b.BootID &&
		a.Sequence == b.Sequence &&
		a.ContentType == b.ContentType &&
		bytes.Equal(a.Payload, b.Payload)
}

func sequenceKey(identityID, bootID string) string {
	return identityID + "\x00" + bootID
}
