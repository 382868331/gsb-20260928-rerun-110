package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"edge-ingest/internal/model"
)

const (
	recordPrepare = 1
	recordCommit  = 2
)

type journalRecord struct {
	Type     int             `json:"type"`
	Txn      string          `json:"txn"`
	Envelope *model.Envelope `json:"envelope,omitempty"`
}

type Result struct {
	Stored    bool
	Duplicate bool
}

type IdentityBinding struct {
	HardwareID string
	DeviceID   string
	ChangedAt  time.Time
}

type Store struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	raw      map[string]model.RawMessage
	messages map[string]string
	obs      map[string]model.Observation
	bindings map[string]IdentityBinding
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "ingest.log")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{
		path:     path,
		file:     file,
		raw:      make(map[string]model.RawMessage),
		messages: make(map[string]string),
		obs:      make(map[string]model.Observation),
		bindings: make(map[string]IdentityBinding),
	}
	if err := s.recover(); err != nil {
		file.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

func (s *Store) Ingest(envelope model.Envelope) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	message := envelope.Message
	if message.HardwareID == "" || message.BootID == "" || message.MessageID == "" {
		return Result{}, errors.New("incomplete device message identity")
	}
	if envelope.Reading.HardwareID != message.HardwareID {
		return Result{}, errors.New("observation hardware identity does not match raw message")
	}
	transportKey := message.TransportKey()
	if _, ok := s.raw[transportKey]; ok {
		return Result{Duplicate: true}, nil
	}
	if _, ok := s.messages[message.MessageID]; ok {
		return Result{Duplicate: true}, nil
	}
	if _, ok := s.obs[envelope.Reading.ID]; ok {
		return Result{Duplicate: true}, nil
	}

	txn := transactionID(envelope)
	if err := s.writeRecord(journalRecord{Type: recordPrepare, Txn: txn, Envelope: &envelope}); err != nil {
		return Result{}, err
	}
	if err := s.writeRecord(journalRecord{Type: recordCommit, Txn: txn}); err != nil {
		return Result{}, err
	}
	s.apply(envelope)
	return Result{Stored: true}, nil
}

func (s *Store) RawMessages() []model.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]model.RawMessage, 0, len(s.raw))
	for _, message := range s.raw {
		result = append(result, message)
	}
	return result
}

func (s *Store) Observations() []model.Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]model.Observation, 0, len(s.obs))
	for _, observation := range s.obs {
		result = append(result, observation)
	}
	return result
}

func (s *Store) CurrentBinding(hardwareID string) (IdentityBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	binding, ok := s.bindings[hardwareID]
	return binding, ok
}

func (s *Store) recover() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	prepared := make(map[string]model.Envelope)
	committed := make(map[string]bool)
	durableOffset := int64(0)
	for {
		offset, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		record, n, err := readRecord(s.file)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			if truncateErr := s.file.Truncate(offset); truncateErr != nil {
				return truncateErr
			}
			break
		}
		switch record.Type {
		case recordPrepare:
			if record.Envelope == nil {
				if err := s.file.Truncate(offset); err != nil {
					return err
				}
				return nil
			}
			prepared[record.Txn] = *record.Envelope
		case recordCommit:
			envelope, ok := prepared[record.Txn]
			if !ok || committed[record.Txn] {
				if err := s.file.Truncate(offset); err != nil {
					return err
				}
				return nil
			}
			committed[record.Txn] = true
			s.apply(envelope)
			durableOffset = offset + n
		}
	}
	if err := s.file.Truncate(durableOffset); err != nil {
		return err
	}
	if _, err := s.file.Seek(durableOffset, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (s *Store) apply(envelope model.Envelope) {
	message := envelope.Message
	key := message.TransportKey()
	if _, exists := s.raw[key]; !exists {
		s.raw[key] = message
		s.messages[message.MessageID] = key
		s.obs[envelope.Reading.ID] = envelope.Reading
	}
	binding := IdentityBinding{
		HardwareID: message.HardwareID,
		DeviceID:   message.ClaimedID,
		ChangedAt:  message.ReceivedAt,
	}
	current, exists := s.bindings[message.HardwareID]
	if !exists || !binding.ChangedAt.Before(current.ChangedAt) {
		s.bindings[message.HardwareID] = binding
	}
}

func (s *Store) writeRecord(record journalRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	frame := make([]byte, 8+len(payload)+4)
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	binary.BigEndian.PutUint32(frame[8+len(payload):], crc32.ChecksumIEEE(frame[:8+len(payload)]))
	if _, err := s.file.Write(frame); err != nil {
		return err
	}
	return s.file.Sync()
}

func readRecord(reader io.Reader) (journalRecord, int64, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(reader, header); err != nil {
		return journalRecord{}, 0, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length > 64*1024*1024 {
		return journalRecord{}, 0, fmt.Errorf("record too large: %d", length)
	}
	checksum := binary.BigEndian.Uint32(header[4:8])
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return journalRecord{}, 0, err
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return journalRecord{}, 0, errors.New("payload checksum mismatch")
	}
	trailer := make([]byte, 4)
	if _, err := io.ReadFull(reader, trailer); err != nil {
		return journalRecord{}, 0, err
	}
	frameChecksum := binary.BigEndian.Uint32(trailer)
	frame := append(append([]byte{}, header...), payload...)
	if crc32.ChecksumIEEE(frame) != frameChecksum {
		return journalRecord{}, 0, errors.New("frame checksum mismatch")
	}
	var record journalRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return journalRecord{}, 0, err
	}
	return record, int64(8 + length + 4), nil
}

func transactionID(envelope model.Envelope) string {
	message := envelope.Message
	hash := sha256.Sum256([]byte(message.MessageID + "\x00" + message.TransportKey()))
	return fmt.Sprintf("%x", hash[:16])
}
