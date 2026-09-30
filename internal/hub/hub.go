package hub

import (
	"errors"
	"fmt"
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
	}
	result.Remaining = h.store.PendingCount()
	return result, nil
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
