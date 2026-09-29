package hub

import (
	"fmt"
	"sync"
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/device"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

type Ack struct {
	IdentityID string `json:"identity_id"`
	BootID     string `json:"boot_id"`
	Sequence   int64  `json:"sequence"`
	Duplicate  bool   `json:"duplicate"`
	Stored     bool   `json:"stored"`
}

type ResumeState struct {
	BootID       string  `json:"boot_id"`
	NextSequence int64   `json:"next_sequence"`
	HasProgress  bool    `json:"has_progress"`
	Missing      []int64 `json:"missing,omitempty"`
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

func (h *Hub) Provision(id, hardwareID, deviceType string, now time.Time) error {
	a, err := h.adapters.Get(deviceType)
	if err != nil {
		return err
	}
	_ = a
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
	if !ok {
		return ResumeState{BootID: bootID}, nil
	}
	return ResumeState{
		BootID:       bootID,
		NextSequence: progress.Last + 1,
		HasProgress:  true,
		Missing:      h.store.Missing(identityID, bootID),
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
	codec, err := h.adapters.Get(identity.Type)
	if err != nil {
		return Ack{}, err
	}
	receivedAt := h.now().UTC()
	raw := model.RawMessage{
		DeliveryID:  fmt.Sprintf("%s:%s:%d", identityID, delivery.BootID, delivery.Sequence),
		IdentityID:  identityID,
		HardwareID:  identity.HardwareID,
		BootID:      delivery.BootID,
		Sequence:    delivery.Sequence,
		ContentType: delivery.ContentType,
		Payload:     append([]byte(nil), delivery.Payload...),
		ReceivedAt:  receivedAt,
	}
	observation, err := codec.Decode(raw)
	if err != nil {
		return Ack{}, err
	}
	result, err := h.store.Ingest(raw, &observation)
	if err != nil {
		return Ack{}, err
	}
	return Ack{
		IdentityID: identityID,
		BootID:     delivery.BootID,
		Sequence:   delivery.Sequence,
		Duplicate:  result.Duplicate,
		Stored:     true,
	}, nil
}

func DefaultAdapters() *adapter.Registry {
	return adapter.NewRegistry(
		device.TemperatureAdapter{},
		device.VibrationAdapter{},
	)
}
