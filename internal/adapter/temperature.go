package adapter

import (
	"encoding/json"
	"fmt"
	"time"

	"edge-ingest/internal/model"
)

type TemperaturePayload struct {
	Serial   string  `json:"serial"`
	DeviceID string  `json:"device_id"`
	Reading  float64 `json:"reading"`
	When     string  `json:"when"`
}

type TemperatureAdapter struct{}

func (TemperatureAdapter) Name() string { return "temperature" }

func (TemperatureAdapter) Adapt(device model.Identity, bootID string, sequence int64, payload []byte, receivedAt string) (model.Envelope, error) {
	var body TemperaturePayload
	if err := json.Unmarshal(payload, &body); err != nil {
		return model.Envelope{}, err
	}
	if body.Serial == "" || body.When == "" {
		return model.Envelope{}, fmt.Errorf("temperature message missing serial or timestamp")
	}
	observedAt, err := time.Parse(time.RFC3339Nano, body.When)
	if err != nil {
		return model.Envelope{}, err
	}
	received, err := time.Parse(time.RFC3339Nano, receivedAt)
	if err != nil {
		return model.Envelope{}, err
	}
	messageID := fmt.Sprintf("temp:%s:%s:%020d", body.Serial, bootID, sequence)
	raw := model.RawMessage{
		MessageID:  messageID,
		HardwareID: body.Serial,
		ClaimedID:  body.DeviceID,
		BootID:     bootID,
		Sequence:   sequence,
		Source:     "temperature/v1",
		Payload:    append([]byte(nil), payload...),
		ReceivedAt: received.UTC(),
	}
	reading := model.Observation{
		ID:         fmt.Sprintf("temperature:%s:%s", body.Serial, body.When),
		HardwareID: body.Serial,
		DeviceID:   body.DeviceID,
		Metric:     "temperature",
		Value:      body.Reading,
		Unit:       "C",
		ObservedAt: observedAt.UTC(),
		ReceivedAt: received.UTC(),
		MessageID:  messageID,
	}
	return model.Envelope{Message: raw, Reading: reading}, nil
}
