package adapter

import (
	"encoding/json"
	"fmt"
	"time"

	"edge-ingest/internal/model"
)

type VibrationFrame struct {
	HW       string            `json:"hw"`
	Logon    string            `json:"logical_id"`
	SampleMS int64             `json:"sample_ms"`
	Measure  float64           `json:"mm_s"`
	Meta     map[string]string `json:"meta,omitempty"`
}

type VibrationAdapter struct{}

func (VibrationAdapter) Name() string { return "vibration" }

func (VibrationAdapter) Adapt(device model.Identity, bootID string, sequence int64, payload []byte, receivedAt string) (model.Envelope, error) {
	var frame VibrationFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		return model.Envelope{}, err
	}
	if frame.HW == "" || frame.SampleMS <= 0 {
		return model.Envelope{}, fmt.Errorf("vibration message missing hardware or timestamp")
	}
	received, err := time.Parse(time.RFC3339Nano, receivedAt)
	if err != nil {
		return model.Envelope{}, err
	}
	observedAt := time.UnixMilli(frame.SampleMS).UTC()
	messageID := fmt.Sprintf("vib:%s:%s:%020d", frame.HW, bootID, sequence)
	raw := model.RawMessage{
		MessageID:  messageID,
		HardwareID: frame.HW,
		ClaimedID:  frame.Logon,
		BootID:     bootID,
		Sequence:   sequence,
		Source:     "vibration/v1",
		Payload:    append([]byte(nil), payload...),
		ReceivedAt: received.UTC(),
		Headers:    frame.Meta,
	}
	reading := model.Observation{
		ID:         fmt.Sprintf("vibration:%s:%d", frame.HW, frame.SampleMS),
		HardwareID: frame.HW,
		DeviceID:   frame.Logon,
		Metric:     "vibration",
		Value:      frame.Measure,
		Unit:       "mm/s",
		ObservedAt: observedAt,
		ReceivedAt: received.UTC(),
		MessageID:  messageID,
	}
	return model.Envelope{Message: raw, Reading: reading}, nil
}
