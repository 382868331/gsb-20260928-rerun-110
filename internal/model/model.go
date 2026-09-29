package model

import "time"

type Identity struct {
	HardwareID string
	ClaimedID  string
	Kind       string
}

type RawMessage struct {
	MessageID  string            `json:"message_id"`
	HardwareID string            `json:"hardware_id"`
	ClaimedID  string            `json:"claimed_id"`
	BootID     string            `json:"boot_id"`
	Sequence   int64             `json:"sequence"`
	Source     string            `json:"source"`
	Payload    []byte            `json:"payload"`
	ReceivedAt time.Time         `json:"received_at"`
	Headers    map[string]string `json:"headers,omitempty"`
}

type Observation struct {
	ID         string    `json:"id"`
	HardwareID string    `json:"hardware_id"`
	DeviceID   string    `json:"device_id"`
	Metric     string    `json:"metric"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
	MessageID  string    `json:"message_id"`
}

type Envelope struct {
	Message RawMessage
	Reading Observation
}

func (m RawMessage) TransportKey() string {
	return m.HardwareID + ":" + m.BootID + ":" + sequenceKey(m.Sequence)
}

func sequenceKey(sequence int64) string {
	if sequence < 0 {
		return "n" + formatUint(-sequence)
	}
	return "p" + formatUint(sequence)
}

func formatUint(value int64) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
