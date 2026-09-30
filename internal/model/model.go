package model

import "time"

type DeviceIdentity struct {
	ID         string    `json:"id"`
	HardwareID string    `json:"hardware_id"`
	Type       string    `json:"type"`
	Active     bool      `json:"active"`
	ReplacedBy string    `json:"replaced_by,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type RawMessage struct {
	DeliveryID  string    `json:"delivery_id"`
	IdentityID  string    `json:"identity_id"`
	HardwareID  string    `json:"hardware_id"`
	BootID      string    `json:"boot_id"`
	Sequence    int64     `json:"sequence"`
	Format      string    `json:"format,omitempty"`
	ContentType string    `json:"content_type"`
	Payload     []byte    `json:"payload"`
	ReceivedAt  time.Time `json:"received_at"`
}

type Observation struct {
	ID         string             `json:"id"`
	IdentityID string             `json:"identity_id"`
	HardwareID string             `json:"hardware_id"`
	Type       string             `json:"type"`
	ObservedAt time.Time          `json:"observed_at"`
	ReceivedAt time.Time          `json:"received_at"`
	Measures   map[string]float64 `json:"measures"`
	Attributes map[string]any     `json:"attributes,omitempty"`
}

type RawDelivery struct {
	BootID      string
	Sequence    int64
	Format      string
	ContentType string
	Payload     []byte
}

type RawReference struct {
	DeliveryID  string `json:"delivery_id"`
	IdentityID  string `json:"identity_id"`
	BootID      string `json:"boot_id"`
	Sequence    int64  `json:"sequence"`
	Format      string `json:"format,omitempty"`
	ContentType string `json:"content_type"`
	PayloadSHA  string `json:"payload_sha256"`
}
