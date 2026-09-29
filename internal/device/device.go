package device

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type TemperatureSample struct {
	Serial   string  `json:"serial"`
	DeviceID string  `json:"device_id"`
	Reading  float64 `json:"reading"`
	When     string  `json:"when"`
}

type VibrationSample struct {
	HW       string            `json:"hw"`
	Logon    string            `json:"logical_id"`
	SampleMS int64             `json:"sample_ms"`
	Measure  float64           `json:"mm_s"`
	Meta     map[string]string `json:"meta,omitempty"`
}

type VirtualDevice struct {
	Kind       string
	HardwareID string
	DeviceID   string
	BootID     string
	Sequence   int64
}

func New(kind, hardwareID, deviceID string) (*VirtualDevice, error) {
	bootID, err := newBootID()
	if err != nil {
		return nil, err
	}
	if kind != "temperature" && kind != "vibration" {
		return nil, fmt.Errorf("unsupported virtual device kind %q", kind)
	}
	return &VirtualDevice{Kind: kind, HardwareID: hardwareID, DeviceID: deviceID, BootID: bootID}, nil
}

func (d *VirtualDevice) Restart() error {
	bootID, err := newBootID()
	if err != nil {
		return err
	}
	d.BootID = bootID
	d.Sequence = 0
	return nil
}

func (d *VirtualDevice) ReplaceIdentity(deviceID string) error {
	if err := d.Restart(); err != nil {
		return err
	}
	d.DeviceID = deviceID
	return nil
}

func (d *VirtualDevice) Frame(value float64, observedAt time.Time) ([]byte, error) {
	d.Sequence++
	switch d.Kind {
	case "temperature":
		return json.Marshal(TemperatureSample{
			Serial:   d.HardwareID,
			DeviceID: d.DeviceID,
			Reading:  value,
			When:     observedAt.Format(time.RFC3339Nano),
		})
	case "vibration":
		return json.Marshal(VibrationSample{
			HW:       d.HardwareID,
			Logon:    d.DeviceID,
			SampleMS: observedAt.UnixMilli(),
			Measure:  value,
		})
	default:
		return nil, fmt.Errorf("unsupported virtual device kind %q", d.Kind)
	}
}

func newBootID() (string, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
