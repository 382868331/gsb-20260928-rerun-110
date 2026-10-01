package device

import (
	"encoding/json"
	"fmt"
	"time"

	"edge-ingest/internal/model"
)

const (
	TemperatureType   = "temperature_v1"
	TemperatureV2Type = "temperature_v2"
)

type TemperaturePayload struct {
	ObservationID string  `json:"observation_id"`
	ObservedAt    string  `json:"observed_at"`
	Celsius       float64 `json:"temp_c"`
	HumidityPct   float64 `json:"humidity_pct"`
}

type TemperatureAdapter struct{}

func (TemperatureAdapter) Type() string { return TemperatureType }

func (TemperatureAdapter) Decode(msg model.RawMessage) (model.Observation, error) {
	var p TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature: %w", err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, p.ObservedAt)
	if err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature time: %w", err)
	}
	return model.Observation{
		ID:            p.ObservationID,
		IdentityID:    msg.IdentityID,
		HardwareID:    msg.HardwareID,
		Type:          TemperatureType,
		ObservedAt:    observedAt,
		ReceivedAt:    msg.ReceivedAt,
		InterpretedAt: msg.ReceivedAt,
		RuleID:        TemperatureType,
		Explanation:   "temperature decoded as degrees Celsius",
		Measures: map[string]float64{
			"temperature_c": p.Celsius,
			"humidity_pct":  p.HumidityPct,
		},
		Units: map[string]string{"temperature_c": "C", "humidity_pct": "percent"},
	}, nil
}

type TemperatureV2Payload struct {
	ObservationID string  `json:"observation_id"`
	ObservedAt    string  `json:"observed_at"`
	Fahrenheit    float64 `json:"temp_f"`
	HumidityPct   float64 `json:"humidity_pct"`
}

type TemperatureV2Adapter struct{}

func (TemperatureV2Adapter) Type() string { return TemperatureV2Type }

func (TemperatureV2Adapter) Decode(msg model.RawMessage) (model.Observation, error) {
	var p TemperatureV2Payload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature v2: %w", err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, p.ObservedAt)
	if err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature v2 time: %w", err)
	}
	return model.Observation{
		ID:            p.ObservationID,
		IdentityID:    msg.IdentityID,
		HardwareID:    msg.HardwareID,
		Type:          TemperatureV2Type,
		ObservedAt:    observedAt,
		ReceivedAt:    msg.ReceivedAt,
		InterpretedAt: msg.ReceivedAt,
		RuleID:        TemperatureV2Type,
		Explanation:   "temperature converted from Fahrenheit to degrees Celsius",
		Measures: map[string]float64{
			"temperature_c": (p.Fahrenheit - 32) * 5 / 9,
			"humidity_pct":  p.HumidityPct,
		},
		Units: map[string]string{"temperature_c": "C", "humidity_pct": "percent"},
	}, nil
}

type Temperature struct {
	identityID string
	hardwareID string
	bootID     string
	nextSeq    int64
	format     string
}

func NewTemperature(identityID, hardwareID, bootID string, startSeq int64) *Temperature {
	return &Temperature{identityID: identityID, hardwareID: hardwareID, bootID: bootID, nextSeq: startSeq, format: TemperatureType}
}

func NewTemperatureV2(identityID, hardwareID, bootID string, startSeq int64) *Temperature {
	d := NewTemperature(identityID, hardwareID, bootID, startSeq)
	d.format = TemperatureV2Type
	return d
}

func (d *Temperature) IdentityID() string { return d.identityID }
func (d *Temperature) HardwareID() string { return d.hardwareID }
func (d *Temperature) BootID() string     { return d.bootID }

func (d *Temperature) Reboot(bootID string, startSeq int64) {
	d.bootID = bootID
	d.nextSeq = startSeq
}

func (d *Temperature) SetFormat(format string) {
	d.format = format
}

func (d *Temperature) ReplaceIdentity(identityID string) {
	d.identityID = identityID
}

func (d *Temperature) Reading(observedAt time.Time, celsius, humidity float64) model.RawDelivery {
	format := d.format
	if format == "" {
		format = TemperatureType
	}
	id := fmt.Sprintf("%s-%d", d.bootID, d.nextSeq)
	observedAtText := observedAt.UTC().Format(time.RFC3339Nano)
	var payload []byte
	switch format {
	case TemperatureV2Type:
		payload, _ = json.Marshal(TemperatureV2Payload{
			ObservationID: id,
			ObservedAt:    observedAtText,
			Fahrenheit:    celsius*9/5 + 32,
			HumidityPct:   humidity,
		})
	default:
		payload, _ = json.Marshal(TemperaturePayload{
			ObservationID: id,
			ObservedAt:    observedAtText,
			Celsius:       celsius,
			HumidityPct:   humidity,
		})
	}
	delivery := model.RawDelivery{
		BootID:      d.bootID,
		Sequence:    d.nextSeq,
		Format:      format,
		ContentType: "application/json",
		Payload:     payload,
	}
	d.nextSeq++
	return delivery
}
