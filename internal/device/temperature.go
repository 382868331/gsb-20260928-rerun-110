package device

import (
	"encoding/json"
	"fmt"
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/model"
)

const TemperatureType = "temperature_v1"

const TemperatureV2ContentType = "application/json; format=temperature_v2"

type TemperaturePayload struct {
	ObservationID string  `json:"observation_id"`
	ObservedAt    string  `json:"observed_at"`
	Celsius       float64 `json:"temp_c"`
	HumidityPct   float64 `json:"humidity_pct"`
}

type TemperatureV2Payload struct {
	ObservationID string  `json:"observation_id"`
	ObservedAt    string  `json:"observed_at"`
	Fahrenheit    float64 `json:"temp_f"`
	HumidityPct   float64 `json:"humidity_pct"`
}

type TemperatureV1Adapter struct{}

type TemperatureV2Adapter struct{}

type TemperatureAdapter = TemperatureV2Adapter

func (TemperatureV1Adapter) Type() string { return TemperatureType }
func (TemperatureV2Adapter) Type() string { return TemperatureType }

func (TemperatureV1Adapter) Decode(msg model.RawMessage) (model.Observation, error) {
	if msg.ContentType != "" && msg.ContentType != "application/json" {
		return model.Observation{}, fmt.Errorf("%w: %s", adapter.ErrUnsupportedFormat, msg.ContentType)
	}
	var p TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature: %w", err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, p.ObservedAt)
	if err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature time: %w", err)
	}
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       TemperatureType,
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures: map[string]float64{
			"temperature_c": p.Celsius,
			"humidity_pct":  p.HumidityPct,
		},
		Attributes: map[string]any{
			"format_version":   "temperature_v1",
			"temperature_unit": "C",
		},
	}, nil
}

func (TemperatureV2Adapter) Decode(msg model.RawMessage) (model.Observation, error) {
	switch msg.ContentType {
	case "", "application/json":
		return TemperatureV1Adapter{}.Decode(msg)
	case TemperatureV2ContentType:
	default:
		return model.Observation{}, fmt.Errorf("%w: %s", adapter.ErrUnsupportedFormat, msg.ContentType)
	}

	var p TemperatureV2Payload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature v2: %w", err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, p.ObservedAt)
	if err != nil {
		return model.Observation{}, fmt.Errorf("decode temperature v2 time: %w", err)
	}
	celsius := (p.Fahrenheit - 32) * 5 / 9
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       TemperatureType,
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures: map[string]float64{
			"temperature_c": celsius,
			"humidity_pct":  p.HumidityPct,
		},
		Attributes: map[string]any{
			"format_version":   "temperature_v2",
			"temperature_unit": "F",
		},
	}, nil
}

type Temperature struct {
	identityID string
	hardwareID string
	bootID     string
	nextSeq    int64
}

func NewTemperature(identityID, hardwareID, bootID string, startSeq int64) *Temperature {
	return &Temperature{identityID: identityID, hardwareID: hardwareID, bootID: bootID, nextSeq: startSeq}
}

func (d *Temperature) IdentityID() string { return d.identityID }
func (d *Temperature) HardwareID() string { return d.hardwareID }
func (d *Temperature) BootID() string     { return d.bootID }

func (d *Temperature) Reboot(bootID string, startSeq int64) {
	d.bootID = bootID
	d.nextSeq = startSeq
}

func (d *Temperature) ReplaceIdentity(identityID string) {
	d.identityID = identityID
}

func (d *Temperature) Reading(observedAt time.Time, celsius, humidity float64) model.RawDelivery {
	p := TemperaturePayload{
		ObservationID: fmt.Sprintf("%s-%d", d.bootID, d.nextSeq),
		ObservedAt:    observedAt.UTC().Format(time.RFC3339Nano),
		Celsius:       celsius,
		HumidityPct:   humidity,
	}
	b, _ := json.Marshal(p)
	delivery := model.RawDelivery{
		BootID:      d.bootID,
		Sequence:    d.nextSeq,
		ContentType: "application/json",
		Payload:     b,
	}
	d.nextSeq++
	return delivery
}

func (d *Temperature) ReadingV2(observedAt time.Time, fahrenheit, humidity float64) model.RawDelivery {
	p := TemperatureV2Payload{
		ObservationID: fmt.Sprintf("%s-%d", d.bootID, d.nextSeq),
		ObservedAt:    observedAt.UTC().Format(time.RFC3339Nano),
		Fahrenheit:    fahrenheit,
		HumidityPct:   humidity,
	}
	b, _ := json.Marshal(p)
	delivery := model.RawDelivery{
		BootID:      d.bootID,
		Sequence:    d.nextSeq,
		ContentType: TemperatureV2ContentType,
		Payload:     b,
	}
	d.nextSeq++
	return delivery
}
