package device

import (
	"encoding/json"
	"fmt"
	"time"

	"edge-ingest/internal/model"
)

const TemperatureType = "temperature_v1"

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
