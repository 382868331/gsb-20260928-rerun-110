package device

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	"edge-ingest/internal/model"
)

const VibrationType = "vibration_v1"

type VibrationAdapter struct{}

func (VibrationAdapter) Type() string { return VibrationType }

func (VibrationAdapter) Decode(msg model.RawMessage) (model.Observation, error) {
	rows, err := csv.NewReader(strings.NewReader(string(msg.Payload))).ReadAll()
	if err != nil || len(rows) != 1 || len(rows[0]) != 6 {
		return model.Observation{}, fmt.Errorf("decode vibration: want one CSV row with six fields")
	}
	row := rows[0]
	observedAt, err := time.Parse(time.RFC3339Nano, row[1])
	if err != nil {
		return model.Observation{}, fmt.Errorf("decode vibration time: %w", err)
	}
	values := make(map[string]float64, 3)
	names := []string{"axis_x_g", "axis_y_g", "axis_z_g"}
	for i, name := range names {
		v, err := strconv.ParseFloat(row[2+i], 64)
		if err != nil {
			return model.Observation{}, fmt.Errorf("decode vibration %s: %w", name, err)
		}
		values[name] = v
	}
	rms, err := strconv.ParseFloat(row[5], 64)
	if err != nil {
		return model.Observation{}, fmt.Errorf("decode vibration rms: %w", err)
	}
	values["rms_g"] = rms
	return model.Observation{
		ID:            row[0],
		IdentityID:    msg.IdentityID,
		HardwareID:    msg.HardwareID,
		Type:          VibrationType,
		ObservedAt:    observedAt,
		ReceivedAt:    msg.ReceivedAt,
		InterpretedAt: msg.ReceivedAt,
		RuleID:        VibrationType,
		Explanation:   "vibration decoded as acceleration and RMS in g",
		Measures:      values,
		Units: map[string]string{
			"axis_x_g": "g",
			"axis_y_g": "g",
			"axis_z_g": "g",
			"rms_g":    "g",
		},
	}, nil
}

type Vibration struct {
	identityID string
	hardwareID string
	bootID     string
	nextSeq    int64
}

func NewVibration(identityID, hardwareID, bootID string, startSeq int64) *Vibration {
	return &Vibration{identityID: identityID, hardwareID: hardwareID, bootID: bootID, nextSeq: startSeq}
}

func (d *Vibration) IdentityID() string { return d.identityID }
func (d *Vibration) HardwareID() string { return d.hardwareID }
func (d *Vibration) BootID() string     { return d.bootID }

func (d *Vibration) Reboot(bootID string, startSeq int64) {
	d.bootID = bootID
	d.nextSeq = startSeq
}

func (d *Vibration) Reading(observedAt time.Time, x, y, z, rms float64) model.RawDelivery {
	id := fmt.Sprintf("%s-%d", d.bootID, d.nextSeq)
	line := fmt.Sprintf("%s,%s,%f,%f,%f,%f", id, observedAt.UTC().Format(time.RFC3339Nano), x, y, z, rms)
	delivery := model.RawDelivery{
		BootID:      d.bootID,
		Sequence:    d.nextSeq,
		ContentType: "text/csv",
		Payload:     []byte(line),
	}
	d.nextSeq++
	return delivery
}
