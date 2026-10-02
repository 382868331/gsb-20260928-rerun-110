package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"edge-ingest/internal/device"
	"edge-ingest/internal/hub"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

func main() {
	dataDir := flag.String("data", filepath.Join("data", "edge"), "journal directory")
	flag.Parse()
	if err := run(*dataDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dataDir string) error {
	s, err := store.Open(dataDir)
	if err != nil {
		return err
	}
	h := hub.New(s, hub.DefaultAdapters())
	now := time.Now().UTC()
	clock := now
	h.SetClock(func() time.Time { return clock })
	if err := h.Provision("demo-temperature", "hw-temperature", device.TemperatureType, now); err != nil {
		return err
	}
	if err := h.Provision("demo-vibration", "hw-vibration", device.VibrationType, now); err != nil {
		return err
	}
	if err := h.Provision("demo-future", "hw-future", "future-temperature", now); err != nil {
		return err
	}
	tempDevice := device.NewTemperature("demo-temperature", "hw-temperature", "demo-boot", 1)
	vibDevice := device.NewVibration("demo-vibration", "hw-vibration", "demo-boot", 1)
	messages := []struct {
		identity string
		delivery model.RawDelivery
	}{
		{"demo-temperature", tempDevice.Reading(now.Add(-3*time.Second), 21.4, 43.2)},
		{"demo-temperature", tempDevice.Reading(now.Add(-2*time.Second), 21.6, 43.0)},
		{"demo-vibration", vibDevice.Reading(now.Add(-time.Second), 0.1, -0.2, 1.01, 1.03)},
	}
	acks := make([]hub.Ack, 0, len(messages)+2)
	for i, message := range messages {
		clock = now.Add(time.Duration(i) * time.Second)
		ack, err := h.Receive(message.identity, message.delivery)
		if err != nil {
			return err
		}
		acks = append(acks, ack)
	}
	resume, err := h.Resume("demo-temperature", "demo-boot")
	if err != nil {
		return err
	}
	duplicate, err := h.Receive("demo-vibration", messages[2].delivery)
	if err != nil {
		return err
	}
	acks = append(acks, duplicate)

	// Unknown format: stored pending, explained after a simulated restart.
	futurePayload, _ := json.Marshal(map[string]any{
		"observation_id": "future-1",
		"observed_at":    now.Format(time.RFC3339Nano),
		"temp_c":         18.5,
		"humidity_pct":   55,
	})
	pending, err := h.Receive("demo-future", model.RawDelivery{
		BootID: "demo-boot", Sequence: 1, Format: "future-temperature",
		ContentType: "application/json", Payload: futurePayload,
	})
	if err != nil {
		return err
	}
	acks = append(acks, pending)
	if err := s.Close(); err != nil {
		return err
	}

	// Reopen: pending state, branches and adoptions are recovered.
	s2, err := store.Open(dataDir)
	if err != nil {
		return err
	}
	defer s2.Close()
	h = hub.New(s2, hub.DefaultAdapters())
	h.RegisterAdapter(demoFutureRule{})
	reprocess, err := h.ReprocessPending()
	if err != nil {
		return err
	}

	// Downstream consumers reference the published interpretations.
	first := "demo-temperature:demo-boot:1"
	second := "demo-temperature:demo-boot:2"
	if err := h.RegisterDownstream("dashboard", first, "", "live dashboard"); err != nil {
		return err
	}
	if err := h.RegisterDownstream("audit", second, "", "audit export"); err != nil {
		return err
	}

	// Operator alice opens a calibration correction branch and adopts it.
	alice, err := h.OpenCorrection(demoCorrectedRule{}, hub.ReinterpretFilter{IdentityID: "demo-temperature", To: now.Add(500 * time.Millisecond)}, "alice", "calibration offset +0.3")
	if err != nil {
		return err
	}
	// Operator bob concurrently prepares an imperial-units correction over
	// the same range before alice's adoption is finalized.
	bob, err := h.OpenCorrection(demoImperialRule{}, hub.ReinterpretFilter{IdentityID: "demo-temperature"}, "bob", "imperial units")
	if err != nil {
		return err
	}
	adopted, err := h.AdoptBranch(alice.ID, hub.AdoptOptions{Operator: "alice"})
	if err != nil {
		return err
	}

	// Bob's overlapping adoption conflicts: it is narrowed to the
	// non-conflicting part, then the abandoned remainder keeps its rationale.
	conflict, conflictErr := h.AdoptBranch(bob.ID, hub.AdoptOptions{Operator: "bob"})
	narrowed, err := h.AdoptBranch(bob.ID, hub.AdoptOptions{Operator: "bob", Narrow: true})
	if err != nil {
		return err
	}
	abandoned, err := h.AbandonBranch(bob.ID, "bob", "defer unit migration for overlapping range")
	if err != nil {
		return err
	}

	// Partial retraction: alice withdraws the correction for the reading
	// received before now+1.5s only; it reverts to the original value.
	retracted, err := h.RetractAdoption(adopted.AdoptionID, hub.ReinterpretFilter{To: now.Add(500 * time.Millisecond)}, "alice", "sensor was being replaced")
	if err != nil {
		return err
	}

	// Every current value traces to its raw message and adoption decision.
	current := h.CurrentValues(hub.ReinterpretFilter{})

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"acks":             acks,
		"resume":           resume,
		"reprocess":        reprocess,
		"adopted":          adopted,
		"conflict":         conflict,
		"conflict_error":   fmt.Sprint(conflictErr),
		"narrowed":         narrowed,
		"abandoned_branch": abandoned,
		"retracted":        retracted,
		"current_values":   current,
	})
}

// demoCorrectedRule fixes a calibration offset while keeping the unit.
type demoCorrectedRule struct{}

func (demoCorrectedRule) Type() string { return "temperature_v1_corrected" }

func (demoCorrectedRule) Decode(msg model.RawMessage) (model.Observation, error) {
	var p device.TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, err
	}
	observedAt, err := time.Parse(time.RFC3339Nano, p.ObservedAt)
	if err != nil {
		return model.Observation{}, err
	}
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       "temperature_v1_corrected",
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures: map[string]float64{
			"temperature_c": p.Celsius + 0.3,
			"humidity_pct":  p.HumidityPct,
		},
	}, nil
}

// demoImperialRule reinterprets the same payload in Fahrenheit.
type demoImperialRule struct{}

func (demoImperialRule) Type() string { return "temperature_v1_imperial" }

func (demoImperialRule) Decode(msg model.RawMessage) (model.Observation, error) {
	var p device.TemperaturePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return model.Observation{}, err
	}
	observedAt, err := time.Parse(time.RFC3339Nano, p.ObservedAt)
	if err != nil {
		return model.Observation{}, err
	}
	return model.Observation{
		ID:         p.ObservationID,
		IdentityID: msg.IdentityID,
		HardwareID: msg.HardwareID,
		Type:       "temperature_v1_imperial",
		ObservedAt: observedAt,
		ReceivedAt: msg.ReceivedAt,
		Measures: map[string]float64{
			"temperature_f": p.Celsius*9/5 + 32,
			"humidity_pct":  p.HumidityPct,
		},
	}, nil
}

// demoFutureRule explains the previously unknown future-temperature format.
type demoFutureRule struct{}

func (demoFutureRule) Type() string { return "future-temperature" }

func (demoFutureRule) Decode(msg model.RawMessage) (model.Observation, error) {
	return device.TemperatureAdapter{}.Decode(msg)
}
