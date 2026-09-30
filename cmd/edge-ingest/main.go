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
	defer s.Close()
	adapters := hub.DefaultAdapters()
	h := hub.New(s, adapters)
	now := time.Now().UTC()
	if err := h.Provision("demo-temperature", "hw-temperature", device.TemperatureType, now); err != nil {
		return err
	}
	if err := h.Provision("demo-vibration", "hw-vibration", device.VibrationType, now); err != nil {
		return err
	}
	tempDevice := device.NewTemperature("demo-temperature", "hw-temperature", "demo-boot", 1)
	vibDevice := device.NewVibration("demo-vibration", "hw-vibration", "demo-boot", 1)
	messages := []struct {
		identity string
		delivery model.RawDelivery
	}{
		{"demo-temperature", tempDevice.Reading(now.Add(-2*time.Second), 21.4, 43.2)},
		{"demo-vibration", vibDevice.Reading(now.Add(-time.Second), 0.1, -0.2, 1.01, 1.03)},
	}
	acks := make([]hub.Ack, 0, len(messages)+1)
	for _, message := range messages {
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
	duplicate, err := h.Receive("demo-temperature", messages[0].delivery)
	if err != nil {
		return err
	}
	acks = append(acks, duplicate)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{"acks": acks, "resume": resume})
}
