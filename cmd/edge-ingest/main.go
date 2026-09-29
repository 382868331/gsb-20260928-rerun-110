package main

import (
	"log"
	"os"
	"time"

	"edge-ingest/internal/device"
	"edge-ingest/internal/ingest"
	"edge-ingest/internal/link"
	"edge-ingest/internal/store"
)

func main() {
	dataDir := os.Getenv("EDGE_DATA_DIR")
	if dataDir == "" {
		dataDir = "data/edge-ingest"
	}
	storage, err := store.Open(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer storage.Close()

	service := ingest.NewService(storage)
	connection := link.New(service)
	now := time.Now().UTC()

	temperature, err := device.New("temperature", "temp-demo-01", "zone-a")
	if err != nil {
		log.Fatal(err)
	}
	vibration, err := device.New("vibration", "vib-demo-01", "pump-1")
	if err != nil {
		log.Fatal(err)
	}

	onlineFrame, err := temperature.Frame(22.4, now.Add(-2*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	if err := connection.Send(temperature, onlineFrame, now.Add(-time.Second)); err != nil {
		log.Fatal(err)
	}

	connection.Connected = false
	firstBacklog, err := temperature.Frame(22.5, now.Add(time.Second))
	if err != nil {
		log.Fatal(err)
	}
	if err := connection.Send(temperature, firstBacklog, now.Add(3*time.Second)); err != nil {
		log.Fatal(err)
	}
	secondBacklog, err := vibration.Frame(1.8, now.Add(2*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	if err := connection.Send(vibration, secondBacklog, now.Add(4*time.Second)); err != nil {
		log.Fatal(err)
	}

	flushed, err := connection.Reconnect(now.Add(5 * time.Second))
	if err != nil {
		log.Fatal(err)
	}
	if err := temperature.Restart(); err != nil {
		log.Fatal(err)
	}
	restartedFrame, err := temperature.Frame(22.6, now.Add(6*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	if err := connection.Send(temperature, restartedFrame, now.Add(7*time.Second)); err != nil {
		log.Fatal(err)
	}

	log.Printf("backfilled=%d stored=%d duplicates=%d raw_messages=%d observations=%d pending=%d",
		flushed,
		connection.StoredCount,
		connection.DuplicateCount,
		len(storage.RawMessages()),
		len(storage.Observations()),
		connection.PendingCount(),
	)
}
