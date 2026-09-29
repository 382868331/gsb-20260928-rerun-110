package link

import (
	"sort"
	"time"

	"edge-ingest/internal/device"
	"edge-ingest/internal/ingest"
)

type Frame struct {
	Kind       string
	BootID     string
	Sequence   int64
	Payload    []byte
	ReceivedAt time.Time
}

type pendingKey struct {
	bootID   string
	sequence int64
}

type Link struct {
	service        *ingest.Service
	Connected      bool
	pending        map[string]map[pendingKey]Frame
	StoredCount    int
	DuplicateCount int
}

func New(service *ingest.Service) *Link {
	return &Link{service: service, Connected: true, pending: make(map[string]map[pendingKey]Frame)}
}

func (l *Link) Send(sender *device.VirtualDevice, payload []byte, receivedAt time.Time) error {
	frame := Frame{
		Kind:       sender.Kind,
		BootID:     sender.BootID,
		Sequence:   sender.Sequence,
		Payload:    append([]byte(nil), payload...),
		ReceivedAt: receivedAt,
	}
	return l.sendFrame(sender.HardwareID, frame)
}

func (l *Link) Reconnect(receivedAt time.Time) (int, error) {
	l.Connected = true
	return l.Flush(receivedAt)
}

func (l *Link) Flush(receivedAt time.Time) (int, error) {
	flushed := 0
	for hardwareID := range l.pending {
		keys := make([]pendingKey, 0, len(l.pending[hardwareID]))
		for key := range l.pending[hardwareID] {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].bootID != keys[j].bootID {
				return keys[i].bootID < keys[j].bootID
			}
			return keys[i].sequence < keys[j].sequence
		})
		for _, key := range keys {
			frame := l.pending[hardwareID][key]
			if err := l.deliver(hardwareID, frame); err != nil {
				return flushed, err
			}
			delete(l.pending[hardwareID], key)
			flushed++
		}
	}
	return flushed, nil
}

func (l *Link) PendingCount() int {
	total := 0
	for _, frames := range l.pending {
		total += len(frames)
	}
	return total
}

func (l *Link) sendFrame(hardwareID string, frame Frame) error {
	if l.Connected {
		return l.deliver(hardwareID, frame)
	}
	if l.pending[hardwareID] == nil {
		l.pending[hardwareID] = make(map[pendingKey]Frame)
	}
	l.pending[hardwareID][pendingKey{frame.BootID, frame.Sequence}] = frame
	return nil
}

func (l *Link) deliver(hardwareID string, frame Frame) error {
	result, _, err := l.service.Accept(frame.Kind, frame.BootID, frame.Sequence, frame.Payload, frame.ReceivedAt)
	if err != nil {
		if l.pending[hardwareID] == nil {
			l.pending[hardwareID] = make(map[pendingKey]Frame)
		}
		l.pending[hardwareID][pendingKey{frame.BootID, frame.Sequence}] = frame
		return err
	}
	if result.Stored {
		l.StoredCount++
	}
	if result.Duplicate {
		l.DuplicateCount++
	}
	delete(l.pending[hardwareID], pendingKey{frame.BootID, frame.Sequence})
	return nil
}
