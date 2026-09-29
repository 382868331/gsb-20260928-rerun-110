package ingest

import (
	"time"

	"edge-ingest/internal/adapter"
	"edge-ingest/internal/model"
	"edge-ingest/internal/store"
)

type Service struct {
	store *store.Store
}

func NewService(storage *store.Store) *Service {
	return &Service{store: storage}
}

func (s *Service) Accept(kind, bootID string, sequence int64, payload []byte, receivedAt time.Time) (store.Result, model.Envelope, error) {
	protocol, err := adapter.Resolve(kind)
	if err != nil {
		return store.Result{}, model.Envelope{}, err
	}
	envelope, err := protocol.Adapt(model.Identity{Kind: kind}, bootID, sequence, payload, receivedAt.Format(time.RFC3339Nano))
	if err != nil {
		return store.Result{}, model.Envelope{}, err
	}
	result, err := s.store.Ingest(envelope)
	return result, envelope, err
}
