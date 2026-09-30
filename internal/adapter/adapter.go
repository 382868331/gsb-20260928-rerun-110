package adapter

import (
	"errors"

	"edge-ingest/internal/model"
)

var ErrUnknownDeviceType = errors.New("unknown device type")

type Adapter interface {
	Type() string
	Decode(msg model.RawMessage) (model.Observation, error)
}

type Registry struct {
	adapters map[string]Adapter
}

func NewRegistry(adapters ...Adapter) *Registry {
	r := &Registry{adapters: make(map[string]Adapter)}
	for _, a := range adapters {
		r.adapters[a.Type()] = a
	}
	return r
}

func (r *Registry) Register(a Adapter) {
	r.adapters[a.Type()] = a
}

func (r *Registry) Get(deviceType string) (Adapter, error) {
	a, ok := r.adapters[deviceType]
	if !ok {
		return nil, ErrUnknownDeviceType
	}
	return a, nil
}
