package adapter

import (
	"fmt"

	"edge-ingest/internal/model"
)

type Adapter interface {
	Name() string
	Adapt(device model.Identity, bootID string, sequence int64, payload []byte, receivedAt string) (model.Envelope, error)
}

func Registry() map[string]Adapter {
	return map[string]Adapter{
		"temperature": TemperatureAdapter{},
		"vibration":   VibrationAdapter{},
	}
}

func Resolve(kind string) (Adapter, error) {
	adapter := Registry()[kind]
	if adapter == nil {
		return nil, fmt.Errorf("unknown device kind %q", kind)
	}
	return adapter, nil
}
