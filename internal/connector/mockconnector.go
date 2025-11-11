package connector

import "github.com/MathewBravo/cdc-pipeline/internal/events"

type MockConnector struct {
	eventChan chan events.ChangeEvent
	stopChan  chan struct{}
}

func (m *MockConnector) Start() (<-chan events.ChangeEvent, error) {
	m.eventChan = make(chan events.ChangeEvent)
	m.stopChan = make(chan struct{})

	return m.eventChan, nil
}

func (m *MockConnector) Stop() error {
	return nil
}
