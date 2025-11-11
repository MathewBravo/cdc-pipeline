package connector

import "github.com/MathewBravo/cdc-pipeline/internal/events"

type Connector interface {
	Start() (<-chan events.ChangeEvent, error)
	Stop() error
}
