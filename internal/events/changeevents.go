package events

import (
	"encoding/json"
	"fmt"
)

type Operation int

const (
	OperationUpdate Operation = iota
	OperationInsert
	OperationDelete
)

type ChangeEvent struct {
	Operation Operation
	NameSpace string
	Table     string
	Before    map[string]any
	After     map[string]any
	Lsn       string
	Route     string
	// !TODO: Include later, info about the system, commit time
	// TsMs      time.Time
	// TsNs      time.Time
	// TsUs      time.Time // time.Microsecond

	// version   string
	// connector string
	// name      string
}

func (o Operation) ToString() string {
	switch o {
	case OperationInsert:
		return "INSERT"
	case OperationUpdate:
		return "UPDATE"
	case OperationDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func (e *ChangeEvent) Pretty() string {
	b, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return fmt.Sprintf("error marshaling ChangeEvent: %v", err)
	}
	return string(b)
}
