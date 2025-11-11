package events

import "time"

type Operation int

const (
	OperationUpdate Operation = iota
	OperationInsert
	OperationDelete
)

type ChangeEvent struct {
	Operation Operation
	Table     string
	Before    map[string]any
	After     map[string]any
	TxId      *int32
	Lsn       *int32
	TsMs      time.Time
	TsNs      time.Time
	TsUs      time.Time // time.Microsecond

	// !TODO: Include later, info about the system
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
