package connector

import (
	"fmt"

	"github.com/MathewBravo/cdc-pipeline/internal/events"
	"github.com/jackc/pglogrepl"
)

type MessageMapper struct {
	relationCache map[uint32]*pglogrepl.RelationMessage
}

func (m *MessageMapper) mapToChangeEvent(walMessage pglogrepl.Message) events.ChangeEvent {
	switch walMessage.Type() {
	case pglogrepl.MessageTypeInsert:
		insertMsg, ok := walMessage.(*pglogrepl.InsertMessage)
		if !ok {
			// shouldn't happen
			return events.ChangeEvent{}
		}
		return handleInsert(insertMsg)
	case pglogrepl.MessageTypeDelete:
		deleteMsg, ok := walMessage.(*pglogrepl.DeleteMessage)
		if !ok {
			// shouldn't happen
			return events.ChangeEvent{}
		}
		return handleDelete(deleteMsg)
	case pglogrepl.MessageTypeUpdate:
		updateMsg, ok := walMessage.(*pglogrepl.UpdateMessage)
		if !ok {
			// shouldn't happen
			return events.ChangeEvent{}
		}
		return handleUpdate(updateMsg)
	default:
		fmt.Println("SHOULD NOT BE REACHED")
		return events.ChangeEvent{}
	}
}

func handleInsert(insertMsg *pglogrepl.InsertMessage) events.ChangeEvent {
	fmt.Println("HANDLE INSERT REACHED")
	return events.ChangeEvent{}
}

func handleDelete(deleteMsg *pglogrepl.DeleteMessage) events.ChangeEvent {
	fmt.Println("HANDLE DELETE REACHED")
	return events.ChangeEvent{}
}

func handleUpdate(updateMsg *pglogrepl.UpdateMessage) events.ChangeEvent {
	fmt.Println("HANDLE UPDATE REACHED")
	return events.ChangeEvent{
		Operation: events.OperationUpdate,
	}
}
