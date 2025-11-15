package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/events"
)

type Pipeline struct {
	config   *configs.PipelineConfig
	outputCh chan events.ChangeEvent
}

func NewPipeline(cfg *configs.PipelineConfig) *Pipeline {
	return &Pipeline{
		config:   cfg,
		outputCh: make(chan events.ChangeEvent, 100),
	}
}

func (p *Pipeline) Start(eventCh <-chan events.ChangeEvent) <-chan events.ChangeEvent {
	go p.processLoop(eventCh)
	return p.outputCh
}

func (p *Pipeline) processLoop(eventCh <-chan events.ChangeEvent) {
	for event := range eventCh {
		if p.isExcluded(event) {
			continue
		}
		if !p.isOperationAllowed(event) {
			continue
		}
		event = p.applyPIIMasks(event)
		event = p.determineRoute(event)
		p.outputCh <- event
	}
	close(p.outputCh)
}

func (p *Pipeline) isExcluded(event events.ChangeEvent) bool {
	return slices.Contains(p.config.ExcludedTables, event.Table)
}

func (p *Pipeline) isOperationAllowed(event events.ChangeEvent) bool {
	tableOptions, exists := p.config.Tables[event.Table]
	if !exists {
		return true
	}
	return slices.Contains(tableOptions.Operations, event.Operation.ToString())
}

func (p *Pipeline) applyPIIMasks(event events.ChangeEvent) events.ChangeEvent {
	tableOptions, exists := p.config.Tables[event.Table]
	if !exists {
		return event
	}
	for _, mask := range tableOptions.PIIMasks {
		switch mask.Action {
		case "redact":
			if event.Before != nil {
				event.Before[mask.Field] = "REDACTED"
			}
			if event.After != nil {
				event.After[mask.Field] = "REDACTED"
			}
		case "hash":
			if event.Before != nil {
				fld := event.Before[mask.Field]
				fldStr := fmt.Sprintf("%v", fld)
				hash := sha256.Sum256([]byte(fldStr))
				event.Before[mask.Field] = hex.EncodeToString(hash[:])
			}
			if event.After != nil {
				fld := event.After[mask.Field]
				fldStr := fmt.Sprintf("%v", fld)
				hash := sha256.Sum256([]byte(fldStr))
				event.After[mask.Field] = hex.EncodeToString(hash[:])
			}
		case "mask_partial":
			if event.Before != nil {
				if fld, ok := event.Before[mask.Field].(string); ok {
					event.Before[mask.Field] = maskPartial(fld)
				}
			}
			if event.After != nil {
				if fld, ok := event.After[mask.Field].(string); ok {
					event.After[mask.Field] = maskPartial(fld)
				}
			}
		}
	}

	return event
}

func maskPartial(s string) string {
	n := len(s)
	if n <= 4 {
		return strings.Repeat("*", n)
	}
	return strings.Repeat("*", n-4) + s[n-4:]
}

func (p *Pipeline) determineRoute(event events.ChangeEvent) events.ChangeEvent {
	tableOptions, exists := p.config.Tables[event.Table]
	if !exists {
		event.Route = p.config.DefaultRoute
		return event
	}
	route := tableOptions.Route
	event.Route = route
	return event
}
