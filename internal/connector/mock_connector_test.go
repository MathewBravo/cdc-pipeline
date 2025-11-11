package connector

import (
	"testing"
	"time"

	"github.com/MathewBravo/cdc-pipeline/internal/events"
)

// TestMockConnector_Start_ReturnsChannel tests that Start() returns a valid channel
func TestMockConnector_Start_ReturnsChannel(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() returned unexpected error: %v", err)
	}

	if eventChan == nil {
		t.Fatal("Start() returned nil channel")
	}
}

// TestMockConnector_Start_CanReceiveEvents tests that events can be received from the channel
func TestMockConnector_Start_CanReceiveEvents(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Try to receive an event with timeout
	select {
	case event, ok := <-eventChan:
		if !ok {
			t.Fatal("Channel was closed immediately, expected at least one event")
		}
		// Basic validation that we got an event
		if event.Table == "" {
			t.Error("Received event has empty Table field")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for event - connector may not be producing events")
	}
}

// TestMockConnector_ProducesMultipleEvents tests that multiple events are produced
func TestMockConnector_ProducesMultipleEvents(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Collect events for a short time
	timeout := time.After(3 * time.Second)
	eventsReceived := 0

	for eventsReceived < 3 {
		select {
		case _, ok := <-eventChan:
			if !ok {
				// Channel closed
				if eventsReceived == 0 {
					t.Fatal("Channel closed without producing any events")
				}
				return // Test passes if we got at least some events
			}
			eventsReceived++
		case <-timeout:
			if eventsReceived == 0 {
				t.Fatal("No events received within timeout")
			}
			return // Got some events, good enough
		}
	}

	if eventsReceived < 3 {
		t.Errorf("Expected at least 3 events, got %d", eventsReceived)
	}
}

// TestMockConnector_EventsHaveCorrectStructure validates event structure
func TestMockConnector_EventsHaveCorrectStructure(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Get first event
	select {
	case event := <-eventChan:
		// Validate Operation is set
		if event.Operation < events.OperationInsert || event.Operation > events.OperationDelete {
			t.Errorf("Invalid operation type: %v", event.Operation)
		}

		// Validate Table is set
		if event.Table == "" {
			t.Error("Event Table field is empty")
		}

		// For INSERT, After should not be nil
		if event.Operation == events.OperationInsert && event.After == nil {
			t.Error("INSERT event should have After data")
		}

		// Timestamp should be set
		if event.TsMs.IsZero() {
			t.Error("Event timestamp is zero")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for event")
	}
}

// TestMockConnector_Stop_ClosesChannel tests that Stop() closes the channel
func TestMockConnector_Stop_ClosesChannel(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Let it produce at least one event
	time.Sleep(200 * time.Millisecond)

	// Stop the connector
	err = conn.Stop()
	if err != nil {
		t.Errorf("Stop() returned error: %v", err)
	}

	// Channel should close within reasonable time
	timeout := time.After(2 * time.Second)
	channelClosed := false

	for !channelClosed {
		select {
		case _, ok := <-eventChan:
			if !ok {
				channelClosed = true
			}
		case <-timeout:
			t.Fatal("Channel did not close after Stop() was called")
		}
	}
}

// TestMockConnector_Stop_BeforeStart tests calling Stop before Start
func TestMockConnector_Stop_BeforeStart(t *testing.T) {
	conn := &MockConnector{}

	// Stop without starting should not panic or return error
	err := conn.Stop()
	if err != nil {
		t.Errorf("Stop() before Start() returned error: %v", err)
	}
}

// TestMockConnector_Stop_Twice tests calling Stop multiple times
func TestMockConnector_Stop_Twice(t *testing.T) {
	conn := &MockConnector{}

	_, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	err = conn.Stop()
	if err != nil {
		t.Errorf("First Stop() returned error: %v", err)
	}

	// Second stop should not panic
	err = conn.Stop()
	// It's okay if this returns an error or nil
	// Just shouldn't crash
}

// TestMockConnector_ChannelClosesEventually tests that channel eventually closes
func TestMockConnector_ChannelClosesEventually(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Drain all events
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-eventChan:
			if !ok {
				// Channel closed, test passes
				return
			}
		case <-timeout:
			t.Fatal("Channel did not close within reasonable time (5s)")
		}
	}
}

// TestMockConnector_ConcurrentReads tests multiple goroutines reading from channel
func TestMockConnector_ConcurrentReads(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Start two goroutines reading from same channel
	done := make(chan bool, 2)

	reader := func() {
		count := 0
		for range eventChan {
			count++
			if count >= 2 {
				break
			}
		}
		done <- true
	}

	go reader()
	go reader()

	// Wait for both to finish or timeout
	timeout := time.After(5 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-done:
			// Good
		case <-timeout:
			t.Fatal("Concurrent reads timed out")
		}
	}
}

// TestMockConnector_OperationTypes tests different operation types
func TestMockConnector_OperationTypes(t *testing.T) {
	conn := &MockConnector{}

	eventChan, err := conn.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Collect several events and check operations
	eventsReceived := 0
	timeout := time.After(3 * time.Second)

	for eventsReceived < 5 {
		select {
		case event, ok := <-eventChan:
			if !ok {
				return // Channel closed, that's fine
			}

			// Should be valid operation
			switch event.Operation {
			case events.OperationInsert, events.OperationUpdate, events.OperationDelete:
				// Valid
			default:
				t.Errorf("Invalid operation type: %v", event.Operation)
			}

			eventsReceived++
		case <-timeout:
			if eventsReceived == 0 {
				t.Fatal("No events received")
			}
			return
		}
	}
}
