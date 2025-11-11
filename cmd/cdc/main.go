package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/connector"
)

func main() {
	// 1. Load config
	cfg, err := configs.Load("./data/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Println("Config loaded successfully")

	// 2. Create connector
	conn := connector.NewPGConnector(cfg.Source)

	// 3. Start connector
	eventCh, err := conn.Start()
	if err != nil {
		log.Fatalf("Failed to start connector: %v", err)
	}

	fmt.Println("Connector started, listening for events...")

	// 4. Set up graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 5. Main loop - consume events or wait for shutdown
	go func() {
		for event := range eventCh {
			fmt.Printf("📦 Received event: %+v\n", event)
		}
	}()

	// Wait for shutdown signal
	<-sigCh
	fmt.Println("\n🛑 Shutting down...")

	// 6. Stop connector
	if err := conn.Stop(); err != nil {
		log.Printf("Error stopping connector: %v", err)
	}

	fmt.Println("Shutdown complete")
}
