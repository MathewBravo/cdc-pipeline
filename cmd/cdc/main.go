package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/connector"
	i "github.com/MathewBravo/cdc-pipeline/internal/init"
	"github.com/MathewBravo/cdc-pipeline/internal/pipeline"
)

func main() {
	i.Init()

	cfg, err := configs.Load("./data/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Println("Config loaded successfully")

	conn := connector.NewPGConnector(cfg.Source)

	eventCh, err := conn.Start()
	if err != nil {
		log.Fatalf("Failed to start connector: %v", err)
	}

	p := pipeline.NewPipeline(&cfg.Pipeline)
	outputCh := p.Start(eventCh)

	fmt.Println("Connector started, listening for events...")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for event := range outputCh {
			fmt.Printf("Received event: %+v\n", event)
		}
	}()

	<-sigCh
	fmt.Println("\nShutting down...")

	if err := conn.Stop(); err != nil {
		log.Printf("Error stopping connector: %v", err)
	}

	fmt.Println("Shutdown complete")
}
