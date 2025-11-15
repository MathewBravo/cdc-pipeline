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
	"github.com/MathewBravo/cdc-pipeline/internal/sink"
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
	fmt.Println("Connector started")

	p := pipeline.NewPipeline(&cfg.Pipeline)
	outputCh := p.Start(eventCh)
	fmt.Println("Pipeline started")

	s, err := sink.NewKafkaSink(&cfg.Sink)
	if err != nil {
		log.Fatalf("Failed to create kafka sink: %v", err)
	}
	err = s.Start(outputCh)
	if err != nil {
		log.Fatalf("Failed to start kafka sink: %v", err)
	}
	fmt.Println("Kafka sink started")

	fmt.Println("\nCDC Pipeline running. Press Ctrl+C to stop.")
	fmt.Println("Check Kafka UI at http://localhost:8080 to see messages")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown in reverse order
	fmt.Println("\nShutting down...")

	fmt.Println("Stopping Kafka sink...")
	s.Stop()

	fmt.Println("Stopping connector...")
	if err := conn.Stop(); err != nil {
		log.Printf("Error stopping connector: %v", err)
	}

	fmt.Println("Shutdown complete")
}
