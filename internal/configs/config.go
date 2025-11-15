package configs

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

type Config struct {
	Source   SourceConfig   `yaml:"source"`
	CDC      CDCConfig      `yaml:"cdc"`
	Pipeline PipelineConfig `yaml:"pipeline"`
}

type SourceConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Database        string `yaml:"database"`
	User            string `yaml:"user"`
	SlotName        string `yaml:"slot_name"`
	PublicationName string `yaml:"publication_name"`
	SSLMode         string `yaml:"ssl_mode"`
	Password        string `yaml:"-"`
}

type CDCConfig struct {
	StartingLSN       string `yaml:"starting_lsn"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`
}

type PipelineConfig struct {
	Tables         map[string]TableOptions `yaml:"tables"`
	DefaultRoute   string                  `yaml:"default_route"`
	ExcludedTables []string                `yaml:"excluded_tables"`
}

type TableOptions struct {
	Operations []string  `yaml:"operations"`
	PIIMasks   []PIIMask `yaml:"pii_masks"`
	Route      string    `yaml:"route_to"`
}

type PIIMask struct {
	Field  string `yaml:"field"`
	Action string `yaml:"action"`
}

func Load(path string) (*Config, error) {
	fmt.Println("READING CONFIG: ", path)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	cfg.Source.Password = os.Getenv("PG_PASSWORD")
	if cfg.Source.Password == "" {
		return nil, fmt.Errorf("CONFIG ERR: Empty PG_PASSWORD env variable")
	}

	if cfg.CDC.HeartbeatInterval == "" {
		cfg.CDC.HeartbeatInterval = "10s"
	}
	if cfg.Source.SSLMode == "" {
		cfg.Source.SSLMode = "disable"
	}

	return &cfg, nil
}

func verifyConfig(config CDCConfig) error {
	return nil
}
