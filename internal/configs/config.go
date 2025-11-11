package configs

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

type Config struct {
	Source SourceConfig `yaml:"source"`
	CDC    CDCConfig    `yaml:"cdc"`
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

func (c *Config) PrettyPrint() {
	fmt.Println("Config:")
	fmt.Println("source: ")
	fmt.Printf("\thost: %s\n", c.Source.Host)
	fmt.Printf("\tport: %d\n", c.Source.Port)
	fmt.Printf("\tdatabase: %s\n", c.Source.Database)
	fmt.Printf("\tuser: %s\n", c.Source.User)
	fmt.Printf("\tslot_name: %s\n", c.Source.SlotName)
	fmt.Printf("\tpublication_name: %s\n", c.Source.PublicationName)
	fmt.Printf("\tssl_mode: %s\n", c.Source.SSLMode)
	fmt.Printf("\tpassword: %s\n", c.Source.Password)

	fmt.Println("cdc:")
	fmt.Printf("\tstarting_lsn: %s\n", c.CDC.StartingLSN)
	fmt.Printf("\theartbeat_interval: %s\n", c.CDC.HeartbeatInterval)
}
