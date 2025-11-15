package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kgo.SeedBrokers("10.255.255.254:9092 WSL WINDOWS IP")
type KafkaSink struct {
	client   *kgo.Client
	config   *configs.SinkConfig
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewKafkaSink(cfg *configs.SinkConfig) (*KafkaSink, error) {
	cmp := getCompression(cfg.Compression)
	batch := cfg.BatchSize * 1024

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerBatchCompression(cmp),
		kgo.ProducerBatchMaxBytes(int32(batch)),
		kgo.ProducerLinger(cfg.FlushInterval),
	)
	if err != nil {
		return nil, err
	}

	return &KafkaSink{
		client:   cl,
		config:   cfg,
		stopChan: make(chan struct{}),
	}, nil
}

func (k *KafkaSink) Start(eventCh <-chan events.ChangeEvent) error {
	k.wg.Go(func() {
		for {
			select {
			case event, ok := <-eventCh:
				if !ok {
					k.client.Close()
					return
				}
				record, err := k.handleEvent(event)
				if err != nil {
					fmt.Printf("ERROR: Failed to handle event: %v\n", err)
					continue
				}
				k.produceRecord(record)
			case <-k.stopChan:
				k.client.Close()
				return
			}
		}
	})
	return nil
}

func (k *KafkaSink) Stop() {
	close(k.stopChan)
	k.wg.Wait()
}

func (k *KafkaSink) handleEvent(event events.ChangeEvent) (*kgo.Record, error) {
	jsonEvent, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	pk := strings.Join(event.PK, "|")

	return &kgo.Record{
		Topic: event.Route,
		Key:   []byte(pk),
		Value: jsonEvent,
	}, nil
}

func (k *KafkaSink) produceRecord(record *kgo.Record) {
	k.client.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("ERROR: Produce failed: %v\n", err)
		}
	})
}

func getCompression(compression string) kgo.CompressionCodec {
	switch compression {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}
