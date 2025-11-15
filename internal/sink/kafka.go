package sink

// TODO: Tomorrow's tasks for Kafka sink implementation
//
// 1. Parse compression from config string → kgo.CompressionCodec
//    - Map "snappy", "gzip", "lz4", "zstd", "none" to kgo types
//    - Handle invalid compression values
//
// 2. Use config.BatchSize and config.FlushInterval
//    - Convert to kgo.ProducerBatchMaxBytes() and kgo.ProducerLinger()
//    - Handle zero/nil values with sensible defaults
//
// 3. Implement Start() method:
//    - Start goroutine with for-range loop over eventCh
//    - For each event:
//      a. Serialize event to JSON (json.Marshal)
//      b. Extract primary key for partitioning (how to identify PK field?)
//      c. Create kgo.Record with Topic, Key, Value
//      d. Produce to Kafka (sync or async?)
//    - Handle context for shutdown
//    - Close Kafka client on exit
//
// 4. Error handling:
//    - Handle NewClient errors properly (don't return nil)
//    - Handle produce errors and retries
//    - Logging for failed messages
//
// 5. Primary key extraction strategy:
//    - Look for "id" field in After/Before maps?
//    - Use LSN as fallback?
//    - Make it configurable?
//
// 6. Add Stop() method for graceful shutdown

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kgo.SeedBrokers("10.255.255.254:9092")
type KafkaSink struct {
	client   *kgo.Client
	config   *configs.SinkConfig
	stopChan chan struct{}
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
		client: cl,
		config: cfg,
	}, nil
}

func (k *KafkaSink) Start(eventCh <-chan events.ChangeEvent) error {
	go func() {
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
				err = k.produceRecord(record)
				if err != nil {
					fmt.Printf("ERROR: Failed to produce record: %v\n", err)
					continue
				}
			case <-k.stopChan:
				k.client.Close()
				return
			}
		}
	}()
	return nil
}

func (k *KafkaSink) Stop() {
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

func (k *KafkaSink) produceRecord(record *kgo.Record) error {
	return k.client.ProduceSync(context.Background(), record).FirstErr()
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
