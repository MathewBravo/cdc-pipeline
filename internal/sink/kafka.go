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
	"fmt"

	"github.com/MathewBravo/cdc-pipeline/internal/configs"
	"github.com/MathewBravo/cdc-pipeline/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kgo.SeedBrokers("10.255.255.254:9092")
type KafkaSink struct {
	client *kgo.Client
	config *configs.SinkConfig
}

func NewKafkaSink(cfg *configs.SinkConfig) *KafkaSink {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerBatchMaxBytes(1000000),
	)
	if err != nil {
		return nil
	}

	return &KafkaSink{
		client: cl,
		config: cfg,
	}
}

func (k *KafkaSink) Start(eventCh <-chan events.ChangeEvent) error {
	return fmt.Errorf("Hello")
}
