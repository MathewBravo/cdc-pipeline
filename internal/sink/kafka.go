package sink

import "github.com/twmb/franz-go/pkg/kgo"

type KafkaSink struct {
	client *kgo.Client
}
