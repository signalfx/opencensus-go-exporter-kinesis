package kinesis

import (
	"log"
	"os"
	"testing"

	"github.com/jaegertracing/jaeger/model"
	"go.uber.org/zap"
)

var exp *Exporter

var spans []*model.Span

func setup() error {
	options := &Options{
		Name:               "test",
		StreamName:         "in_dev_test_tenant",
		AWSKinesisEndpoint: "http://0.0.0.0:4567",
		AWSRegion:          "us-west-2",
		ListFlushInterval:  1,
		MaxListSize:        1,
		/*
			QueueSize               int
			NumWorkers              int
			MaxListSize             int
			ListFlushInterval       int
			KPLAggregateBatchCount  int
			KPLAggregateBatchSize   int
			KPLBatchSize            int
			KPLBatchCount           int
			KPLBacklogCount         int
			KPLFlushIntervalSeconds int
			KPLMaxConnections       int
			KPLMaxRetries           int
			KPLMaxBackoffSeconds    int
			MaxAllowedSizePerSpan   int
		*/
	}
	logger := zap.NewNop()
	e, err := NewExporter(options, logger)
	if err == nil {
		exp = e
	}
	spans = generateSpans()
	return err
}
func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		log.Fatal(err.Error())
	}
	code := m.Run()
	os.Exit(code)
}

func BenchmarkPuts(b *testing.B) {
	span := spans[0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exp.ExportSpan(span)
	}
}

/*

func BenchmarkCompress(b *testing.B) {
		pr := producer.New(&producer.Config{
			StreamName:          "in_dev_test_tenant",
			AggregateBatchSize:  1,
			AggregateBatchCount: 1,
			BatchSize:           1,
			BatchCount:          1,
			BacklogCount:        1,
			MaxConnections:      24,
			FlushInterval:       time.Second * time.Duration(1),
			MaxRetries:          20,
			MaxBackoffTime:      time.Second * time.Duration(10),
			Client:              client,
			Verbose:             false,
		}, hooks)
		producers = append(producers, &shardProducer{
			pr:            pr,
			shard:         shard,
			hooks:         newKinesisHooksNoop(o.Name, o.StreamName),
			maxSize:       uint64(o.MaxListSize),
			flushInterval: time.Duration(o.ListFlushInterval) * time.Second,
			partitionKey:  shard.startingHashKey.String(),
		})
}
*/
