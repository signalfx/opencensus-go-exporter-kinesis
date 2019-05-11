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
	options := Options{
		Name:               "test",
		StreamName:         "in_dev_test_tenant",
		AWSKinesisEndpoint: "http://0.0.0.0:4567",
		AWSRegion:          "us-west-2",
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
