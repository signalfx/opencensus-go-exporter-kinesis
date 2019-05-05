package kinesis

import (
	"time"

	gf "github.com/brianvoe/gofakeit"
	"github.com/jaegertracing/jaeger/model"
)

const (
	fakeIP uint32 = 1<<24 | 2<<16 | 3<<8 | 4

	fakeSpanDuration = 123 * time.Microsecond
)

func generateSpans() []*model.Span {
	spans := []*model.Span{}
	for i := 0; i <= 1000; i++ {
		spans = append(spans, &model.Span{
			SpanID:  model.SpanID(gf.Int64()),
			TraceID: model.TraceID{gf.Uint64(), gf.Uint64()},
			// StartTime: gf.Date(),
			Duration:      time.Duration(gf.Second()),
			OperationName: gf.Word(),
			Process:       model.NewProcess(gf.Word(), []model.KeyValue{}),
			Tags:          []model.KeyValue{},
		})
	}
	return spans
}
