package kinesis

import (
	"time"

	gf "github.com/brianvoe/gofakeit"
	"github.com/jaegertracing/jaeger/model"
)

func generateSpans() []*model.Span {
	spans := []*model.Span{}
	for i := 0; i <= 1000; i++ {
		spans = append(spans, &model.Span{
			SpanID:  model.SpanID(gf.Int64()),
			TraceID: model.TraceID{Low: gf.Uint64(), High: gf.Uint64()},
			// StartTime: gf.Date(),
			Duration:      time.Duration(gf.Second()),
			OperationName: gf.Word(),
			Process:       model.NewProcess(gf.Word(), []model.KeyValue{}),
			Tags:          []model.KeyValue{},
		})
	}
	return spans
}
