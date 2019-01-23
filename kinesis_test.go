// Copyright 2019, Omnition
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kinesis

import (
	"reflect"
	"testing"
	"time"

	gen "github.com/jaegertracing/jaeger/model"
	"go.opencensus.io/trace"
)

// TODO(jbd): Test export.

func Test_spanDataToJaegerPB(t *testing.T) {
	answerValue := int64(42)
	keyValue := "value"
	resultValue := true
	statusCodeValue := int64(2)
	statusMessage := "error"
	now := time.Now()

	e := Exporter{}

	tests := []struct {
		name string
		data *trace.SpanData
		want *gen.Span
	}{
		{
			name: "no parent",
			data: &trace.SpanData{
				SpanContext: trace.SpanContext{
					TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
				},
				Name:      "/foo",
				StartTime: now,
				EndTime:   now,
				Attributes: map[string]interface{}{
					"key": keyValue,
				},
				Annotations: []trace.Annotation{
					{
						Time:    now,
						Message: statusMessage,
						Attributes: map[string]interface{}{
							"answer": answerValue,
						},
					},
					{
						Time:    now,
						Message: statusMessage,
						Attributes: map[string]interface{}{
							"result": resultValue,
						},
					},
				},
				Status: trace.Status{Code: trace.StatusCodeUnknown, Message: "error"},
			},
			want: &gen.Span{
				TraceID:       traceIDMapper(trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
				SpanID:        spanIDMapper(trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}),
				OperationName: "/foo",
				StartTime:     now,
				Duration:      time.Duration(0),
				Tags: []gen.KeyValue{
					{Key: "key", VType: gen.ValueType_STRING, VStr: keyValue},
					{Key: "status.code", VType: gen.ValueType_INT64, VInt64: statusCodeValue},
					{Key: "status.message", VType: gen.ValueType_STRING, VStr: statusMessage},
				},
				Logs: []gen.Log{
					{Timestamp: now, Fields: []gen.KeyValue{
						{Key: "answer", VType: gen.ValueType_INT64, VInt64: answerValue},
						{Key: "message", VType: gen.ValueType_STRING, VStr: statusMessage},
					}},
					{Timestamp: now, Fields: []gen.KeyValue{
						{Key: "result", VType: gen.ValueType_BOOL, VBool: resultValue},
						{Key: "message", VType: gen.ValueType_STRING, VStr: statusMessage},
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.spanDataToJaegerPB(tt.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("spanDataToThrift() = %v, want %v", got, tt.want)
			}
		})
	}
}
