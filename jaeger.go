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

// Package jaeger contains an OpenCensus tracing exporter for AWS Kinesis.
package kinesis // import "github.com/omnition/opencensus-go-exporter-kinesis"

import (
	//"bytes"
	// "encoding/binary"
	"errors"
	"fmt"
	//"io"
	//"io/ioutil"
	"log"
	//"net/http"
	"time"

	producer "github.com/a8m/kinesis-producer"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"go.opencensus.io/trace"
	"google.golang.org/api/support/bundler"

	gen "github.com/omnition/opencensus-go-exporter-kinesis/internal/gen-go/jaeger"
)

const defaultServiceName = "OpenCensus"

// Options are the options to be used when initializing a Jaeger exporter.
type Options struct {
	// CollectorEndpoint is the full url to the Jaeger HTTP Thrift collector.
	// For example, http://localhost:14268/api/traces

	StreamName              string
	AWSRegion               string
	AWSRole                 string
	AWSKinesisEndpoint      string
	KPLBatchSize            int
	KPLBatchCount           int
	KPLBacklogCount         int
	KPLFlushIntervalSeconds int

	// Encoding defines the format in which spans should be exporter to kinesis
	// only Jaeger is supported right now
	Encoding string

	//BufferMaxCount defines the total number of traces that can be buffered in memory
	BufferMaxCount int

	// OnError is the hook to be called when there is
	// an error occurred when uploading the stats data.
	// If no custom hook is set, errors are logged.
	// Optional.
	OnError func(err error)

	// ServiceName is the Jaeger service name.
	// Deprecated: Specify Process instead.
	ServiceName string

	// Process contains the information about the exporting process.
	Process Process
}

// NewExporter returns a trace.Exporter implementation that exports
// the collected spans to Jaeger.
func NewExporter(o Options) (*Exporter, error) {
	if o.AWSRegion == "" {
		return nil, errors.New("missing AWS Region for Kinesis exporter")
	}

	if o.StreamName == "" {
		return nil, errors.New("missing Stream Name for Kinesis exporter")
	}

	if o.Encoding != "jaeger" {
		return nil, errors.New("invalid option for Encoding. Valid choices are: jaeger")
	}

	// TODO: Set defaults here

	onError := func(err error) {
		if o.OnError != nil {
			o.OnError(err)
			return
		}
		log.Printf("Error puttings spans to Kinesis: %v", err)
	}

	service := o.Process.ServiceName
	if service == "" && o.ServiceName != "" {
		// fallback to old service name if specified
		service = o.ServiceName
	} else if service == "" {
		service = defaultServiceName
	}
	// tags := make([]*gen.Tag, len(o.Process.Tags))
	tags := make([]*gen.KeyValue, len(o.Process.Tags))
	for i, tag := range o.Process.Tags {
		tags[i] = attributeToKeyValue(tag.key, tag.value)
	}

	// create KPL producer

	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion(o.AWSRegion)))
	cfgs := []*aws.Config{}
	if o.AWSRole != "" {
		cfgs = append(cfgs, &aws.Config{Credentials: stscreds.NewCredentials(sess, o.AWSRole)})
	}
	if o.AWSKinesisEndpoint != "" {
		cfgs = append(cfgs, &aws.Config{Endpoint: aws.String(o.AWSKinesisEndpoint)})
	}
	client := kinesis.New(sess, cfgs...)

	pr := producer.New(&producer.Config{
		StreamName:    o.StreamName,
		BatchSize:     o.KPLBatchSize,
		BatchCount:    o.KPLBatchCount,
		BacklogCount:  o.KPLBacklogCount,
		FlushInterval: time.Second * time.Duration(o.KPLFlushIntervalSeconds),
		Client:        client,
		Verbose:       false,
	})

	// end creating KPL producer

	e := &Exporter{
		process: &gen.Process{
			ServiceName: service,
			Tags:        tags,
		},
		producer: pr,
	}
	// TODO: hook onError with KPL lib

	bundler := bundler.NewBundler((*gen.Span)(nil), func(bundle interface{}) {
		if err := e.upload(bundle.([]*gen.Span)); err != nil {
			onError(err)
		}
	})

	// Set BufferedByteLimit with the total number of spans that are permissible to be held in memory.
	// This needs to be done since the size of messages is always set to 1. Failing to set this would allow
	// 1G messages to be held in memory since that is the default value of BufferedByteLimit.
	if o.BufferMaxCount != 0 {
		bundler.BufferedByteLimit = o.BufferMaxCount
	}

	e.bundler = bundler

	return e, nil
}

// Process contains the information exported to jaeger about the source
// of the trace data.
type Process struct {
	// ServiceName is the Jaeger service name.
	ServiceName string

	// Tags are added to Jaeger Process exports
	Tags []Tag
}

// Tag defines a key-value pair
// It is limited to the possible conversions to *jaeger.Tag by attributeToKeyValue
type Tag struct {
	key   string
	value interface{}
}

// BoolTag creates a new tag of type bool, exported as jaeger.TagType_BOOL
func BoolTag(key string, value bool) Tag {
	return Tag{key, value}
}

// StringTag creates a new tag of type string, exported as jaeger.TagType_STRING
func StringTag(key string, value string) Tag {
	return Tag{key, value}
}

// Int64Tag creates a new tag of type int64, exported as jaeger.TagType_LONG
func Int64Tag(key string, value int64) Tag {
	return Tag{key, value}
}

// Exporter is an implementation of trace.Exporter that uploads spans to Jaeger.
type Exporter struct {
	process  *gen.Process
	producer *producer.Producer

	bundler *bundler.Bundler
}

var _ trace.Exporter = (*Exporter)(nil)

// Flush waits for exported trace spans to be uploaded.
//
// This is useful if your program is ending and you do not want to lose recent spans.
func (e *Exporter) Flush() {
	// e.bundler.Flush()
}

func (e *Exporter) upload(spans []*gen.Span) error {
	/*
		batch := &gen.Batch{
			Spans:   spans,
			Process: e.process,
		}
	*/

	// TODO: handler errors
	// TODO: metrics
	errors := []error{}
	for _, span := range spans {
		encoded, err := proto.Marshal(span)
		if err != nil {
			// error callback
			errors = append(errors, err)
			continue
		}
		err = e.producer.Put(encoded, traceIdToString(span.TraceId))
		if err != nil {
			errors = append(errors, err)
			continue
		}
	}
	if len(errors) > 0 {
		// TODO: better error
		return fmt.Errorf("batch processing failed")
	}
	return nil
}

// ExportSpan exports a SpanData to Jaeger.
func (e *Exporter) ExportSpan(data *trace.SpanData) {
	e.bundler.Add(e.spanDataToJaegerPB(data), 1)
	// TODO(jbd): Handle oversized bundlers.
}

func (e *Exporter) spanDataToJaegerPB(data *trace.SpanData) *gen.Span {
	tags := make([]*gen.KeyValue, 0, len(data.Attributes))
	for k, v := range data.Attributes {
		tag := attributeToKeyValue(k, v)
		if tag != nil {
			tags = append(tags, tag)
		}
	}

	tags = append(tags,
		attributeToKeyValue("status.code", data.Status.Code),
		attributeToKeyValue("status.message", data.Status.Message),
	)

	var logs []*gen.Log
	for _, a := range data.Annotations {
		fields := make([]*gen.KeyValue, 0, len(a.Attributes))
		for k, v := range a.Attributes {
			tag := attributeToKeyValue(k, v)
			if tag != nil {
				fields = append(fields, tag)
			}
		}
		fields = append(fields, attributeToKeyValue("message", a.Message))
		tsp, err := ptypes.TimestampProto(a.Time)
		if err != nil {
			// TODO: log error and fallback to some other value?
		}
		logs = append(logs, &gen.Log{
			Timestamp: tsp,
			Fields:    fields,
		})
	}

	var refs []*gen.SpanRef
	for _, link := range data.Links {
		ref := &gen.SpanRef{
			TraceId: link.TraceID[:],
			SpanId:  link.SpanID[:],
		}
		switch link.Type {
		case trace.LinkTypeChild:
			ref.RefType = gen.SpanRefType_CHILD_OF
		case trace.LinkTypeParent:
			ref.RefType = gen.SpanRefType_FOLLOWS_FROM
		}
		refs = append(refs, ref)
	}
	startTime, err := ptypes.TimestampProto(data.StartTime)
	if err != nil {
		// TODO: handle
	}

	// REVIEW: verify flags (traceoptions bit to denote IsSampled) translate well to jaeger
	// REVIEW: Is it correct attact process info like this here?
	return &gen.Span{
		TraceId:       data.TraceID[:],
		SpanId:        data.SpanID[:],
		OperationName: name(data),
		Flags:         uint32(data.TraceOptions),
		StartTime:     startTime,
		Duration:      ptypes.DurationProto(data.EndTime.Sub(data.StartTime)),
		Tags:          tags,
		Logs:          logs,
		References:    refs,
		Process:       e.process,
	}
}

func name(sd *trace.SpanData) string {
	n := sd.Name
	switch sd.SpanKind {
	case trace.SpanKindClient:
		n = "Sent." + n
	case trace.SpanKindServer:
		n = "Recv." + n
	}
	return n
}

func attributeToKeyValue(key string, a interface{}) *gen.KeyValue {
	var kv *gen.KeyValue
	switch value := a.(type) {
	case bool:
		kv = &gen.KeyValue{
			Key:   key,
			VBool: value,
			VType: gen.ValueType_BOOL,
		}
	case string:
		kv = &gen.KeyValue{
			Key:   key,
			VStr:  value,
			VType: gen.ValueType_STRING,
		}
	case int64:
		kv = &gen.KeyValue{
			Key:    key,
			VInt64: value,
			VType:  gen.ValueType_INT64,
		}
	case int32:
		v := int64(value)
		kv = &gen.KeyValue{
			Key:    key,
			VInt64: v,
			VType:  gen.ValueType_INT64,
		}
	case float64:
		kv = &gen.KeyValue{
			Key:      key,
			VFloat64: value,
			VType:    gen.ValueType_FLOAT64,
		}
	}
	return kv
}

func traceIdToString(traceId []byte) string {
	return fmt.Sprintf("%02x", traceId)
}
