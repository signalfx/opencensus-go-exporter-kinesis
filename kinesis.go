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
	"errors"
	"fmt"
	"time"

	producer "github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gogo/protobuf/proto"
	gen "github.com/jaegertracing/jaeger/model"
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

	// TODO: reconsider this. we probably don't need it here as queued collector would do in-mem batching
	//BufferMaxCount defines the total number of traces that can be buffered in memory
	BufferMaxCount int

	// OnError is the hook to be called when there is
	// an error occurred when uploading the stats data.
	// If no custom hook is set, errors are logged.
	// Optional.
	OnError func(err error)
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

	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion(o.AWSRegion)))
	cfgs := []*aws.Config{}
	if o.AWSRole != "" {
		cfgs = append(cfgs, &aws.Config{Credentials: stscreds.NewCredentials(sess, o.AWSRole)})
	}
	if o.AWSKinesisEndpoint != "" {
		cfgs = append(cfgs, &aws.Config{Endpoint: aws.String(o.AWSKinesisEndpoint)})
	}
	client := kinesis.New(sess, cfgs...)
	// Make sure stream exists and we can access it. This makes the collector crash
	// early when misconfigured.
	_, err := client.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(o.StreamName),
		Limit:      aws.Int64(1),
	})
	if err != nil {
		return nil, fmt.Errorf(
			"Kinesis stream %s not available: %s", o.StreamName, err.Error(),
		)
	}
	pr := producer.New(&producer.Config{
		StreamName:    o.StreamName,
		BatchSize:     o.KPLBatchSize,
		BatchCount:    o.KPLBatchCount,
		BacklogCount:  o.KPLBacklogCount,
		FlushInterval: time.Second * time.Duration(o.KPLFlushIntervalSeconds),
		Client:        client,
		Verbose:       false,
	})

	e := &Exporter{
		producer: pr,
		onError: func(err error) {
			if o.OnError != nil {
				o.OnError(err)
				return
			}
		},
	}

	e.producer.Start()
	go func() {
		for r := range e.producer.NotifyFailures() {
			// kinesis producer internally retries transient errors while
			// putting records to kinesis.
			// It only notifies impossible to recover errors like a stream
			// not existing.
			fmt.Println("kinesis put failed:: " + r.Error())
		}
	}()

	return e, nil
}

// Exporter is an implementation of trace.Exporter that uploads spans to Jaeger.
type Exporter struct {
	producer *producer.Producer
	onError  func(err error)
}

// Note: We do not implement trace.Exporter interface yet but it is planned
// var _ trace.Exporter = (*Exporter)(nil)

func (e *Exporter) Flush() {
	e.producer.Stop()
}

// ExportSpan exports a Jaeger protbuf span to Kinesis
func (e *Exporter) ExportSpan(span *gen.Span) {
	encoded, err := proto.Marshal(span)
	if err != nil {
		e.onError(err)
		return
	}

	err = e.producer.Put(encoded, span.TraceID.String())
	if err != nil {
		e.onError(err)
	}
}

