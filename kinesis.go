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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gogo/protobuf/proto"
	gen "github.com/jaegertracing/jaeger/model"
	producer "github.com/omnition/kinesis-producer"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
)

const defaultEncoding = "jaeger-proto"

var supportedEncodings = [1]string{defaultEncoding}

// Options are the options to be used when initializing a Jaeger exporter.
type Options struct {
	Name                    string
	StreamName              string
	AWSRegion               string
	AWSRole                 string
	AWSKinesisEndpoint      string
	QueueSize               int
	NumWorkers              int
	KPLBatchSize            int
	KPLBatchCount           int
	KPLBacklogCount         int
	KPLFlushIntervalSeconds int
	KPLMaxConnections       int

	// Encoding defines the format in which spans should be exporter to kinesis
	// only Jaeger is supported right now
	Encoding string
}

func (o Options) isValidEncoding() bool {
	for _, e := range supportedEncodings {
		if e == o.Encoding {
			return true
		}
	}
	return false
}

// NewExporter returns a trace.Exporter implementation that exports
// the collected spans to Jaeger.
func NewExporter(o Options, logger *zap.Logger) (*Exporter, error) {
	if o.QueueSize == 0 {
		o.QueueSize = 100000
	}
	if o.NumWorkers == 0 {
		o.NumWorkers = 8
	}
	if o.AWSRegion == "" {
		return nil, errors.New("missing AWS Region for Kinesis exporter")
	}

	if o.StreamName == "" {
		return nil, errors.New("missing Stream Name for Kinesis exporter")
	}

	if o.Encoding == "" {
		o.Encoding = defaultEncoding
	}

	if !o.isValidEncoding() {
		return nil, fmt.Errorf("invalid option for Encoding. Valid choices are: %v", supportedEncodings)
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

	shards, err := getShards(client, o.StreamName)
	if err != nil {
		return nil, err
	}

	producers := make([]*shardProducer, 0, len(shards))
	for _, shard := range shards {

		hooks := &kinesisHooks{
			o.Name,
			o.StreamName,
			shard.shardId,
		}

		pr := producer.New(&producer.Config{
			StreamName:     o.StreamName,
			BatchSize:      o.KPLBatchSize,
			BatchCount:     o.KPLBatchCount,
			BacklogCount:   o.KPLBacklogCount,
			MaxConnections: o.KPLMaxConnections,
			FlushInterval:  time.Second * time.Duration(o.KPLFlushIntervalSeconds),
			Client:         client,
			Verbose:        false,
		}, hooks)
		producers = append(producers, &shardProducer{
			pr:    pr,
			shard: shard,
		})
	}

	e := &Exporter{
		producers: producers,
		logger:    logger,
		hooks: &kinesisHooks{
			exporterName: o.Name,
			streamName:   o.StreamName,
		},
		queue: make(chan *gen.Span, o.QueueSize),
	}

	v := metricViews()
	if err := view.Register(v...); err != nil {
		return nil, err
	}

	for _, sp := range e.producers {
		sp.pr.Start()
		go func(sp *shardProducer) {
			for r := range sp.pr.NotifyFailures() {
				err, _ := r.Err.(awserr.Error)
				logger.Error("err pushing records to kinesis: ", zap.Error(err))
			}
		}(sp)
	}

	for i := 0; i < o.NumWorkers; i++ {
		go e.loop()
	}

	return e, nil
}

type shardProducer struct {
	pr    *producer.Producer
	shard *Shard
}

// Exporter takes spans in jaeger proto format and forwards them to a kinesis stream
type Exporter struct {
	producers []*shardProducer
	logger    *zap.Logger
	hooks     *kinesisHooks
	queue     chan *gen.Span
}

// Note: We do not implement trace.Exporter interface yet but it is planned
// var _ trace.Exporter = (*Exporter)(nil)
func (e *Exporter) Flush() {
	for _, sp := range e.producers {
		sp.pr.Stop()
	}
}

// ExportSpan exports a Jaeger protbuf span to Kinesis
func (e *Exporter) ExportSpan(span *gen.Span) error {
	e.queue <- span
	e.hooks.OnSpanEnqueued()
	return nil
}

func (e *Exporter) loop() {
	// TODO: Add graceful shutdown
	for {
		// TODO: check all errors and record metrics
		span := <-e.queue
		e.hooks.OnSpanDequeued()
		traceID := span.TraceID.String()
		sp, err := e.getShardProducer(span.TraceID.String())
		if err != nil {
			fmt.Println("failed to get producer/shard for traceID: ", err)
		}
		encoded, err := proto.Marshal(span)
		if err != nil {
			fmt.Println("failed to marshal: ", err)
		}
		err = sp.pr.Put(encoded, traceID)
		if err != nil {
			fmt.Println("error putting span: ", err)
		}
	}
}

func (e *Exporter) getShardProducer(partitionKey string) (*shardProducer, error) {
	for _, sp := range e.producers {
		ok, err := sp.shard.belongsToShard(partitionKey)
		if err != nil {
			return nil, err
		}
		if ok {
			return sp, nil
		}
	}
	return nil, fmt.Errorf("no shard found for parition key %s", partitionKey)
}
