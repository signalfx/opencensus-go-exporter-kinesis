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
package kinesis // import "github.com/signalfx/opencensus-go-exporter-kinesis"

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gogo/protobuf/proto"

	gen "github.com/jaegertracing/jaeger/model"
	producer "github.com/signalfx/omnition-kinesis-producer"
	"go.uber.org/zap"
)

const defaultEncoding = "jaeger-proto"

var supportedEncodings = [1]string{defaultEncoding}

type HookProducer func(name, streamName, shardID string) KinesisHooker

// Options are the options to be used when initializing a Jaeger exporter.
type Options struct {
	Name                    string
	StreamName              string
	AWSRegion               string
	AWSRole                 string
	AWSKinesisEndpoint      string
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
	// to be called if data is put on an unexpected shard
	OnReshard func()
	// if you want to inject your own hooks into the exporter, else th default hooks will be created
	HookProducer HookProducer

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

	if o.MaxListSize == 0 {
		o.MaxListSize = 100000
	}

	if o.ListFlushInterval == 0 {
		o.ListFlushInterval = 5
	}

	if o.MaxAllowedSizePerSpan == 0 {
		o.MaxAllowedSizePerSpan = 900000
	}

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

	if o.HookProducer == nil {
		o.HookProducer = func(name, streamName, shardID string) KinesisHooker {
			return newKinesisHooks(name, streamName, shardID)
		}
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
		hooks := o.HookProducer(o.Name, o.StreamName, shard.shardId)
		pr := producer.New(&producer.Config{
			OnReshard:           o.OnReshard,
			Shard:               shard.shardId,
			StreamName:          o.StreamName,
			AggregateBatchSize:  o.KPLAggregateBatchSize,
			AggregateBatchCount: o.KPLAggregateBatchCount,
			BatchSize:           o.KPLBatchSize,
			BatchCount:          o.KPLBatchCount,
			BacklogCount:        o.KPLBacklogCount,
			MaxConnections:      o.KPLMaxConnections,
			FlushInterval:       time.Second * time.Duration(o.KPLFlushIntervalSeconds),
			MaxRetries:          o.KPLMaxRetries,
			MaxBackoffTime:      time.Second * time.Duration(o.KPLMaxBackoffSeconds),
			Client:              client,
			Verbose:             false,
		}, hooks)
		producers = append(producers, &shardProducer{
			pr:            pr,
			shard:         shard,
			hooks:         hooks,
			maxSize:       uint64(o.MaxListSize),
			flushInterval: time.Duration(o.ListFlushInterval) * time.Second,
			partitionKey:  shard.startingHashKey.String(),
		})
	}

	e := &Exporter{
		options:   &o,
		producers: producers,
		logger:    logger,
		hooks:     o.HookProducer(o.Name, o.StreamName, ""),
		semaphore: nil,
	}

	maxReceivers, _ := strconv.Atoi(os.Getenv("MAX_KINESIS_RECEIVERS"))
	if maxReceivers > 0 {
		e.semaphore = make(chan struct{}, maxReceivers)
	}

	if err := registerMetricViews(); err != nil {
		return nil, err
	}

	for _, sp := range e.producers {
		sp.start()
	}

	return e, nil
}

// Exporter takes spans in jaeger proto format and forwards them to a kinesis stream
type Exporter struct {
	options   *Options
	producers []*shardProducer
	logger    *zap.Logger
	hooks     KinesisHooker
	semaphore chan struct{}
}

// Note: We do not implement trace.Exporter interface yet but it is planned
// var _ trace.Exporter = (*Exporter)(nil)

// Flush flushes queues and stops exporters
func (e *Exporter) Flush() {
	for _, sp := range e.producers {
		sp.pr.Stop()
	}
	if e.semaphore != nil {
		close(e.semaphore)
	}
}

func (e *Exporter) acquire() {
	if e.semaphore != nil {
		e.semaphore <- struct{}{}
	}
}

func (e *Exporter) release() {
	if e.semaphore != nil {
		<-e.semaphore
	}
}

// ExportSpan exports a Jaeger protbuf span to Kinesis
func (e *Exporter) ExportSpan(span *gen.Span) error {
	e.hooks.OnSpanEnqueued()
	e.acquire()
	go e.processSpan(span)
	return nil
}

func (e *Exporter) processSpan(span *gen.Span) {
	defer e.release()
	e.hooks.OnSpanDequeued()
	sp, err := e.getShardProducer(span.TraceID.String())
	if err != nil {
		fmt.Println("failed to get producer/shard for traceID: ", err)
		return
	}
	encoded, err := proto.Marshal(span)
	if err != nil {
		fmt.Println("failed to marshal: ", err)
		return
	}
	size := len(encoded)
	if size > e.options.MaxAllowedSizePerSpan {
		sp.hooks.OnXLSpanDropped(size)
		span.Tags = []gen.KeyValue{
			{Key: "omnition.dropped", VBool: true, VType: gen.ValueType_BOOL},
			{Key: "omnition.dropped.reason", VStr: "unsupported size", VType: gen.ValueType_STRING},
			{Key: "omnition.dropped.size", VInt64: int64(size), VType: gen.ValueType_INT64},
		}
		span.Logs = []gen.Log{}
		encoded, err = proto.Marshal(span)
		if err != nil {
			fmt.Println("failed to modified span: ", err)
			return
		}
		size = len(encoded)
	}
	// TODO: See if we can encode only once and put encoded span on the shard producer.
	// shard producer will have to arrange the bytes exactly as protobuf marshaller would
	// encode a SpanList object.
	// err = sp.pr.Put(encoded, traceID)
	err = sp.put(span, uint64(size))
	if err != nil {
		fmt.Println("error putting span: ", err)
	}
}

/*
func (e *Exporter) loop() {
	// TODO: Add graceful shutdown
	for {
		// TODO: check all errors and record metrics
		// handle channel closing
		span := <-e.queue
		e.processSpan(span)
	}
}
*/

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
