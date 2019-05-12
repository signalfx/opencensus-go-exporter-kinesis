package kinesis

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"sync"
	"time"

	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto"
	gen "github.com/jaegertracing/jaeger/model"

	// gzip "github.com/klauspost/pgzip"

	producer "github.com/omnition/omnition-kinesis-producer"

	model "github.com/omnition/opencensus-go-exporter-kinesis/models/gen"
)

const magicByteSize = 8
const avgBatchSize = 1000

var jaegerCompressedMagicByte = [8]byte{111, 109, 58, 106, 115, 112, 108, 122}
var ocCompressedMagicByte = [8]byte{111, 109, 58, 111, 115, 112, 108, 122}

type shardProducer struct {
	sync.RWMutex

	pr            *producer.Producer
	shard         *Shard
	hooks         *kinesisHooks
	maxSize       uint64
	flushInterval time.Duration
	partitionKey  string
	isJaeger      bool

	gzipWriter *gzip.Writer
	jSpans     *model.SpanList
	ocSpans    *tracepb.SpanList
	size       uint64
}

func (sp *shardProducer) start() {
	sp.gzipWriter = gzip.NewWriter(&bytes.Buffer{})
	if sp.isJaeger {
		sp.jSpans = &model.SpanList{Spans: make([]*gen.Span, 0, avgBatchSize)}
	} else {
		// sp.ocSpans = &model.SpanList{Spans: make([]*gen.Span, 0, avgBatchSize)}
	}
	sp.size = 0

	sp.pr.Start()
	go sp.flushPeriodically()
}

func (sp *shardProducer) currentSize() uint64 {
	sp.RLock()
	defer sp.RUnlock()
	return sp.size
}

func (sp *shardProducer) putJaeger(span *gen.Span, size uint64) error {
	// flush the queue and enqueue new span
	if sp.currentSize()+size >= sp.maxSize {
		sp.flush()
	}

	sp.Lock()
	sp.jSpans.Spans = append(sp.jSpans.Spans, span)
	sp.size += size
	sp.Unlock()
	// atomic.AddUint64(&sp.size, size)
	return nil
}

func (sp *shardProducer) putOC(span *tracepb.Span, size uint64) error {
	// flush the queue and enqueue new span
	if sp.currentSize()+size >= sp.maxSize {
		sp.flush()
	}

	sp.Lock()
	sp.ocSpans.Spans = append(sp.ocSpans.Spans, span)
	sp.size += size
	sp.Unlock()
	return nil
}

func (sp *shardProducer) flush() {
	sp.Lock()
	defer sp.Unlock()
	if sp.size <= 0 {
		return
	}
	var encErr error
	var encoded []byte
	var traceID string
	var numSpans int
	if sp.isJaeger {
		encoded, encErr = gogoproto.Marshal(sp.jSpans)
		traceID = sp.jSpans.Spans[0].TraceID.String()
		numSpans = len(sp.jSpans.Spans)
	} else {
		encoded, encErr = proto.Marshal(sp.ocSpans)
		traceID = string(sp.ocSpans.Spans[0].TraceId)
		numSpans = len(sp.ocSpans.Spans)
	}
	if encErr != nil {
		fmt.Println("failed to marshal: ", encErr)
		return
	}

	compressed := sp.compress(encoded)
	sp.pr.Put(compressed, traceID)

	sp.hooks.OnCompressed(int64(len(encoded)), int64(len(compressed)))
	sp.hooks.OnPutSpanListFlushed(int64(numSpans), int64(len(compressed)))

	// TODO: iterate over and set items to nil to enable GC on them?
	// Re-slicing to zero re-uses the same underlying array insead of re-allocating it.
	// This saves us a huge number of allocations but the downside is that spans from the
	// underlying array are never GC'ed. This should be okay as they'll be overwritten
	// anyway as newer spans arrive. This should allow us to make the spanlist consume
	// a static amount of memory throughout the life of the process.
	// sp.spans.Spans = sp.spans.Spans[:0]
	if sp.isJaeger {
		sp.jSpans.Spans = sp.jSpans.Spans[:0]
	} else {
		sp.ocSpans.Spans = sp.ocSpans.Spans[:0]
	}
	sp.size = 0
}

// compress is unsafe for concurrent usage. caller must protect calls with mutexes
func (sp *shardProducer) compress(in []byte) []byte {
	var buf bytes.Buffer
	if sp.isJaeger {
		buf.Write(jaegerCompressedMagicByte[:])
	} else {
		buf.Write(ocCompressedMagicByte[:])
	}
	sp.gzipWriter.Reset(&buf)

	_, err := sp.gzipWriter.Write(in)
	if err != nil {
		log.Fatal(err)
	}

	if err := sp.gzipWriter.Close(); err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func (sp *shardProducer) flushPeriodically() {
	ticker := time.NewTicker(sp.flushInterval)
	for {
		// add heuristics to not send very small batches unless
		// with too recent records
		<-ticker.C
		size := sp.currentSize()
		if size > 0 {
			sp.flush()
		}
	}
}
