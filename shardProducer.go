package kinesis

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/gogo/protobuf/proto"
	gen "github.com/jaegertracing/jaeger/model"
	producer "github.com/omnition/kinesis-producer"

	model "github.com/omnition/opencensus-go-exporter-kinesis/models/gen"
)

var compressedMagicByte = [8]byte{111, 109, 58, 106, 115, 112, 108, 122}


type shardProducer struct {
	sync.RWMutex

	pr            *producer.Producer
	shard         *Shard
	hooks         *kinesisHooks
	maxSize       uint64
	flushInterval time.Duration
	partitionKey  string

	spans *model.SpanList
	size  uint64
}

func (sp *shardProducer) start() {
	sp.spans = &model.SpanList{}
	sp.size = 0
	sp.pr.Start()
	go sp.flushPeriodically()
	go func(sp *shardProducer) {
		for r := range sp.pr.NotifyFailures() {
			err, _ := r.Err.(awserr.Error)
			fmt.Println(err)
			// TODO: replace logger with recording metrics
		}
	}(sp)
}

func (sp *shardProducer) currentSize() uint64 {
	sp.RLock()
	defer sp.RUnlock()
	return sp.size
}

func (sp *shardProducer) put(span *gen.Span, size uint64) error {
	// flush the queue and enqueue new span

	currentSize := sp.currentSize()
	if currentSize+size >= sp.maxSize {
		sp.flush()
	}

	sp.Lock()
	sp.spans.Spans = append(sp.spans.Spans, span)
	sp.size += size
	sp.Unlock()
	// atomic.AddUint64(&sp.size, size)
	return nil
}

func (sp *shardProducer) flush() {
	sp.Lock()
	defer sp.Unlock()

	numSpans := len(sp.spans.Spans)
	if numSpans == 0 {
		return
	}
	encoded, err := proto.Marshal(sp.spans)
	if err != nil {
		fmt.Println("failed to marshal: ", err)
		return
	}

	compressed := sp.compress(encoded)
	sp.pr.Put(compressed, sp.spans.Spans[0].TraceID.String())

	sp.hooks.OnCompressed(int64(len(encoded)), int64(len(compressed)))
	sp.hooks.OnPutSpanListFlushed(int64(len(sp.spans.Spans)), int64(len(compressed)))

	sp.spans = &model.SpanList{}
	sp.size = 0
}

func (sp *shardProducer) compress(in []byte) []byte {
	var buf bytes.Buffer
	buf.Write(compressedMagicByte[:])

	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(in)
	if err != nil {
		log.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		log.Fatal(err)
	}

	return buf.Bytes()
}

func (sp *shardProducer) flushPeriodically() {
	// use ticker
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
