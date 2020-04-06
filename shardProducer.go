package kinesis

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	gen "github.com/jaegertracing/jaeger/model"

	// gzip "github.com/klauspost/pgzip"

	producer "github.com/signalfx/omnition-kinesis-producer"

	model "github.com/signalfx/opencensus-go-exporter-kinesis/models/gen"
)

const avgBatchSize = 1000

var compressedMagicByte = [8]byte{111, 109, 58, 106, 115, 112, 108, 122}

type shardProducer struct {
	sync.RWMutex

	pr            *producer.Producer
	hooks         KinesisHooker
	maxSize       uint64
	flushInterval time.Duration
	done          chan struct{}
	wg            sync.WaitGroup
	gzipWriter    *gzip.Writer
	spans         *model.SpanList
	size          uint64
}

func (sp *shardProducer) stop() {
	sp.wg.Wait()
	sp.flush()
	sp.pr.Stop()
}

func (sp *shardProducer) start() {
	sp.gzipWriter = gzip.NewWriter(&bytes.Buffer{})
	sp.spans = &model.SpanList{Spans: make([]*gen.Span, 0, avgBatchSize)}
	sp.size = 0
	sp.pr.Start()
	sp.wg.Add(1)
	go sp.flushPeriodically()
}

func (sp *shardProducer) currentSize() uint64 {
	sp.RLock()
	defer sp.RUnlock()
	return sp.size
}

func (sp *shardProducer) put(span *gen.Span, size uint64) error {
	// flush the queue and enqueue new span
	if sp.currentSize()+size >= sp.maxSize {
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

	// TODO: iterate over and set items to nil to enable GC on them?
	// Re-slicing to zero re-uses the same underlying array insead of re-allocating it.
	// This saves us a huge number of allocations but the downside is that spans from the
	// underlying array are never GC'ed. This should be okay as they'll be overwritten
	// anyway as newer spans arrive. This should allow us to make the spanlist consume
	// a static amount of memory throughout the life of the process.
	sp.spans.Spans = sp.spans.Spans[:0]
	sp.size = 0
}

// compress is unsafe for concurrent usage. caller must protect calls with mutexes
func (sp *shardProducer) compress(in []byte) []byte {
	var buf bytes.Buffer
	buf.Write(compressedMagicByte[:])
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
	defer sp.wg.Done()
	for {
		// add heuristics to not send very small batches unless
		// with too recent records
		select {
		case <-ticker.C:
			size := sp.currentSize()
			if size > 0 {
				sp.flush()
			}
		case <-sp.done:
			return
		}
	}
}
