package kinesis

import (
	"context"
	"sync"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

type tagCache struct {
	sync.RWMutex
	tags map[tag.Key]map[string]tag.Mutator
}

func (c *tagCache) get(name tag.Key, value string) tag.Mutator {
	c.RLock()
	values, ok := c.tags[name]
	c.RUnlock()

	if !ok {
		t := tag.Upsert(name, value)
		c.Lock()
		c.tags[name] = map[string]tag.Mutator{
			value: t,
		}
		c.Unlock()
		return t
	}

	c.RLock()
	t, ok := values[value]
	c.RUnlock()
	if ok {
		return t
	}
	t = tag.Upsert(name, value)
	c.Lock()
	values[value] = t
	c.Unlock()
	return t
}

// KinesisHooker interface abstracts away the hook so one can pass in a producer and inject your own implementation
type KinesisHooker interface {
	OnDrain(bytes, length int64)
	OnPutRecords(batches, spanlists, bytes, putLatencyMS int64, reason string)
	OnPutErr(errCode string)
	OnDropped(batches, spanlists, bytes int64)
	OnSpanEnqueued()
	OnSpanDequeued()
	OnXLSpanDropped(size int)
	OnPutSpanListFlushed(spans, bytes int64)
	OnCompressed(original, compressed int64)
}

var _ KinesisHooker = &kinesisHooks{}

type kinesisHooks struct {
	exporterName string
	streamName   string
	commonTags   []tag.Mutator
	tagCache     tagCache
}

func newKinesisHooks(name, streamName, shardID string) *kinesisHooks {
	tags := []tag.Mutator{
		tag.Upsert(tagExporterName, name),
		tag.Upsert(tagStreamName, streamName),
	}
	if shardID != "" {
		tags = append(tags, tag.Upsert(tagShardID, shardID))
	}
	return &kinesisHooks{
		exporterName: name,
		streamName:   streamName,
		commonTags:   tags,
		tagCache: tagCache{
			tags: map[tag.Key]map[string]tag.Mutator{},
		},
	}
}

func (h *kinesisHooks) tags(name tag.Key, value string) []tag.Mutator {
	tags := make([]tag.Mutator, 0, len(h.commonTags)+1)
	copy(tags, h.commonTags)
	tags = append(tags, h.tagCache.get(name, value))
	return tags
}

func (h *kinesisHooks) OnDrain(bytes, length int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.commonTags,
		statDrainBytes.M(bytes),
		statDrainLength.M(length),
	)
}

func (h *kinesisHooks) OnPutRecords(batches, spanlists, bytes, putLatencyMS int64, reason string) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(tagFlushReason, reason),
		statPutRequests.M(1),
		statPutBatches.M(batches),
		statPutSpanLists.M(spanlists),
		statPutBytes.M(bytes),
		statPutLatency.M(putLatencyMS),
	)
}

func (h *kinesisHooks) OnPutErr(errCode string) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(tagErrCode, errCode),
		statPutErrors.M(1),
	)
}

func (h *kinesisHooks) OnDropped(batches, spanlists, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.commonTags,
		statDroppedBatches.M(batches),
		statDroppedSpanLists.M(spanlists),
		statDroppedBytes.M(bytes),
	)
}

func (h *kinesisHooks) OnSpanEnqueued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.commonTags,
		statEnqueuedSpans.M(1),
	)
}

func (h *kinesisHooks) OnSpanDequeued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.commonTags,
		statDequeuedSpans.M(1),
	)
}

func (h *kinesisHooks) OnXLSpanDropped(size int) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.commonTags,
		statXLSpansBytes.M(int64(size)),
		statXLSpans.M(1),
	)
}

func (h *kinesisHooks) OnPutSpanListFlushed(spans, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.commonTags,
		statFlushedSpans.M(spans),
		statSpanListBytes.M(bytes),
	)
}

func (h *kinesisHooks) OnCompressed(original, compressed int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.commonTags,
		statCompressFactor.M(original/compressed),
	)
}
