package kinesis

import (
	"context"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

type kinesisHooks2 struct {
	exporterName string
	streamName   string
	shardID      string
}

func (h *kinesisHooks2) tags(tags ...tag.Mutator) []tag.Mutator {
	tags = append(tags, tag.Upsert(tagStreamName, h.streamName))
	tags = append(tags, tag.Upsert(tagExporterName, h.exporterName))
	tags = append(tags, tag.Upsert(tagShardID, h.shardID))
	return tags
}

func (h *kinesisHooks2) OnDrain(bytes, length int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDrainBytes.M(bytes),
		statDrainLength.M(length),
	)
}

func (h *kinesisHooks2) OnPutRecords(batches, spanlists, bytes, putLatencyMS int64, reason string) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagFlushReason, reason)),
		statPutRequests.M(1),
		statPutBatches.M(batches),
		// statPutSpans.M(spans),
		statPutSpanLists.M(spanlists),
		statPutBytes.M(bytes),
		statPutLatency.M(putLatencyMS),
	)
}

func (h *kinesisHooks2) OnPutErr(errCode string) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagErrCode, errCode)),
		statPutErrors.M(1),
	)
}

func (h *kinesisHooks2) OnDropped(batches, spanlists, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDroppedBatches.M(batches),
		statDroppedSpanLists.M(spanlists),
		statDroppedBytes.M(bytes),
	)
}

func (h *kinesisHooks2) OnSpanEnqueued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statEnqueuedSpans.M(1),
	)
}

func (h *kinesisHooks2) OnSpanDequeued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDequeuedSpans.M(1),
	)
}

func (h *kinesisHooks2) OnXLSpanDropped(size int) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statXLSpansBytes.M(int64(size)),
		statXLSpans.M(1),
	)
}

func (h *kinesisHooks2) OnPutSpanListFlushed(spans, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statFlushedSpans.M(spans),
		statSpanListBytes.M(bytes),
	)
}

func (h *kinesisHooks2) OnCompressed(original, compressed int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statCompressFactor.M(original/compressed),
	)
}

type kinesisHooks struct {
	exporterName string
	streamName   string
	shardID      string
}

func (h *kinesisHooks) tags(tags ...tag.Mutator) []tag.Mutator {
	tags = append(tags, tag.Upsert(tagStreamName, h.streamName))
	tags = append(tags, tag.Upsert(tagExporterName, h.exporterName))
	tags = append(tags, tag.Upsert(tagShardID, h.shardID))
	return tags
}

func (h *kinesisHooks) OnDrain(bytes, length int64) {
}

func (h *kinesisHooks) OnPutRecords(batches, spanlists, bytes, putLatencyMS int64, reason string) {
}

func (h *kinesisHooks) OnPutErr(errCode string) {
}

func (h *kinesisHooks) OnDropped(batches, spanlists, bytes int64) {
}

func (h *kinesisHooks) OnSpanEnqueued() {
}

func (h *kinesisHooks) OnSpanDequeued() {
}

func (h *kinesisHooks) OnXLSpanDropped(size int) {
}

func (h *kinesisHooks) OnPutSpanListFlushed(spans, bytes int64) {
}

func (h *kinesisHooks) OnCompressed(original, compressed int64) {
}

type kinesisHooksOpt struct {
	exporterName string
	streamName   string
	shardID      string
	_tags        []tag.Mutator
}

func newKinesisHooksOpt(name, streamName string) *kinesisHooksOpt {
	return &kinesisHooksOpt{
		exporterName: name,
		streamName:   streamName,
	}
}

func (h *kinesisHooksOpt) tags(tags ...tag.Mutator) []tag.Mutator {
	return h._tags
	/*
		tags = append(tags, tag.Upsert(tagStreamName, h.streamName))
		tags = append(tags, tag.Upsert(tagExporterName, h.exporterName))
		tags = append(tags, tag.Upsert(tagShardID, h.shardID))
		return tags
	*/
}

func (h *kinesisHooksOpt) OnDrain(bytes, length int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDrainBytes.M(bytes),
		statDrainLength.M(length),
	)
}

func (h *kinesisHooksOpt) OnPutRecords(batches, spanlists, bytes, putLatencyMS int64, reason string) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagFlushReason, reason)),
		statPutRequests.M(1),
		statPutBatches.M(batches),
		// statPutSpans.M(spans),
		statPutSpanLists.M(spanlists),
		statPutBytes.M(bytes),
		statPutLatency.M(putLatencyMS),
	)
}

func (h *kinesisHooksOpt) OnPutErr(errCode string) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagErrCode, errCode)),
		statPutErrors.M(1),
	)
}

func (h *kinesisHooksOpt) OnDropped(batches, spanlists, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDroppedBatches.M(batches),
		statDroppedSpanLists.M(spanlists),
		statDroppedBytes.M(bytes),
	)
}

func (h *kinesisHooksOpt) OnSpanEnqueued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statEnqueuedSpans.M(1),
	)
}

func (h *kinesisHooksOpt) OnSpanDequeued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDequeuedSpans.M(1),
	)
}

func (h *kinesisHooksOpt) OnXLSpanDropped(size int) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statXLSpansBytes.M(int64(size)),
		statXLSpans.M(1),
	)
}

func (h *kinesisHooksOpt) OnPutSpanListFlushed(spans, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statFlushedSpans.M(spans),
		statSpanListBytes.M(bytes),
	)
}

func (h *kinesisHooksOpt) OnCompressed(original, compressed int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statCompressFactor.M(original/compressed),
	)
}
