package kinesis

import (
	"context"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

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
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDrainBytes.M(bytes),
		statDrainLength.M(length),
	)
}

func (h *kinesisHooks) OnPutRecords(batches, spanlists, bytes, putLatencyMS int64, reason string) {
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

func (h *kinesisHooks) OnPutErr(errCode string) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagErrCode, errCode)),
		statPutErrors.M(1),
	)
}

func (h *kinesisHooks) OnDropped(batches, spanlists, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDroppedBatches.M(batches),
		statDroppedSpanLists.M(spanlists),
		statDroppedBytes.M(bytes),
	)
}

func (h *kinesisHooks) OnSpanEnqueued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statEnqueuedSpans.M(1),
	)
}

func (h *kinesisHooks) OnSpanDequeued() {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDequeuedSpans.M(1),
	)
}

func (h *kinesisHooks) OnXLSpanDropped(size int) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statXLSpansBytes.M(int64(size)),
		statXLSpans.M(1),
	)
}

func (h *kinesisHooks) OnPutSpanListFlushed(spans, bytes int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statFlushedSpans.M(spans),
		statSpanListBytes.M(bytes),
	)
}

func (h *kinesisHooks) OnCompressed(original, compressed int64) {
	_ = stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statCompressFactor.M(original/compressed),
	)
}
