package kinesis

import (
	"context"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

type kinesisHooks struct {
	exporterName string
	streamName   string
}

func (h *kinesisHooks) tags(tags ...tag.Mutator) []tag.Mutator {
	tags = append(tags, tag.Upsert(tagStreamName, h.streamName))
	tags = append(tags, tag.Upsert(tagExporterName, h.exporterName))
	return tags
}

func (h *kinesisHooks) OnDrain(bytes, length int64) {
	stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDrainBytes.M(bytes),
		statDrainLength.M(length),
	)
}

func (h *kinesisHooks) OnPutRecords(batches, spans, bytes, putLatencyMS int64, reason string) {
	stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagFlushReason, reason)),
		statPutRequests.M(1),
		statPutBatches.M(batches),
		statPutSpans.M(spans),
		statPutBytes.M(bytes),
		statPutLatency.M(putLatencyMS),
	)
}

func (h *kinesisHooks) OnPutErr(errCode string) {
	stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagErrCode, errCode)),
		statPutErrors.M(1),
	)
}

func (h *kinesisHooks) OnDropped(batches, spans, bytes int64) {
	stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDroppedBatches.M(batches),
		statDroppedSpans.M(spans),
		statDroppedBytes.M(bytes),
	)
}

func (h *kinesisHooks) OnSpanEnqueued() {
	stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statEnqueuedSpans.M(1),
	)
}

func (h *kinesisHooks) OnSpanDequeued() {
	stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDequeuedSpans.M(1),
	)
}
