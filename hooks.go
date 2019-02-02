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

func (h *kinesisHooks) OnDrain(size, length int64) {
	stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDrainSize.M(size),
		statDrainLength.M(length),
	)
}

func (h *kinesisHooks) OnPutRecords(batches, records, putLatencyMS int64, reason string) {
	stats.RecordWithTags(
		context.Background(),
		h.tags(tag.Upsert(tagFlushReason, reason)),
		statPutRequests.M(1),
		statPutBatches.M(batches),
		statPutSpans.M(records),
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

func (h *kinesisHooks) OnDropped(numRecords int64) {
	stats.RecordWithTags(
		context.Background(),
		h.tags(),
		statDroppedBatches.M(numRecords),
	)
}
