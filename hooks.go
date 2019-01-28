package kinesis

import (
	"context"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

type kinesisHooks struct {
	streamName string
}

func (h *kinesisHooks) OnDrain(size, length int64) {
	statsTags := []tag.Mutator{tag.Upsert(tagStreamName, h.streamName)}

	stats.RecordWithTags(
		context.Background(),
		statsTags,
		statDrainSize.M(size),
		statDrainLength.M(length),
	)
}

func (h *kinesisHooks) OnPutRecords(batches, records, putLatencyMS int64, reason string) {
	statsTags := []tag.Mutator{
		tag.Upsert(tagStreamName, h.streamName),
		tag.Upsert(tagFlushReason, reason),
	}
	stats.RecordWithTags(
		context.Background(),
		statsTags,
		statPutRequests.M(1),
		statPutBatches.M(batches),
		statPutSpans.M(records),
		statPutLatency.M(putLatencyMS),
	)
}

func (h *kinesisHooks) OnPutErr(errCode string) {
	statsTags := []tag.Mutator{
		tag.Upsert(tagStreamName, h.streamName),
		tag.Upsert(tagErrCode, errCode),
	}
	stats.RecordWithTags(context.Background(), statsTags, statPutErrors.M(1))
}
