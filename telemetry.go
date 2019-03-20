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

package kinesis

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var latencyDistributionAggregation = view.Distribution(
	10, 25, 50, 75, 100, 250, 500, 750, 1000, 2000, 3000, 4000, 5000, 10000, 20000, 30000, 50000,
)

// Keys and stats for telemetry.
var (
	tagExporterName, _ = tag.NewKey("exporter")
	tagStreamName, _   = tag.NewKey("stream_name")
	tagShardId, _      = tag.NewKey("shard_id")
	tagFlushReason, _  = tag.NewKey("flush_reason")
	tagErrCode, _      = tag.NewKey("err_code")

	statEnqueuedSpans = stats.Int64("kinesis_enqueued_spans", "spans received and put in a queue to be processed by kinesis exporter", stats.UnitDimensionless)
	statDequeuedSpans = stats.Int64("kinesis_dequeued_spans", "spans taken out of queue and processed by kinesis exporter", stats.UnitDimensionless)

	statPutRequests = stats.Int64("kinesis_put_requests", "number of put requests made", stats.UnitDimensionless)
	statPutBatches  = stats.Int64("kinesis_put_batches", "number of batches pushed to a stream", stats.UnitDimensionless)
	statPutSpans    = stats.Int64("kinesis_put_spans", "number of spans pushed to a stream", stats.UnitDimensionless)
	statPutBytes    = stats.Int64("kinesis_put_bytes", "number of bytes pushed to a stream", stats.UnitDimensionless)
	statPutErrors   = stats.Int64("kinesis_put_errors", "number of errors from put requests", stats.UnitDimensionless)
	statPutLatency  = stats.Int64("kinesis_put_latency", "time (ms) it took to complete put requests", stats.UnitMilliseconds)

	statDroppedBatches = stats.Int64("kinesis_dropped_batches", "number of batches dropped by producer", stats.UnitDimensionless)
	statDroppedSpans   = stats.Int64("kinesis_dropped_spans", "number of spans dropped by producer", stats.UnitDimensionless)
	statDroppedBytes   = stats.Int64("kinesis_dropped_bytes", "number of bytes dropped by producer", stats.UnitDimensionless)

	statDrainBytes  = stats.Int64("kinesis_drain_bytes", "size (bytes) of batches when drained from queue", stats.UnitBytes)
	statDrainLength = stats.Int64("kinesis_drain_length", "number of batches drained from queue", stats.UnitDimensionless)
)

// TODO: support telemetry level

// MetricViews return the metrics views according to given telemetry level.
func metricViews() []*view.View {
	tagKeys := []tag.Key{tagExporterName, tagStreamName, tagShardId, tagFlushReason}

	// There are some metrics enabled, return the views.
	enqueuedSpansView := &view.View{
		Name:        statEnqueuedSpans.Name(),
		Measure:     statEnqueuedSpans,
		Description: "spans received and put in a queue to be processed by kinesis exporter",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	dequeuedSpansView := &view.View{
		Name:        statDequeuedSpans.Name(),
		Measure:     statDequeuedSpans,
		Description: "spans taken out of queue and processed by kinesis exporter",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	putRequestsView := &view.View{
		Name:        statPutRequests.Name(),
		Measure:     statPutRequests,
		Description: "Number of put requests made to kinesis.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	putBatchesView := &view.View{
		Name:        statPutBatches.Name(),
		Measure:     statPutBatches,
		Description: "Number of batches pushed to kinesis.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	putSpansView := &view.View{
		Name:        statPutSpans.Name(),
		Measure:     statPutSpans,
		Description: "Number of spans pushed to kinesis.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	putBytesView := &view.View{
		Name:        statPutBytes.Name(),
		Measure:     statPutBytes,
		Description: "Bytes pushed to kinesis.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	errorsView := &view.View{
		Name:        statPutErrors.Name(),
		Measure:     statPutErrors,
		Description: "Number of errors raised by kinesis put requests.",
		TagKeys:     append(tagKeys, tagErrCode),
		Aggregation: view.Sum(),
	}

	putLatencyView := &view.View{
		Name:        statPutLatency.Name(),
		Measure:     statPutLatency,
		Description: "Time it takes for put request to kinesis to complete.",
		TagKeys:     tagKeys,
		Aggregation: latencyDistributionAggregation,
	}

	droppedBatchesView := &view.View{
		Name:        statDroppedBatches.Name(),
		Measure:     statDroppedBatches,
		Description: "Number of batches dropped due to recurring errors.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	droppedSpansView := &view.View{
		Name:        statDroppedSpans.Name(),
		Measure:     statDroppedSpans,
		Description: "Number of spans dropped due to recurring errors.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	droppedBytesView := &view.View{
		Name:        statDroppedBytes.Name(),
		Measure:     statDroppedBytes,
		Description: "Bytes dropped due to recurring errors.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	drainBytesView := &view.View{
		Name:        statDrainBytes.Name(),
		Measure:     statDrainBytes,
		Description: "Size (bytes) of aggregated batches.",
		TagKeys:     tagKeys,
		Aggregation: view.LastValue(),
	}

	drainLengthView := &view.View{
		Name:        statDrainLength.Name(),
		Measure:     statDrainLength,
		Description: "Length (number of records) of aggregated batches.",
		TagKeys:     tagKeys,
		Aggregation: view.LastValue(),
	}

	return []*view.View{
		enqueuedSpansView,
		dequeuedSpansView,
		putRequestsView,
		putBatchesView,
		putBytesView,
		putSpansView,
		putLatencyView,
		errorsView,
		droppedBatchesView,
		droppedSpansView,
		droppedBytesView,
		drainBytesView,
		drainLengthView,
	}
}
