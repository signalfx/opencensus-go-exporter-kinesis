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

// Keys and stats for telemetry.
var (
	tagExporterName, _ = tag.NewKey("exporter")
	tagStreamName, _   = tag.NewKey("stream_name")
	tagFlushReason, _  = tag.NewKey("flush_reason")
	tagErrCode, _      = tag.NewKey("err_code")

	statPutRequests = stats.Int64("kinesis_put_requests", "number of put requests made", stats.UnitDimensionless)
	statPutBatches  = stats.Int64("kinesis_put_batches", "number of batches pushed to a stream", stats.UnitDimensionless)
	statPutSpans    = stats.Int64("kinesis_put_spans", "number of spans pushed to a stream", stats.UnitDimensionless)
	statPutErrors   = stats.Int64("kinesis_put_errors", "number of errors from put requests", stats.UnitDimensionless)
	statPutLatency  = stats.Int64("kinesis_put_latency", "time (ms) it took to complete put requests", stats.UnitMilliseconds)

	statDroppedBatches = stats.Int64("kinesis_dropped_batches", "number of batches dropped by producer", stats.UnitDimensionless)

	statDrainSize   = stats.Int64("kinesis_drain_size", "size of batches when drained from queue", stats.UnitBytes)
	statDrainLength = stats.Int64("kinesis_drain_length", "number of batches drained from queue", stats.UnitDimensionless)
)

// TODO: support telemetry level

// MetricViews return the metrics views according to given telemetry level.
func metricViews() []*view.View {
	tagKeys := []tag.Key{tagExporterName, tagStreamName, tagFlushReason}

	// There are some metrics enabled, return the views.
	putView := &view.View{
		Name:        statPutRequests.Name(),
		Measure:     statPutRequests,
		Description: "Number of put requests made to kinesis.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	batchesView := &view.View{
		Name:        statPutBatches.Name(),
		Measure:     statPutBatches,
		Description: "Number of batches pushed to kinesis.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	spansView := &view.View{
		Name:        statPutSpans.Name(),
		Measure:     statPutSpans,
		Description: "Number of spans pushed to kinesis.",
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

	latencyDistributionAggregation := view.Distribution(10, 25, 50, 75, 100, 250, 500, 750, 1000, 2000, 3000, 4000, 5000, 10000, 20000, 30000, 50000)
	putLatencyView := &view.View{
		Name:        statPutLatency.Name(),
		Measure:     statPutLatency,
		Description: "Time it takes for put request to kinesis to complete.",
		TagKeys:     tagKeys,
		Aggregation: latencyDistributionAggregation,
	}

	droppedView := &view.View{
		Name:        statDroppedBatches.Name(),
		Measure:     statDroppedBatches,
		Description: "Number of batches dropped due to recurring errors.",
		TagKeys:     tagKeys,
		Aggregation: view.Sum(),
	}

	drainSizeView := &view.View{
		Name:        statDrainSize.Name(),
		Measure:     statDrainSize,
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
		putView, batchesView, spansView, errorsView, putLatencyView,
		droppedView, drainSizeView, drainLengthView,
	}
}
