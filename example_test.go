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

package kinesis_test

import (
	"log"

	"go.opencensus.io/trace"

	"github.com/omnition/opencensus-go-exporter-kinesis"
)

func ExampleNewExporter_collector() {
	// Register the Kinesis exporter to be able to retrieve
	// the collected spans.
	exporter, err := kinesis.NewExporter(kinesis.Options{
		Process: kinesis.Process{
			ServiceName: "trace-demo",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	trace.RegisterExporter(exporter)
}

// ExampleNewExporter_processTags shows how to set ProcessTags
// on a Kinesis exporter. These tags will be added to the exported
// Kinesis process.
func ExampleNewExporter_processTags() {
	// Register the Kinesis exporter to be able to retrieve
	// the collected spans.
	exporter, err := kinesis.NewExporter(kinesis.Options{
		Process: kinesis.Process{
			ServiceName: "trace-demo",
			Tags: []kinesis.Tag{
				kinesis.StringTag("ip", "127.0.0.1"),
				kinesis.BoolTag("demo", true),
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	trace.RegisterExporter(exporter)
}
