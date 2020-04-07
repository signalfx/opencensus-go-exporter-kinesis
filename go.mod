module github.com/signalfx/opencensus-go-exporter-kinesis

require (
	github.com/aws/aws-sdk-go v1.16.26
	github.com/brianvoe/gofakeit v3.17.0+incompatible
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/jaegertracing/jaeger v1.15.1
	github.com/signalfx/golib/v3 v3.3.0
	github.com/signalfx/omnition-kinesis-producer v0.5.0
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.2
	go.uber.org/zap v1.9.1
)

go 1.12

replace git.apache.org/thrift.git => github.com/apache/thrift v0.12.0
