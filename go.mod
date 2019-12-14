module github.com/signalfx/opencensus-go-exporter-kinesis

require (
	github.com/aws/aws-sdk-go v1.16.26
	github.com/brianvoe/gofakeit v3.17.0+incompatible
	github.com/gogo/googleapis v1.1.0 // indirect
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.8.5 // indirect
	github.com/jaegertracing/jaeger v1.8.2
	github.com/opentracing/opentracing-go v1.1.0 // indirect
	github.com/signalfx/omnition-kinesis-producer v0.4.6
	go.opencensus.io v0.19.1
	go.uber.org/zap v1.9.1
	golang.org/x/sys v0.0.0-20190502175342-a43fa875dd82 // indirect
	golang.org/x/text v0.3.2 // indirect
)

go 1.12

replace git.apache.org/thrift.git => github.com/apache/thrift v0.12.0
