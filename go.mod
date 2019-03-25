module github.com/omnition/opencensus-go-exporter-kinesis

require (
	github.com/aws/aws-sdk-go v1.16.26
	github.com/gogo/googleapis v1.1.0 // indirect
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.8.5 // indirect
	github.com/jaegertracing/jaeger v1.8.2
	github.com/omnition/kinesis-producer v0.4.4
	github.com/opentracing/opentracing-go v1.0.2 // indirect
	github.com/pkg/errors v0.8.1 // indirect
	go.opencensus.io v0.19.1
	go.uber.org/zap v1.9.1
)

// replace github.com/omnition/kinesis-producer => ../kinesis-producer
