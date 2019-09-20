PROTO_PACKAGE_PATH?=./models/gen/

.PHONY: generate-protobuf
generate-protobuf:
	docker run --rm -v $(PWD):$(PWD) -w $(PWD) znly/protoc --gofast_out=$(PROTO_PACKAGE_PATH) -Iidl idl/*.proto
