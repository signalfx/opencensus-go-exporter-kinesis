PROTO_PACKAGE_PATH?=./internal/gen-go/jaegerpb

.PHONY: generate-protobuf
generate-protobuf:
	mkdir -p $(PROTO_PACKAGE_PATH)
	docker run --rm -v $(PWD):$(PWD) -w $(PWD) znly/protoc --go_out=$(PROTO_PACKAGE_PATH) -Iidl idl/model.proto
