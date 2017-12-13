init:
	go get github.com/micro/protobuf/{proto,protoc-gen-go}
	$(MAKE) -C api
	go get -u github.com/golang/dep/cmd/dep
	dep ensure

# --go_out=plugins=micro:$(GOPATH)/src

test:
	go test -v -cover ./...

test-race:
	go test -race ./...

pre:
	mkdir -p build

build-gots: pre
	go build -o build/gots github.com/murphybytes/gots/cmd/gots

build-publisher: pre
	go build -o build/pub github.com/murphybytes/gots/cmd/test/publisher

build: build-gots build-publisher

.PHONY: pre