init:
	go get github.com/micro/protobuf/{proto,protoc-gen-go}
	go get -u github.com/golang/dep/cmd/dep
	protoc -I$(GOPATH)/src --go_out=plugins=micro:$(GOPATH)/src \
    	$(GOPATH)/src/github.com/murphybytes/gots/api/gots.proto
	dep ensure

test:
	go test -v -cover ./...

test-race:
	go test -race ./...