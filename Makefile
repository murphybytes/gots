generate:
	go get github.com/micro/protobuf/{proto,protoc-gen-go}
	protoc -I$(GOPATH)/src --go_out=plugins=micro:$(GOPATH)/src \
    	$(GOPATH)/src/github.com/murphybytes/gots/api/gots.proto