# based on goprotobuf (https://code.google.com/p/goprotobuf/) Makefile

all: install

install:
	$(GO) install ./protoc-gen-gpbrpc-go
	$(GO) install .

test:
	$(GO) test ./protoc-gen-gpbrpc-go
	$(GO) test .

clean:
	$(GO) clean ./...

nuke:
	$(GO) clean -i ./...
