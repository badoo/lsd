# based on goprotobuf (https://code.google.com/p/goprotobuf/) Makefile

WITH_ZMQ ?=

all: install

install:
	if [ ! "x$(WITH_ZMQ)" = "x" ]; then \
		CGO_CFLAGS="-I$(WITH_ZMQ)/include" \
		CGO_LDFLAGS="-static-libstdc++ -static-libgcc -lrt $(WITH_ZMQ)/lib/libzmq.a" \
			$(GO) install . ; \
	fi

test:
	$(GO) test .

clean:
	$(GO) clean ./...

nuke:
	$(GO) clean -i ./...
