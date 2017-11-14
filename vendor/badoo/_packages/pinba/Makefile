all: protofiles
	$(GO) install

protofiles: proto/pinba.proto
	$(BADOO_SCRIPTS)/build_protofile.sh "$(PROTOC)" "gogo_out" proto/pinba.proto .

clean:
	find . -name '*.pb.go' -delete
	find . -name '*.gpbrpc.go' -delete
