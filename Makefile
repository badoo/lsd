include $(GOPATH)/src/badoo/_scripts/default_targets.mk

DAEMON = lsd
VERSION = 1.13.0
MAINTAINER = a.denisov@corp.badoo.com

daemon: protofiles
	time $(GO) install -v

protofiles: $(PROTO_DIR)/*.proto
	$(BADOO_SCRIPTS)/build_protofile.sh "$(PROTOC)" "gogo_out" $(PROTO_DIR)/lsd-config.proto .
	$(BADOO_SCRIPTS)/build_protofile.sh "$(PROTOC)" "gogo_out gpbrpc-go_out" $(PROTO_DIR)/lsd.proto .
