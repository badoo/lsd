all: config/config.pb.go stats/stats.pb.go stats/stats.gpbrpc.go
	$(GO) install

config/config.pb.go: config/config.proto
	$(PROTOC) --gogo_out=. config/config.proto

stats/stats.pb.go: stats/stats.proto
	$(PROTOC) --gogo_out=. stats/stats.proto

stats/stats.gpbrpc.go: stats/stats.proto
	$(PROTOC) --gpbrpc-go_out=. stats/stats.proto

clean:
	find . -name '*.pb.go' -delete
	find . -name '*.gpbrpc.go' -delete
