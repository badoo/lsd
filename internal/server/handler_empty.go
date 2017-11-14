package server

import (
	"badoo/_packages/gpbrpc"
	"github.com/badoo/lsd/proto"
)

// we can have client only configurations but we have to run some server
// because it's the only way to make a whole service work
type emptyHandler struct{}

func (es *emptyHandler) RequestNewEvents(rctx gpbrpc.RequestT, request *lsd.RequestNewEvents) gpbrpc.ResultT {
	return lsd.Gpbrpc.ErrorGeneric("No server config specified")
}

func (es *emptyHandler) Shutdown() {
}
