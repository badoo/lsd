package gpbrpc

type pcmRequest struct {
	address string
	reply   chan Client
}

type pcmType struct {
	reused  chan Client
	getconn chan pcmRequest
	conns   map[string][]Client
}

type PcmType interface {
	GetClient(address string) Client
	Reuse(client Client)
	StartServing()
}

var Pcm PcmType

func NewPcm() *pcmType {
	// singleton, hah?
	return &pcmType{
		reused:  make(chan Client, 16),
		getconn: make(chan pcmRequest, 16),
		conns:   make(map[string][]Client),
	}
}

func (pcm *pcmType) GetClient(address string) Client {
	reply := make(chan Client)
	/* RPC within one object between different goroutines */
	pcm.getconn <- pcmRequest{address: address, reply: reply}
	return <-reply
}

func (pcm *pcmType) Reuse(client Client) {
	pcm.reused <- client
}

func (pcm *pcmType) fetch(address string) Client {
	p := pcm.conns[address]
	if len(p) == 0 {
		return nil
	}

	client := p[0]
	p = p[1:]
	pcm.conns[address] = p

	return client
}

func (pcm *pcmType) store(client Client) {
	p := pcm.conns[client.Address()]
	p = append(p, client)
	pcm.conns[client.Address()] = p
}

func (pcm *pcmType) StartServing() {
	for {
		select {
		case r := <-pcm.reused:
			pcm.store(r)
		case req := <-pcm.getconn:
			req.reply <- pcm.fetch(req.address)
		}
	}
}

func init() {
	/* singleton, because it is a global cache by definition */
	Pcm = NewPcm()
	go Pcm.StartServing()
}
