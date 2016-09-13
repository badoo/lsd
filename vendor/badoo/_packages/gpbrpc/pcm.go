package gpbrpc

type pcmRequest struct {
	address string
	reply   chan *Client
}

type poolType struct {
	head, tail *Client
}

type pcmType struct {
	reused  chan *Client
	getconn chan *pcmRequest
	conns   map[string]*poolType
}

type PcmType interface {
	GetClient(address string) *Client
	Reuse(client *Client)
	StartServing()
}

var Pcm PcmType

func NewPcm() *pcmType {
	// singleton, hah?
	return &pcmType{
		reused:  make(chan *Client, 16),
		getconn: make(chan *pcmRequest, 16),
		conns:   make(map[string]*poolType),
	}
}

func (pcm *pcmType) GetClient(address string) *Client {
	reply := make(chan *Client)
	/* RPC within one object between different goroutines */
	pcm.getconn <- &pcmRequest{address: address, reply: reply}
	return <-reply
}

func (pcm *pcmType) Reuse(client *Client) {
	pcm.reused <- client
}

func (pcm *pcmType) fetch(address string) *Client {
	if p, ok := pcm.conns[address]; ok {
		ret := p.head
		if ret == nil {
			return nil
		}
		p.head = ret.next
		if ret.next != nil {
			ret.next.prev = nil
		} else {
			p.tail = nil
		}
		ret.prev = nil
		ret.next = nil
		return ret
	}
	return nil
}

func (pcm *pcmType) store(client *Client) {
	if _, ok := pcm.conns[client.address]; !ok {
		pcm.conns[client.address] = new(poolType)
	}
	p := pcm.conns[client.address]
	client.prev = nil
	client.next = p.head
	if p.head != nil {
		p.head.prev = client
	} else {
		p.tail = client
	}
	p.head = client
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
