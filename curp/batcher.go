package curp

import "github.com/imdea-software/swiftpaxos/rpc"

type Batcher struct {
	acks chan rpc.Serializable
	accs chan rpc.Serializable
}

func NewBatcher(r *Replica, size int) *Batcher {
	b := &Batcher{
		acks: make(chan rpc.Serializable, size),
		accs: make(chan rpc.Serializable, size),
	}

	go func() {
		for !r.Shutdown {
			select {
			case op := <-b.acks:
				l1 := len(b.acks) + 1
				l2 := len(b.accs)
				aacks := &MAAcks{
					Acks:    make([]MAcceptAck, l1),
					Accepts: make([]MAccept, l2),
				}
				for i := 0; i < l1; i++ {
					aacks.Acks[i] = *op.(*MAcceptAck)
					if i < l1-1 {
						op = <-b.acks
					}
				}
				for i := 0; i < l2; i++ {
					op = <-b.accs
					aacks.Accepts[i] = *op.(*MAccept)
				}
				r.sender.SendToAll(aacks, r.cs.aacksRPC)

			case op := <-b.accs:
				l1 := len(b.acks)
				l2 := len(b.accs) + 1
				aacks := &MAAcks{
					Acks:    make([]MAcceptAck, l1),
					Accepts: make([]MAccept, l2),
				}
				for i := 0; i < l2; i++ {
					aacks.Accepts[i] = *op.(*MAccept)
					if i < l2-1 {
						op = <-b.accs
					}
				}
				for i := 0; i < l1; i++ {
					op = <-b.acks
					aacks.Acks[i] = *op.(*MAcceptAck)
				}
				r.sender.SendToAll(aacks, r.cs.aacksRPC)
			}
		}
	}()

	return b
}

func (b *Batcher) SendAccept(a *MAccept) {
	b.accs <- a
}

func (b *Batcher) SendAcceptAck(a *MAcceptAck) {
	b.acks <- a
}
