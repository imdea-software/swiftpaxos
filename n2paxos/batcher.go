package n2paxos

import "github.com/imdea-software/swiftpaxos/rpc"

type Batcher struct {
	twoAs chan BatcherOp
	twoBs chan BatcherOp
}

type BatcherOp struct {
	cid          int32
	msg          rpc.Serializable
	sendToClient bool
}

func NewBatcher(r *Replica, size int) *Batcher {
	b := &Batcher{
		twoAs: make(chan BatcherOp, size),
		twoBs: make(chan BatcherOp, size),
	}

	go func() {
		for !r.Shutdown {
			select {
			case op := <-b.twoAs:
				twoA := op.msg.(*M2A)

				aLen := len(b.twoAs) + 1
				bLen := len(b.twoBs)
				twos := &M2s{
					TwoAs: make([]M2A, aLen),
					TwoBs: make([]M2B, bLen),
				}

				twos.TwoAs[0] = *twoA
				for i := 1; i < aLen; i++ {
					opP := <-b.twoAs
					twoA := opP.msg.(*M2A)
					twos.TwoAs[i] = *twoA
				}
				for i := 0; i < bLen; i++ {
					opP := <-b.twoBs
					twoB := opP.msg.(*M2B)
					twos.TwoBs[i] = *twoB
				}

				r.sender.SendToAll(twos, r.cs.twosRPC)
				if op.sendToClient {
					r.sender.SendToClient(op.cid, twos, r.cs.twosRPC)
				}

			case op := <-b.twoBs:
				twoB := op.msg.(*M2B)

				aLen := len(b.twoAs)
				bLen := len(b.twoBs) + 1
				twos := &M2s{
					TwoAs: make([]M2A, aLen),
					TwoBs: make([]M2B, bLen),
				}

				for i := 0; i < aLen; i++ {
					opP := <-b.twoAs
					twoA := opP.msg.(*M2A)
					twos.TwoAs[i] = *twoA
				}
				twos.TwoBs[0] = *twoB
				for i := 1; i < bLen; i++ {
					opP := <-b.twoBs
					twoB := opP.msg.(*M2B)
					twos.TwoBs[i] = *twoB
				}

				r.sender.SendToAll(twos, r.cs.twosRPC)
				if op.sendToClient {
					r.sender.SendToClient(op.cid, twos, r.cs.twosRPC)
				}
			}
		}
	}()

	return b
}

func (b *Batcher) Send2A(twoA *M2A) {
	b.twoAs <- BatcherOp{
		msg:          twoA,
		sendToClient: false,
	}
}

func (b *Batcher) Send2B(twoB *M2B) {
	b.twoBs <- BatcherOp{
		msg:          twoB,
		sendToClient: false,
	}
}

func (b *Batcher) Send2AClient(twoA *M2A, cid int32) {
	b.twoAs <- BatcherOp{
		msg:          twoA,
		cid:          cid,
		sendToClient: true,
	}
}

func (b *Batcher) Send2BClient(twoB *M2B, cid int32) {
	b.twoBs <- BatcherOp{
		msg:          twoB,
		cid:          cid,
		sendToClient: true,
	}
}
