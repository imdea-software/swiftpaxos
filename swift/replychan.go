package swift

import (
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

type replyArgs struct {
	dep     Dep
	hs      []SHash
	val     state.Value
	cmdId   CommandId
	finish  chan interface{}
	propose *defs.GPropose
}

type replyChan struct {
	rep  *defs.ProposeReplyTS
	ok   chan struct{}
	exit chan struct{}
	args chan *replyArgs
}

func NewReplyChan(r *Replica) *replyChan {
	rc := &replyChan{
		rep: &defs.ProposeReplyTS{
			OK: defs.TRUE,
		},
		ok:   make(chan struct{}, 1),
		exit: make(chan struct{}, 2),
		args: make(chan *replyArgs, defs.CHAN_BUFFER_SIZE),
	}

	go func() {
		for !r.Shutdown {
			select {
			case <-rc.exit:
				rc.ok <- struct{}{}
				return
			case args := <-rc.args:
				if args.propose.Proxy && !r.optExec {
					acc := &MAccept{
						Replica: r.Id,
						Ballot:  r.ballot,
						CmdId:   args.cmdId,
						Rep:     args.val,
					}
					r.sender.SendToClient(args.propose.ClientId, acc, r.cs.acceptRPC)
				} else if r.optExec && r.Id == r.leader() {
					reply := &MReply{
						Replica:  r.Id,
						Ballot:   r.ballot,
						CmdId:    args.cmdId,
						Checksum: args.hs,
						Rep:      args.val,
					}
					r.sender.SendToClient(args.propose.ClientId, reply, r.cs.replyRPC)
				} else if args.propose.Proxy && r.optExec {
					// TODO: make replicas send replies when r.optExec
					// acc := &MAccept{
					// 	Replica: r.Id,
					// 	Ballot:  r.ballot,
					// 	CmdId:   args.cmdId,
					// 	Rep:     args.val,
					// }
					// r.sender.SendToClient(args.propose.ClientId, acc, r.cs.acceptRPC)
				}
				// TODO: what if it is optimistically executed by the leader?
				r.historySize = (r.historySize % HISTORY_SIZE) + 1
				args.finish <- (r.historySize - 1)
			}
		}
	}()

	return rc
}

func (r *replyChan) stop() {
	r.exit <- struct{}{}
	<-r.ok
}

func (r *replyChan) reply(desc *commandDesc, cmdId CommandId, val state.Value) {
	dep := make([]CommandId, len(desc.dep))
	copy(dep, desc.dep)

	hs := make([]SHash, len(desc.hs))
	copy(hs, desc.hs)

	r.args <- &replyArgs{
		dep:     dep,
		hs:      hs,
		val:     val,
		cmdId:   cmdId,
		finish:  desc.msgs,
		propose: desc.propose,
	}
}
