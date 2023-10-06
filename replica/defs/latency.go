package defs

import (
	"bufio"
	"net"
	"os"
	"strings"
	"time"
)

var LatencyConf = ""

type LatencyTable struct {
	ti map[int]time.Duration
	tn map[string]time.Duration
	d  time.Duration
}

type DelayProposeChan struct {
	delay time.Duration
	pChan chan *GPropose
	lChan chan logCall
}

type bstNode struct {
	p     *GPropose
	left  *bstNode
	right *bstNode
	ready bool
}

type logCall struct {
	action int
	arg    *GPropose
}

const (
	LOG_INSERT = iota
	LOG_HANDLE
)

func (root *bstNode) insert(p *GPropose) *bstNode {
	if root == nil {
		return &bstNode{
			p: p,
		}
	}
	if j := root.p.CommandId; p.CommandId < j {
		root.left = root.left.insert(p)
	} else if p.CommandId > j {
		root.right = root.right.insert(p)
	}

	return root
}

func (root *bstNode) find(i int32) *bstNode {
	if root == nil {
		return nil
	}
	if j := root.p.CommandId; j == i {
		return root
	} else if j < i {
		return root.right.find(i)
	}
	return root.left.find(i)
}

// call `handle` on each continuous element starting from `min`
// returns new root and the id of the last handled proposal
func (root *bstNode) inorderReady(min int32, handle func(*GPropose)) (*bstNode, int32) {
	if root == nil {
		return nil, min
	}

	l, i := root.left.inorderReady(min, handle)

	if l == nil {
		root.left = nil
	}

	if !root.ready || i != root.p.CommandId-1 {
		return root, i
	}
	handle(root.p)
	r, j := root.right.inorderReady(i+1, handle)
	return r, j
}

func NewDelayProposeChan(d time.Duration, c chan *GPropose) *DelayProposeChan {
	if d == time.Duration(0) {
		return &DelayProposeChan{
			delay: d,
			pChan: c,
		}
	}

	dc := &DelayProposeChan{
		delay: d,
		pChan: c,
		lChan: make(chan logCall),
	}

	var (
		log *bstNode
		min int32 = -1
	)

	go func() {
		for {
			a := <-dc.lChan
			switch a.action {
			case LOG_INSERT:
				log = log.insert(a.arg)
			case LOG_HANDLE:
				n := log.find(a.arg.CommandId)
				if n == nil {
					log = log.insert(a.arg)
					n = log.find(a.arg.CommandId)
				}
				n.ready = true
				log, min = log.inorderReady(min, func(p *GPropose) {
					dc.pChan <- p
				})
			}
		}
	}()

	return dc
}

func (c *DelayProposeChan) Write(p *GPropose) {
	if c.delay == time.Duration(0) {
		c.pChan <- p
		return
	}

	c.lChan <- logCall{
		action: LOG_INSERT,
		arg:    p,
	}
	go func() {
		time.Sleep(c.delay)
		c.lChan <- logCall{
			action: LOG_HANDLE,
			arg:    p,
		}
	}()
}

func NewLatencyTable(conf, myAddr string, addrs []string) *LatencyTable {
	if conf == "" {
		return nil
	}
	f, err := os.Open(conf)
	if err != nil {
		return nil
	}
	defer f.Close()

	myAddr = strings.Split(myAddr, ":")[0]

	dt := &LatencyTable{
		d: time.Duration(0),
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		data := strings.Split(s.Text(), " ")
		if len(data) == 2 {
			if data[0] == "uniform" {
				if d, err := time.ParseDuration(data[1]); err == nil {
					return &LatencyTable{
						d: time.Duration(int64(d) / int64(2)),
					}
				} else {
					return nil
				}
			}
		}
		if len(data) == 3 {
			d, err := time.ParseDuration(data[2])
			if err != nil {
				continue
			}
			d = time.Duration(int64(d) / int64(2))
			addr1, addr2 := data[0], data[1]
			for i := 0; i < 2; i++ {
				if myAddr != addr1 {
					addr1, addr2 = data[1], data[0]
					continue
				}
				if dt.tn == nil {
					dt.tn = make(map[string]time.Duration)
				}
				dt.tn[addr2] = d
				for rid, addr := range addrs {
					if addr2 == strings.Split(addr, ":")[0] {
						if dt.ti == nil {
							dt.ti = make(map[int]time.Duration)
						}
						dt.ti[rid] = d
					}
				}
			}
		}
	}

	return dt
}

func (dt *LatencyTable) WaitDuration(addr string) time.Duration {
	if dt == nil {
		return time.Duration(0)
	}
	d, exists := dt.tn[strings.Split(addr, ":")[0]]
	if exists {
		return d
	}
	return dt.d
}

func (dt *LatencyTable) WaitDurationID(id int) time.Duration {
	if dt == nil {
		return time.Duration(0)
	}
	d, exists := dt.ti[id]
	if exists {
		return d
	}
	return dt.d
}

func IP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
