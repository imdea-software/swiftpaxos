package client

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

type Client struct {
	*dlog.Logger

	ClientId  int32
	LeaderId  int
	ClosestId int // also co-located

	Ping []float64

	Fast       bool
	Verbose    bool
	Leaderless bool

	master  *rpc.Client
	servers []net.Conn
	readers []*bufio.Reader
	writers []*bufio.Writer

	dt         *defs.LatencyTable
	seqnum     int32
	server     string // co-located with
	masterPort int
	masterAddr string
	replicas   []string
}

func NewClient(server, maddr string, mport int, fast, leaderless, verbose bool) *Client {
	return NewClientLog(server, maddr, mport, fast, leaderless, verbose, nil)
}

func NewClientLog(server, maddr string, mport int, fast, leaderless, verbose bool, logger *dlog.Logger) *Client {
	if logger == nil {
		logger = dlog.New("", verbose)
	}

	return &Client{
		ClientId:  int32(uuid.New().ID()),
		LeaderId:  -1,
		ClosestId: -1,

		Fast:       fast,
		Verbose:    verbose,
		Leaderless: leaderless,

		Logger: logger,

		server:     server,
		masterPort: mport,
		masterAddr: maddr,

		seqnum: -1,
	}
}

func (c *Client) Connect() error {
	c.Println("dialing master...")
	_, err := c.dialMaster()
	if err != nil {
		return err
	}

	c.Println("getting list of replicas...")
	rl, err := c.callMaster("GetReplicaList")
	if err != nil {
		return err
	}
	masterReply := rl.(*defs.GetReplicaListReply)
	c.replicas = masterReply.ReplicaList

	c.Println("searching for the closest replica...")
	err = c.findClosest(masterReply.AliveList)
	if err != nil {
		return err
	}
	c.Println("replicas", c.replicas)
	c.Println("closest (alive)", c.ClosestId)

	c.dt = defs.NewLatencyTable(defs.LatencyConf, defs.IP(), c.replicas)

	N := len(c.replicas)
	c.servers = make([]net.Conn, N)
	c.readers = make([]*bufio.Reader, N)
	c.writers = make([]*bufio.Writer, N)

	if !c.Leaderless {
		c.Println("getting leader from master...")
		gl, err := c.callMaster("GetLeader")
		if err != nil {
			return err
		}
		masterReply := gl.(*defs.GetLeaderReply)
		c.LeaderId = masterReply.LeaderId
		c.Println("leader id", c.LeaderId)
	}

	connect := []int{}
	// Connect to all even if !c.Fast
	// this simplifies connection to a new leader when the old one is down
	for i := 0; i < N; i++ {
		if masterReply.AliveList[i] {
			connect = append(connect, i)
		}
	}

	for _, i := range connect {
		c.Println("connecting to", c.replicas[i])
		c.servers[i], err = c.dial(c.replicas[i], false)
		if err != nil {
			return err
		}
		c.readers[i] = bufio.NewReader(c.servers[i])
		c.writers[i] = bufio.NewWriter(c.servers[i])
	}

	return nil
}

func (c *Client) Disconnect() {
	for _, s := range c.servers {
		if s != nil {
			s.Close()
		}
	}
	if c.master != nil {
		c.master.Close()
		c.master = nil
	}
}

func (c *Client) Reconnect() error {
	c.Println("dialing master...")
	_, err := c.dialMaster()
	if err != nil {
		return err
	}

	if !c.Leaderless {
		c.Println("getting leader from master...")
		gl, err := c.callMaster("GetLeader")
		if err != nil {
			return err
		}
		masterReply := gl.(*defs.GetLeaderReply)
		c.LeaderId = masterReply.LeaderId
		c.Println("leader id", c.LeaderId)
	}

	return nil
}

func (c *Client) SendProposal(cmd defs.Propose) {
	d := c.LeaderId
	if c.Leaderless {
		d = c.ClosestId
	}

	if !c.Fast {
		c.Println("sending command", cmd.CommandId, "to", d)
		c.writers[d].WriteByte(defs.PROPOSE)
		cmd.Marshal(c.writers[d])
		c.writers[d].Flush()
	} else {
		c.Println("sending command", cmd.CommandId, "to everyone")
		for rep := 0; rep < len(c.servers); rep++ {
			if c.writers[rep] != nil {
				c.writers[rep].WriteByte(defs.PROPOSE)
				cmd.Marshal(c.writers[rep])
				c.writers[rep].Flush()
			}
		}
	}
}

func (c *Client) SendWrite(key int64, value []byte) int32 {
	c.seqnum++
	p := defs.Propose{
		CommandId: c.seqnum,
		ClientId:  c.ClientId,
		Command: state.Command{
			Op: state.PUT,
			K:  state.Key(key),
			V:  value,
		},
		Timestamp: 0,
	}

	c.SendProposal(p)
	return c.seqnum
}

func (c *Client) SendRead(key int64) int32 {
	c.seqnum++
	p := defs.Propose{
		CommandId: c.seqnum,
		ClientId:  c.ClientId,
		Command: state.Command{
			Op: state.GET,
			K:  state.Key(key),
			V:  state.NIL(),
		},
		Timestamp: 0,
	}

	c.SendProposal(p)
	return c.seqnum
}

func (c *Client) SendScan(key, count int64) int32 {
	c.seqnum++
	p := defs.Propose{
		CommandId: c.seqnum,
		ClientId:  c.ClientId,
		Command: state.Command{
			Op: state.SCAN,
			K:  state.Key(key),
			V:  make([]byte, 8),
		},
		Timestamp: 0,
	}

	binary.LittleEndian.PutUint64(p.Command.V, uint64(count))

	c.SendProposal(p)
	return c.seqnum
}

func (c *Client) GetReplyFrom(rid int) (*defs.ProposeReplyTS, error) {
	rep := &defs.ProposeReplyTS{}
	err := rep.Unmarshal(c.readers[rid])
	return rep, err
}

func (c *Client) RegisterRPCTable(t *fastrpc.Table) {
	for i, reader := range c.readers {
		go func(i int, reader *bufio.Reader) {
			for {
				var (
					msgType uint8
					err     error
				)
				if msgType, err = reader.ReadByte(); err != nil {
					break
				}
				p, exists := t.Get(msgType)
				if !exists {
					c.Println("error: received unknown message:", msgType)
					continue
				}
				obj := p.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				go func(obj fastrpc.Serializable) {
					time.Sleep(c.dt.WaitDuration(c.replicas[i]))
					p.Chan <- obj
				}(obj)
			}
		}(i, reader)
	}
}

// For custom client messages
func (c *Client) SendMsg(rid int32, code uint8, msg fastrpc.Serializable) {
	w := c.writers[rid]
	if w == nil {
		// TODO: return an error
		return
	}
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (c *Client) dial(addr string, connect bool) (net.Conn, error) {
	var (
		err  error    = nil
		conn net.Conn = nil
		resp *http.Response
	)

	for try := 0; try < 3; try++ {
		conn, err = net.DialTimeout("tcp", addr, 3*time.Second)
		if err == nil {
			if connect {
				io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
				resp, err = http.ReadResponse(bufio.NewReader(conn),
					&http.Request{
						Method: "CONNECT",
					})
				if err == nil && resp != nil && resp.Status == "200 Connected to Go RPC" {
					return conn, nil
				}
			} else {
				return conn, nil
			}
		} else {
			c.Println(addr, "connection error:", err)
		}
		if conn != nil {
			conn.Close()
		}
	}

	return nil, errors.New("cannot connect")
}

func (c *Client) dialMaster() (*rpc.Client, error) {
	if c.master != nil {
		c.master.Close()
	}
	addr := fmt.Sprintf("%s:%d", c.masterAddr, c.masterPort)
	conn, err := c.dial(addr, true)
	if err != nil {
		return nil, err
	}
	c.master = rpc.NewClient(conn)
	return c.master, nil
}

func (c *Client) findClosest(alive []bool) error {
	c.Println("pinging all replicas...")

	for i := 0; i < len(c.replicas); i++ {
		if !alive[i] {
			continue
		}
		addr := strings.Split(string(c.replicas[i]), ":")[0]
		if addr == "" {
			addr = "127.0.0.1"
		}

		if addr == c.server {
			c.ClosestId = i
		}

		out, err := exec.Command("ping", addr, "-c 3", "-q").Output()
		if err == nil {
			latency, _ := strconv.ParseFloat(strings.Split(string(out), "/")[4], 64)
			c.Println(i, "->", latency)
			c.Ping = append(c.Ping, latency)
		} else {
			c.Println(c.replicas[i], err)
			return err
		}
	}

	if c.ClosestId == -1 {
		min := math.MaxFloat64
		for i, l := range c.Ping {
			if l < min {
				min = l
				c.ClosestId = i
			}
		}
	}

	return nil
}

func (c *Client) call(r *rpc.Client, method string, args, reply interface{}) error {
	errs := make(chan error, 1)
	go func() {
		errs <- r.Call(method, args, reply)
	}()
	select {
	case err := <-errs:
		if err != nil {
			c.Println("error in RPC: " + method)
		}
		return err

	case <-time.After(3 * time.Second):
		c.Println("RPC timeout: " + method)
		return errors.New("RPC timeout")
	}
}

func (c *Client) callMaster(method string) (interface{}, error) {
	var (
		gl     *defs.GetLeaderReply
		rl     *defs.GetReplicaListReply
		glArgs *defs.GetLeaderArgs
		rlArgs *defs.GetReplicaListArgs
	)

	for i := 0; i < 100; i++ {
		if method == "GetReplicaList" {
			rl = &defs.GetReplicaListReply{}
			rlArgs = &defs.GetReplicaListArgs{}
			err := c.call(c.master, "Master."+method, rlArgs, rl)
			if err == nil && rl.Ready {
				return rl, nil
			}
		} else if method == "GetLeader" {
			gl = &defs.GetLeaderReply{}
			glArgs = &defs.GetLeaderArgs{}
			err := c.call(c.master, "Master."+method, glArgs, gl)
			if err == nil {
				return gl, nil
			}
		}
	}

	return nil, errors.New("too many call attempts!")
}
