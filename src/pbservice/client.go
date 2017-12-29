package pbservice

import (
	"crypto/rand"
	"log"
	"math/big"
	"net/rpc"
	"time"
	"viewservice"
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	curView viewservice.View
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Printf("call rpcname=%v, err=%v\n", rpcname, err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	if !ck.ensureView() {
		return ""
	}

RETRY:
	var reply GetReply
	success := call(ck.curView.Primary, "PBServer.Get", &GetArgs{Key: key}, &reply)
	if !success {
		time.Sleep(viewservice.PingInterval)
		ck.curView.Primary = ""
		if !ck.ensureView() {
			return ""
		}

		goto RETRY
	}
	if reply.Err == ErrViewChanged {
		v, b := ck.vs.Get()
		if !b {
			log.Printf("get view service failed")
			return ""
		}
		ck.curView = v
		goto RETRY
	} else if reply.Err == OK {
		return reply.Value
	} else {
		log.Printf("call PBServer.Get Reply error:%v", reply.Err)
		return ""
	}
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	if !ck.ensureView() {
		return
	}
	opId := nrand()

RETRY:
	var reply PutAppendReply
	args := &PutAppendArgs{
		Key:    key,
		Value:  value,
		OpType: op,
		OpId:   opId,
	}
	success := call(ck.curView.Primary, "PBServer.PutAppend", args, &reply)
	if !success {
		time.Sleep(viewservice.PingInterval)
		ck.curView.Primary = ""
		if !ck.ensureView() {
			return
		}
		goto RETRY
	}
	if reply.Err == ErrViewChanged {
		v, b := ck.vs.Get()
		if !b {
			log.Printf("get view service failed")
			return
		}
		ck.curView = v
		goto RETRY
	} else if reply.Err != OK && reply.Err != ErrDuplicateOp {
		log.Printf("call PBServer.PutAppend Reply error:%v", reply.Err)
	}
}

func (ck *Clerk) ensureView() bool {
	for ck.curView.Primary == "" {

		v, b := ck.vs.Get()
		if !b {
			log.Printf("get view service failed")
			return false
		}
		ck.curView = v
		if ck.curView.Primary != "" {
			return true
		}
		time.Sleep(viewservice.PingInterval)
	}
	return true
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
