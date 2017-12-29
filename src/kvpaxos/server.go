package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Args interface{}
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	doneOpIdMap map[int64]bool //op id -> cur value
	kvMap       map[string]string
	doneSeq     int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	for {
		doneSeq := kv.executeDecidedOp()

		kv.mu.Lock()
		if kv.doneOpIdMap[args.Id] {
			reply.Err = OK
			reply.Value = kv.kvMap[args.Key]
			kv.mu.Unlock()
			return nil
		}
		kv.mu.Unlock()
		kv.doAgreement(Op{*args}, doneSeq+1)
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	for {
		doneSeq := kv.executeDecidedOp()

		kv.mu.Lock()
		if _, exist := kv.doneOpIdMap[args.Id]; exist {
			reply.Err = OK
			kv.mu.Unlock()
			return nil
		}
		kv.mu.Unlock()
		kv.doAgreement(Op{*args}, doneSeq+1)
	}

	return nil
}

func (kv *KVPaxos) doAgreement(op Op, seq int) {
	kv.px.Start(seq, op)

	waitTime := 10 * time.Millisecond
	for {
		status, _ := kv.px.Status(seq)
		if status == paxos.Decided || status == paxos.Forgotten {
			break
		}
		time.Sleep(waitTime)
		if waitTime < 10*time.Second {
			waitTime *= 2
		}
	}
}

func (kv *KVPaxos) executeDecidedOp() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for i := kv.doneSeq + 1; i <= kv.px.Max(); i++ {
		status, value := kv.px.Status(i)
		if status != paxos.Decided {
			break
		}
		opValue := value.(Op)
		switch args := opValue.Args.(type) {
		case PutAppendArgs:
			if _, exist := kv.doneOpIdMap[args.Id]; exist {
				break
			}
			if args.Op == "Append" {
				kv.kvMap[args.Key] = kv.kvMap[args.Key] + args.Value
			} else {
				kv.kvMap[args.Key] = args.Value
			}
			kv.doneOpIdMap[args.Id] = true
		case GetArgs:
			if _, exist := kv.doneOpIdMap[args.Id]; exist {
				break
			}
			kv.doneOpIdMap[args.Id] = true
		}
		kv.doneSeq++
	}
	kv.px.Done(kv.doneSeq)
	return kv.doneSeq
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)
	kv.doneSeq = -1
	kv.doneOpIdMap = make(map[int64]bool)
	kv.kvMap = make(map[string]string)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
