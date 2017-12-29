package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

const (
	alive = iota
	dead
	restart
)

type clientView struct {
	viewNum   uint
	lostPings int
	state     int8
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	sync.RWMutex
	curView   View
	viewAcked bool

	clientViewMap map[string]*clientView
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.Lock()
	defer vs.Unlock()
	//log.Printf("Ping calling addr=%v...", args.Me)
	cv := vs.clientViewMap[args.Me]
	if cv == nil {
		cv = &clientView{}
		vs.clientViewMap[args.Me] = cv
		//	log.Printf("new client view=%v", args.Me)
	}
	cv.lostPings = 0
	cv.state = alive

	if args.Me == vs.curView.Primary {
		if args.Viewnum < cv.viewNum {
			cv.state = restart
		} else if !vs.viewAcked && args.Viewnum == vs.curView.Viewnum {
			vs.viewAcked = true
		}
	} else if args.Me == vs.curView.Backup {
		if args.Viewnum < cv.viewNum {
			cv.state = restart
		}
	}

	vs.proceedView()
	reply.View = vs.curView
	cv.viewNum = args.Viewnum

	return nil
}

func (vs *ViewServer) proceedView() {
	if !vs.viewAcked {
		return
	}
	//	log.Printf("old view=%+v", vs.curView)

	oldPrimary, oldBackup := vs.curView.Primary, vs.curView.Backup

	if vs.curView.Primary != "" {
		cv := vs.clientViewMap[vs.curView.Primary]
		if cv.state == dead || cv.state == restart {
			vs.curView.Primary = ""
		}
	}

	if vs.curView.Backup != "" {
		cv := vs.clientViewMap[vs.curView.Backup]
		if cv.state == dead || cv.state == restart {
			vs.curView.Backup = ""
		}
	}

	for addr, cv := range vs.clientViewMap {
		if cv.state == dead {
			delete(vs.clientViewMap, addr)
		} else if cv.state == restart {
			cv.state = alive
		}
	}

	if vs.curView.Primary == "" {
		if vs.curView.Backup != "" {
			vs.curView.Primary = vs.curView.Backup
			vs.curView.Backup = ""
		} else if vs.curView.Viewnum == 0 {
			for addr := range vs.clientViewMap {
				vs.curView.Primary = addr
				break
			}
		}
	}

	if vs.curView.Backup == "" {
		if vs.curView.Primary != "" {
			for addr := range vs.clientViewMap {
				if addr != vs.curView.Primary {
					vs.curView.Backup = addr
					break
				}
			}
		}
	}

	if oldPrimary != vs.curView.Primary || oldBackup != vs.curView.Backup {
		vs.viewAcked = false
		vs.curView.Viewnum++
	}

	//	log.Printf("proceedView curView=%+v", vs.curView)
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.RLock()
	defer vs.RUnlock()

	reply.View = vs.curView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.Lock()
	defer vs.Unlock()

	// Your code here.
	var lostClient bool
	for _, cv := range vs.clientViewMap {
		cv.lostPings++
		if cv.lostPings >= DeadPings {
			cv.state = dead
			lostClient = true
		}
	}

	if lostClient {
		vs.proceedView()
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.clientViewMap = make(map[string]*clientView)
	vs.viewAcked = true
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
