package pbservice

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	sync.RWMutex
	curView viewservice.View
	data    map[string]string

	doneOpIdMap map[int64]bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.RLock()
	defer pb.RUnlock()

	// Your code here.
	if pb.me != pb.curView.Primary {
		reply.Err = ErrViewChanged
		return nil
	}
	//	log.Printf("%+v %+v", pb.curView, pb.data)

	if pb.curView.Backup == "" {
		reply.Err = OK
		reply.Value = pb.data[args.Key]
		return nil
	}
	args.Primary = pb.me

	success := call(pb.curView.Backup, "PBServer.HandleSyncGet", args, reply)
	if !success {
		return fmt.Errorf("call failed")
	}

	if reply.Err != OK {
		return nil
	}

	if reply.Value != pb.data[args.Key] {
		log.Printf("primary's mismatch backup's data")
		return nil
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.Lock()
	defer pb.Unlock()

	// Your code here.
	if pb.doneOpIdMap[args.OpId] {
		reply.Err = ErrDuplicateOp
		return nil
	}

	if pb.me != pb.curView.Primary {
		reply.Err = ErrViewChanged
		return nil
	}

	if pb.curView.Backup != "" {
		args.Primary = pb.me
		success := call(pb.curView.Backup, "PBServer.HandleSyncPutAppend", args, reply)
		if !success {
			return fmt.Errorf("call failed")
		}

		if reply.Err != OK && reply.Err != ErrDuplicateOp { // wait tick to update view
			return nil
		}
	} else {
		reply.Err = OK
	}

	if args.OpType == "Put" {
		pb.data[args.Key] = args.Value
	} else {
		pb.data[args.Key] = pb.data[args.Key] + args.Value
	}
	//	log.Printf("%+v %+v", *args, pb.data)
	pb.doneOpIdMap[args.OpId] = true

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.Lock()
	defer pb.Unlock()

	// Your code here.
	newView, err := pb.vs.Ping(pb.curView.Viewnum)
	if err != nil {
		log.Printf("ping view service error:%v", err)
		return
	}

	if newView.Viewnum == pb.curView.Viewnum { // no change
		return
	}

	if newView.Primary != "" && newView.Primary == pb.me {
		if newView.Backup != "" && pb.curView.Backup != newView.Backup { // backup changed
			err := pb.clone(newView.Viewnum, newView.Backup)
			if err != nil { // backup not ready
				return
			}
		}
	}

	pb.curView = newView
}

func (pb *PBServer) clone(viewnum uint, backup string) error {
	// prepare the arguments.
	args := &CloneArgs{
		Me:          pb.me,
		AllData:     pb.data,
		DoneOpIdMap: pb.doneOpIdMap,
		Viewnum:     viewnum,
	}
	var reply CloneReply

	// send an RPC request, wait for the reply.
	ok := call(backup, "PBServer.HandleClone", args, &reply)
	if ok == false {
		return fmt.Errorf("HandleClone(%v) failed", viewnum)
	}

	if reply.Err == OK {
		return nil
	} else {
		return errors.New(string(reply.Err))
	}
}

func (pb *PBServer) HandleClone(args *CloneArgs, reply *CloneReply) error {
	pb.Lock()
	defer pb.Unlock()

	if args.Viewnum != pb.curView.Viewnum {
		reply.Err = ErrMismatchView
		return nil
	}
	pb.data = args.AllData
	pb.doneOpIdMap = args.DoneOpIdMap
	reply.Err = OK
	return nil
}

func (pb *PBServer) HandleSyncPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.Lock()
	defer pb.Unlock()

	if pb.doneOpIdMap[args.OpId] {
		reply.Err = ErrDuplicateOp
		return nil
	}

	if pb.curView.Backup != pb.me || pb.curView.Primary != args.Primary {
		reply.Err = ErrViewChanged
		return nil
	}

	if args.OpType == "Put" {
		pb.data[args.Key] = args.Value
	} else {
		pb.data[args.Key] = pb.data[args.Key] + args.Value
	}
	pb.doneOpIdMap[args.OpId] = true
	reply.Err = OK
	return nil

}

func (pb *PBServer) HandleSyncGet(args *GetArgs, reply *GetReply) error {
	pb.Lock()
	defer pb.Unlock()
	if pb.curView.Backup != pb.me || pb.curView.Primary != args.Primary {
		reply.Err = ErrViewChanged
		return nil
	}

	reply.Value = pb.data[args.Key]
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.doneOpIdMap = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
