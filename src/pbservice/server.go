package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
	mu sync.Mutex
	cView *viewservice.View
	bConsistency int

	pReply *viewservice.PingReply
	pArgs *viewservice.PingArgs
	
	gReply *viewservice.GetReply
	gArgs *viewservice.GetArgs

	kvStore map[string]string
	rLog map[int64]string
}

func ForwardRequest(pb *PBServer,args *PutArgs, reply *PutReply){
	fargs:=*(args)
	fargs.Caller = pb.me
	
	freply:=&PutReply{PendingResponse,""}
	for freply.Err == PendingResponse{
		ok := call(pb.cView.Backup, "PBServer.Put", fargs, freply)
		//fmt.Println("Forward Request: " + reply.Err)
		//if forward fails then
		// a) dropped message
		// b) failed backup
		// first check with viewservice
		// if view changed backup failed
		// if backup failed then make consistent and forward request
		// if backup didn't fail then retrasmit message
		//

		if !ok || !(freply.Err== OK){
			pb.gReply.View,_ = pb.vs.Get()
			backup:= pb.gReply.View.Viewnum != pb.cView.Viewnum
			if backup{
				pb.bConsistency=1
				break;
			}else{
				freply.Err = PendingResponse
			}
		}
	}
}

func MakeConsistent(pb *PBServer){
	//if pb.bConsistency == 1 && backup && pb.cView.Primary == pb.me{
	pb.bConsistency = 0
	args:=&UpdateArgs{pb.kvStore,pb.rLog,pb.me}
	reply:=&UpdateReply{PendingResponse}
	//fmt.Print("Updating --> ")
	//fmt.Println(args.KVstore)

	attempts:=0
	for reply.Err == PendingResponse{//NEED TO RETRY IF UPDATE NOT POSSIBLE
		ok := call(pb.cView.Backup, "PBServer.Update", args, reply)
		if !ok || !(reply.Err == OK){
			reply.Err=PendingResponse
		}
		attempts++
	}
	//}
}

func (pb *PBServer) Update(args *UpdateArgs, reply *UpdateReply) error{
	//fmt.Println("Serving Update Request from (" + args.Caller + ") to (" + pb.me + ")")
	*(pb.cView),_=pb.vs.Get()
	pb.mu.Lock()
	//fmt.Print("Making Backup Consistent " + pb.me + " --> ")
	//fmt.Println(args.KVstore)

	if pb.bConsistency == 1 {
		pb.bConsistency = 0
		pb.kvStore = args.KVstore
		pb.rLog = args.RLog
		reply.Err = OK
		pb.mu.Unlock()
		return nil
	}else if pb.bConsistency == 0{
		reply.Err=OK
	}

	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	pb.mu.Lock()
	primary:=(pb.me == pb.cView.Primary)
	caller:=(pb.cView.Primary==args.Caller)
	//backup:=(pb.me == pb.cView.Backup)
	
	if !primary && !caller {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return nil
	}

	//fmt.Print("Serving PUT Request (")
	//fmt.Print(args.Rid)
	//fmt.Println(") in: (" + pb.me + ") having key: [" + args.Key + " , " + args.Value+"]")

	/*if caller && backup{
		reply.Err = OK
		//reply.PreviousValue = oVal
		pb.kvStore[args.Key] = args.Value
		pb.mu.Unlock()
		return nil
	}*/

	rval,rid:=pb.rLog[args.Rid]
	oVal,exists:=pb.kvStore[args.Key]
	nVal:=args.Value
	if rid{//If duplicate request then return old val and OK
		reply.Err=OK
		reply.PreviousValue=rval
	}else if exists{//if exists update, log reguest return old val and OK
		args.Value = nVal
		if(pb.cView.Backup!="" && pb.me == pb.cView.Primary && pb.bConsistency == 0){//Primary Only Forwards Requests if Backup exists and Consistency has been resolved
			ForwardRequest(pb,args,reply)
		}

		if args.DoHash{
			//fmt.Println("Update Server ("+pb.me+") Concat: " + (oVal + nVal) + " hashed " + strconv.Itoa(int(hash(oVal + nVal))))
			nVal  = oVal + nVal
			nVal = strconv.Itoa(int(hash(nVal)))
		}

		reply.Err = OK
		reply.PreviousValue = oVal
		pb.kvStore[args.Key] = nVal
		pb.rLog[args.Rid] = reply.PreviousValue
	}else{//if does not exists insert value, log reguest and return empty val and ErrKey
		oVal = ""
		args.Value = nVal
		if(pb.cView.Backup!="" && pb.me == pb.cView.Primary && pb.bConsistency == 0){//Primary Only Forwards Requests if Backup exists and Consistency has been resolved
			ForwardRequest(pb,args,reply)
		}
		
		if args.DoHash{
			//fmt.Println("Insert Server ("+pb.me+") Concat: " + (oVal + nVal) + " hashed " + strconv.Itoa(int(hash(oVal + nVal))))
			nVal  = oVal + nVal
			nVal = strconv.Itoa(int(hash(nVal)))
		}

		reply.Err = ErrNoKey
		reply.PreviousValue = oVal
		pb.kvStore[args.Key] = nVal
		pb.rLog[args.Rid] = reply.PreviousValue
	}

	pb.mu.Unlock()

  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	primary:=(pb.me == pb.cView.Primary)
	//fmt.Println("Serving GET Request in: " + pb.me)
	//fmt.Println(pb.kvStore)
	if !primary {
		reply.Err = ErrWrongServer
		return nil
	}
	
	key:=args.Key
	pb.mu.Lock()
	val,exists:=pb.kvStore[key]
	if exists{
		reply.Err = OK
		reply.Value = val
	}else{
		reply.Err = ErrNoKey
		reply.Value = val
	}
	pb.mu.Unlock()
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
	//if dead stop

	//Ping ViewService
	pb.pReply.View,_=pb.vs.Ping(pb.cView.Viewnum)
	//primary:= pb.pReply.View.Primary != pb.cView.Primary
	backup:= pb.pReply.View.Backup != pb.cView.Backup
	nobackup:= pb.pReply.View.Backup == ""
	//promotion:= (pb.pReply.View.Primary == pb.cView.Backup)
	*(pb.cView) = pb.pReply.View

	//Primary needs to be consistent with backup
	if pb.bConsistency == -1 && (pb.pReply.View.Primary == pb.me || pb.pReply.View.Backup == pb.me){
		pb.bConsistency = 1
	}else if backup{
		pb.bConsistency = 1
	}

	//Do not promote backup if consisteny has not been resolved
	//if pb.bConsistency == 1 && promotion{
	//	fmt.Println("Demoting Primary")
	//	pb.cView.Primary = ""
	//}
	
	//Try to update backup when it comes online
	if pb.bConsistency == 1 && pb.cView.Primary == pb.me && !nobackup{
		pb.mu.Lock()
		MakeConsistent(pb)
		pb.mu.Unlock()
	}
	
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.

	pb.cView=&viewservice.View{0,"",""}
	pb.bConsistency = -1

	pb.pReply=&viewservice.PingReply{*(pb.cView)}
	pb.pArgs=&viewservice.PingArgs{pb.me,0}

	pb.gReply=&viewservice.GetReply{*(pb.cView)}
	pb.gArgs=&viewservice.GetArgs{}
	
	pb.kvStore=make(map[string]string)
	pb.rLog=make(map[int64]string)
	

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
