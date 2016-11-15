
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "container/list"
//import "strconv"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
	cView *View
	servers map[string]time.Time// [server:url]Time from last ping

	pStatus int
	bStatus int
	
	idles *list.List
}

func HandleStatus(vs *ViewServer){
	//Initialize Actions//
	if vs.pStatus == -1 && vs.idles.Len() > 0{
		idle:= vs.idles.Front()
		vs.cView.Primary = idle.Value.(string)
		vs.pStatus = 1
		vs.idles.Remove(idle)
		//fmt.Println("Primary: "+ vs.cView.Primary)
	}

	if vs.bStatus == -1 && vs.idles.Len()>0 && vs.pStatus == 0{
		idle:=vs.idles.Front()
		vs.cView.Backup = idle.Value.(string)
		vs.bStatus = 1
		vs.idles.Remove(idle)
		//fmt.Println("Backup: "+ vs.cView.Backup)
	}

	//Increase viewnum if Primary ack and backup is born
	if vs.bStatus == 1 && vs.pStatus == 0{
		vs.cView.Viewnum++
		vs.bStatus = 0
		vs.pStatus = 1
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
	//fmt.Println("Ping ["+args.Me+"]<<<< " + vs.cView.Primary + " , " + vs.cView.Backup +" >>>>")
	vs.mu.Lock()
	_,exists:= vs.servers[args.Me]//Check if server is part of the configuration you already know
	vs.servers[args.Me] = time.Now()//Log server response time
	
	if !exists && vs.cView.Primary != args.Me && vs.cView.Backup!=args.Me{//Put idle servers in a list
		vs.idles.PushBack(args.Me)
		//fmt.Println("Registering Idle Server: "+args.Me)
	}

	if vs.pStatus == 1 && vs.cView.Primary == args.Me && vs.cView.Viewnum == args.Viewnum{// Wait for assigned primary to response
		vs.pStatus = 0
		//fmt.Println(">>>>>Primary Acked: " + vs.cView.Primary)
	}

	//Detect crash & restart
	if args.Me == vs.cView.Primary && args.Viewnum == 0{
		vs.pStatus = 1
		vs.cView.Primary = vs.cView.Backup
		vs.cView.Backup = ""
		//fmt.Println("Restarted Server: " + args.Me)
		delete(vs.servers,args.Me)
	}

	if args.Me == vs.cView.Backup && args.Viewnum == 0{
		vs.bStatus = -1
		vs.cView.Backup = ""
		delete(vs.servers,vs.cView.Backup)
	}

	HandleStatus(vs)

	//Reply to Ping
	reply.View = *(vs.cView)
	//fmt.Println("P("+vs.cView.Primary+ "," + vs.cView.Backup+")")
	
	vs.mu.Unlock()

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
	vs.mu.Lock()
	reply.View = *(vs.cView)
	//fmt.Println("Get [ ]<<<< " + vs.cView.Primary + " , " + vs.cView.Backup +" >>>>")
	//fmt.Println("<<<< " + strconv.Itoa(vs.pStatus) + " , " + strconv.Itoa(vs.bStatus) +" >>>>")
	vs.mu.Unlock()
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
	vs.mu.Lock()
	HandleStatus(vs)
	
	//Check for livegness//
	cTime:=time.Now()	
	duration:=DeadPings*PingInterval

	if vs.pStatus == 0{//Check Primary
		elapsed:=cTime.Sub(vs.servers[vs.cView.Primary])
		//fmt.Print("Elapsed: ")
		//fmt.Println(elapsed)
		if elapsed.Seconds() > duration.Seconds(){//Consider server dead and check if primary has a backup which you can rely on
			delete(vs.servers,vs.cView.Primary)
			vs.cView.Primary = vs.cView.Backup
			vs.cView.Backup = ""
			vs.bStatus = -1
			vs.pStatus = 1
			vs.cView.Viewnum++
		}
	}

	if vs.bStatus == 0{
		elapsed:=cTime.Sub(vs.servers[vs.cView.Backup])
		if elapsed.Seconds() > duration.Seconds() && vs.pStatus==0{
			delete(vs.servers,vs.cView.Backup)
			vs.cView.Backup = ""
			vs.bStatus = -1
		}
	}

	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.

	vs.servers = make(map[string]time.Time)
	vs.cView = &View{1,"",""}
	vs.pStatus = -1
	vs.bStatus = -1
	vs.idles = list.New()

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
