package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
//import "container/list"
import "strconv"

const Delay = time.Nanosecond * 10
const Mdelay = 32

const debug = false

type Pair struct{
	peer int
	minS int
}

var gmu sync.Mutex

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
	pnum int

	minS int
	maxS int
	gMinS int	
	log map[int]*LogInstance
	//mus map[int]*sync.Mutex

	cmin chan Pair
}

type LogInstance struct{
	decided bool
	Np int
	Na int
	Va interface{}
}

type PaxosProposal struct{
	N int
	Np int
	Na int
	Va interface{}
}

type PaxosRequest struct{
	Seq int
	Prop PaxosProposal
}

type PaxosReply struct{
	Min int
	Ack bool
	Np int
	Na int
	Va interface{}
}

func max(a int, b int) int{
	if a > b{
		return a
	}else{
		return b
	}
}

func min(a int, b int) int{
	if a < b{
		return a
	}else{
		return b
	}
}

func majority(votes int, quorum int) bool{
	if votes >= ((quorum)/2 + 1){
		return true
	}else{
		return false
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      //fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
	//x:= reply.(PaxosReply)
	//fmt.Println("call")
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) Prepare(prepare *PaxosRequest, reply *PaxosReply) error{
	seq:=prepare.Seq

	px.mu.Lock()
	defer px.mu.Unlock()
	_,exists:= px.log[seq]
	if !exists{
		px.log[seq]=&LogInstance{false,-1,-1, prepare.Prop.Va}
		//px.mus[seq]=&sync.Mutex{}
	}
	
	if prepare.Prop.N > px.log[seq].Np{
		px.log[seq].Np = prepare.Prop.N 
		reply.Ack = true

		reply.Na = px.log[seq].Na
		reply.Va = px.log[seq].Va
		if debug{
		fmt.Println("PrepareRequest["+strconv.Itoa(px.me)+"] = "+"( "+ strconv.Itoa(seq)+" , "+ strconv.Itoa(int(px.log[seq].Np))+" , "+strconv.Itoa(px.log[seq].Va.(int))+" )")
		}
	
	}else{
		reply.Ack = false
		reply.Np = px.log[seq].Np
	}	
	return nil
}

func (px *Paxos) Accept(accept *PaxosRequest, reply *PaxosReply) error{
	seq:=accept.Seq

	px.mu.Lock()
	defer px.mu.Unlock()
	_,exists:= px.log[seq]
	if !exists{
		px.log[seq]=&LogInstance{false,-1,-1, accept.Prop.Va}
		//px.mus[seq]=&sync.Mutex{}
	}

	if accept.Prop.N >= px.log[seq].Np{
		px.log[seq].Np = accept.Prop.N
		px.log[seq].Na = accept.Prop.N
		px.log[seq].Va = accept.Prop.Va
		px.maxS = int(max(int(seq),int(px.maxS)))

		if debug{
		fmt.Println("AcceptRequest["+strconv.Itoa(px.me)+"] = "+"( "+ strconv.Itoa(seq)+" , "+ strconv.Itoa(int(px.log[seq].Na))+" , "+strconv.Itoa(px.log[seq].Va.(int))+" )")
		}

		reply.Ack = true
		reply.Min = px.minS
	}else{
		reply.Min = px.minS
		reply.Ack = false
		reply.Np = px.log[seq].Np
	}

	return nil
}

func (px *Paxos) Decide(decide *PaxosRequest, reply *PaxosReply) error{
	seq:=decide.Seq

	px.mu.Lock()
	defer px.mu.Unlock()
	_,exists:= px.log[seq]
	if !exists{
		px.log[seq]=&LogInstance{false,-1,-1, decide.Prop.Va}
		//px.mus[seq]=&sync.Mutex{}
	}

	px.log[seq].decided = true
	px.log[seq].Np = decide.Prop.N
	px.log[seq].Na = decide.Prop.N
	px.log[seq].Va = decide.Prop.Va
	px.maxS = int(max(int(seq),int(px.maxS)))

	if debug{
	fmt.Println("DecideRequest["+strconv.Itoa(px.me)+"] = "+"( "+ strconv.Itoa(seq)+" , "+ strconv.Itoa(px.log[seq].Np)+" , "+strconv.Itoa(px.log[seq].Va.(int))+" )")
	}

	/*if !exists{
		px.log[seq].decided = true
		px.log[seq].Np = decide.Prop.N
		px.log[seq].Na = decide.Prop.N
		px.log[seq].Va = decide.Prop.Va
		px.maxS = int(max(int(seq),int(px.maxS)))
	}else{
		px.log[seq].decided = true
	}*/

	return nil
}

func Propose(px *Paxos,seq int , v interface{}){
	if debug{
	fmt.Println("Propose ["+strconv.Itoa(px.me)+"] = "+"( "+ strconv.Itoa(seq)+" , "+strconv.Itoa(v.(int))+" )")
	}

	mu:=sync.Mutex{}
	bar:=sync.WaitGroup{}

	decided:=false
	proposal:=PaxosProposal{int(px.me),-1,-1,v}
	_n:=proposal.N//n = -1
	_na:=proposal.Na//n_a = -1
	_v:=proposal.Va// _v = va
	round:=1

	for !decided{
		if debug{
		//fmt.Println("Round: "+strconv.Itoa(round))
		}
		if round>2{
			if round < Mdelay{
				time.Sleep(time.Duration(round)*Delay)
			}else{
				time.Sleep(time.Duration(Mdelay)*Delay)
			}
		}
		round*=2
		
		//Initialize for prepare
		votes:=0
		proposal.N=max(_n,proposal.N) + px.pnum
		_n:=proposal.N
		request:=PaxosRequest{seq,proposal}
		bar.Add(px.pnum)

		if debug{
			fmt.Println("("+strconv.Itoa(round)+")APrepare["+strconv.Itoa(px.me)+"] = "+"( "+ strconv.Itoa(seq)+" , "+strconv.Itoa(proposal.N)+" , "+strconv.Itoa(_v.(int))+" )")
		}

		for i:=0; i < px.pnum; i++ {
			go func(i int, seq int){
				defer func() {
					recover()
				}()
				defer bar.Done()

				var reply PaxosReply
				ok:= false
				if i == px.me{
					px.Prepare(&request,&reply)
					ok=true
				}else{
					ok=call(px.peers[i],"Paxos.Prepare", &request, &reply)
				}
				if debug{
				fmt.Println("("+strconv.Itoa(round)+")Prepare["+strconv.Itoa(i)+","+strconv.FormatBool(reply.Ack)+"] = "+"( "+ strconv.Itoa(seq)+" , "+strconv.Itoa(proposal.N)+" , "+strconv.Itoa(_v.(int))+" )")
				}
				
				mu.Lock()//Local mutex
				if ok && reply.Ack{
					votes++
					if reply.Na > _na{
						_na = reply.Na
						_v = reply.Va
					}
				}else if !reply.Ack{
					_n=max(_n,reply.Np)
				}
				mu.Unlock()
				
			}(i, seq)
		}
		bar.Wait()
		
		if !majority(votes,px.pnum){//if not majority//restart
			continue
		}

		if debug{
		fmt.Println("("+strconv.Itoa(round)+")Send Accept["+strconv.Itoa(px.me)+" , "+strconv.Itoa(votes)+"] = "+"( "+ strconv.Itoa(seq)+" , "+strconv.Itoa(proposal.N)+" , "+strconv.Itoa(_v.(int))+" )")
		}
		//Initialize for accept
		votes=0
		proposal.Va = _v
		request=PaxosRequest{seq,proposal}

		bar.Add(px.pnum)
		for i:=0;i<px.pnum;i++{
			go func(i int, seq int){
				defer func() {
					recover()
				}()
				defer bar.Done()

				//proposal{n,_v}
				var reply PaxosReply
				reply.Ack = false
				ok:= false
								
				if i == px.me{
					px.Accept(&request,&reply)
					ok=true
				}else{
					ok=call(px.peers[i],"Paxos.Accept", &request, &reply)
				}

				if debug{
				fmt.Println("("+strconv.Itoa(round)+")Accept["+strconv.Itoa(i)+" , "+strconv.FormatBool(reply.Ack)+"] = "+"( "+ strconv.Itoa(seq)+" , "+strconv.Itoa(proposal.N)+" , "+strconv.Itoa(_v.(int))+" )")
				}
				px.cmin<-Pair{i,reply.Min}//Piggybacked Min
				mu.Lock()//Local mutex
				if ok && reply.Ack{
					votes++
				}else if !reply.Ack{
					_n=max(_n,reply.Np)
				}
				mu.Unlock()
				
			}(i, seq)
		}
		bar.Wait()

		if !majority(votes,px.pnum){//If no majority//restart
			continue
		}

		decided = true
		request=PaxosRequest{seq,proposal}
		for i:=0;i<px.pnum;i++{
			go func(i int, seq int){
				defer func() {
					recover()
				}()
				var reply PaxosReply
				if debug{
				fmt.Println("("+strconv.Itoa(round)+")Decide ["+strconv.Itoa(i)+"] = "+"( "+ strconv.Itoa(seq)+" , "+strconv.Itoa(int(proposal.N))+" , "+strconv.Itoa(proposal.Va.(int))+" )")
				}
				if px.me == i{
					px.Decide(&request,&reply)
				}else{
					call(px.peers[i],"Paxos.Decide", &request,&reply)
				}
			}(i,seq)
		}
	}
}

func findMin(px *Paxos){
	minPeer:= make(map[int]int)
	last:=0
	for !px.dead{
		
		for msg:=range px.cmin{
			if msg.minS >=0{
				oldm,exists:=minPeer[msg.peer]
				if exists{
					if msg.minS > oldm{
						minPeer[msg.peer]=msg.minS
					}
				}else{
					minPeer[msg.peer]=msg.minS
				}
			}
			

			if len(minPeer) == px.pnum{
				gMin:=minPeer[0]
				for _,v:= range minPeer{
					if v < gMin{
						gMin=v
					}
				}
			
				px.mu.Lock()
				px.gMinS = gMin			
				for seq:=last;seq<=px.gMinS;seq++{
					delete(px.log,seq)			
				}
				last=px.gMinS
				px.mu.Unlock()
				minPeer=make(map[int]int)
			}
		}
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	i,exists:= px.log[seq];
	if !exists{
		i = &LogInstance{false,-1, -1, v}
		px.log[seq] = i
		//px.mus[seq] = &sync.Mutex{}
	}
	//fmt.Println("2["+strconv.Itoa(px.me)+"] = "+"( "+ strconv.Itoa(seq)+" , "+strconv.Itoa(v.(int))+" )")
	//if i.decided {
	//	return
	//}

	go Propose(px, seq,v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	px.minS = seq
	//fmt.Println("Done ["+strconv.Itoa(px.me)+"] = "+"( "+ strconv.Itoa(seq)+")")
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return px.maxS
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  return px.gMinS+1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	inst,exists:= px.log[seq]
	if exists && inst.decided{
		return inst.decided,inst.Va
	}
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
	px.pnum=len(px.peers)	

	px.log = make(map[int]*LogInstance);
	//px.mus = make(map[int]*sync.Mutex);
	px.maxS=-1
	px.minS=-1
	px.gMinS=-1

	px.cmin = make(chan Pair)

	go findMin(px)

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
