package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "time"
import "reflect"

const Debug=0
const sdelay=20*time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 1 {
    log.Printf(format, a...)
  }
  return
}

type Rpair struct{
	Rid int64
	Val string
}

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
	//Rid string//ClientID + Unique Identifier
	Amo AMO
	Type string
	Key string
	Value string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
	rLog map[int64]Rpair
	store map[string]string
	seq int
	tseq int
	execute map[int]Op
}

//func MakeOp(Rid string, Type string, Key string, nValue string) Op{
func MakeOp(Amo AMO, Type string, Key string, nValue string) Op{
	op:=Op{Amo,Type,Key,nValue}
	return op
}

func WaitS(count int){
	if time.Duration(count)*sdelay < 2*time.Second{
		time.Sleep(time.Duration(count)*sdelay)
	}else{
		time.Sleep(2*time.Second)
	}
}

func (kv *KVPaxos) Commit(op Op) int{
	seq:=kv.seq
	count:=1
	for{
		kv.px.Start(seq,op)
		time.Sleep(sdelay)
		
		var cop Op		
		for {
			commited,v:= kv.px.Status(seq)
			if commited{
				cop=v.(Op)
				break;
			}
			WaitS(count)
			count*=2
		}				
		
		if reflect.DeepEqual(cop.Amo, op.Amo){
		//if cop.Amo==op.Amo{
			if Debug==1{
			fmt.Println("Command[ "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+cop.Key+" , "+strconv.FormatInt(cop.Amo.Ip,10) +" , "+strconv.FormatInt(cop.Amo.Rid,10)+" ) == (Mine Committed)")
			}
			kv.execute[seq]=cop
			break;
		}else{
			if Debug==1{
			fmt.Println("Command[ "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+cop.Key+" , "+strconv.FormatInt(cop.Amo.Ip,10) +" , "+strconv.FormatInt(cop.Amo.Rid,10)+" ) == (Committed)")
			}
			kv.execute[seq]=cop
			seq++
		}
	}

	return seq
}

func (kv *KVPaxos) Execute(seq int) (Err,string) {
	preply:=&PutReply{}
	greply:=&GetReply{}
	last:=""
	for i:=kv.seq; i<=seq;i++{
		//_,v:=kv.px.Status(i)//TODO possible not have been commited? Commit ensures commit before break
		//op:=v.(Op)
		op,_:=kv.execute[i]
		//op,commited:=kv.execute[i]
		/*if !commited{
			for{
				committed,v:=kv.px.Status(i)
				if committed{
					op = v.(Op)
					break;
				}
			}
		}*/
		kv.tseq=i
		if Debug == 1{
		fmt.Println("Execute("+op.Type+" , "+ op.Key+")[ " +  strconv.Itoa(kv.me) + " , "+ strconv.Itoa(i) +" , "+strconv.FormatInt(op.Amo.Ip,10) +" , "+strconv.FormatInt(op.Amo.Rid,10) +" ]")
		}
		if op.Type == GET{
			//fmt.Println("Command[ "+ "" +" , "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+op.Key+" , "+op.Value+" ) = Seq ( "+strconv.Itoa(kv.seq)+" )")
			kv.ExecuteGet(&op,greply)
			last=GET			
		}else{
			//fmt.Println("Command[ "+ "" +" , "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+op.Key+" , "+op.Value +" ) = Seq ( "+strconv.Itoa(kv.seq)+" )")
			kv.ExecutePut(&op,preply)
			last=PUT
		}
		delete(kv.execute,i)
	}
	if last==GET{
		return greply.Err, greply.Value
	}else{
		return preply.Err, preply.PreviousValue
	}
}


func (kv *KVPaxos) ExecuteGet(op *Op,reply *GetReply){
	oValue,executed:=kv.rLog[op.Amo.Ip]
	exists:=false
	reply.Value,exists= kv.store[op.Key]
	reply.Err=OK
	if !exists{
		reply.Err=ErrNoKey
	}
	if !executed || ( executed && (oValue.Rid < op.Amo.Rid)){//Execute only if not executed before or executed before and operation request id > old operation request id
		kv.rLog[op.Amo.Ip]=Rpair{op.Amo.Rid,reply.Value}
	}
	if Debug==1{
	fmt.Println("Command[ "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+op.Key+" , "+reply.Value +" ) = Seq ( "+strconv.Itoa(kv.tseq)+" ) == (Request)")
	fmt.Println("Command[ "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+op.Key+" , "+strconv.FormatInt(op.Amo.Ip,10) +" , "+strconv.FormatInt(op.Amo.Rid,10)+" ) = Seq ( "+strconv.Itoa(kv.tseq)+" )")
	}
}

func (kv *KVPaxos) ExecutePut(op *Op,reply *PutReply){
	oValue,executed:=kv.rLog[op.Amo.Ip]
	nVal:=op.Value
	oVal,_:=kv.store[op.Key]
	if Debug==1{
	fmt.Println("Command[ "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+op.Key+" =>>>>>> "+strconv.FormatBool(executed)+" , "+strconv.FormatInt(oValue.Rid,10) + " < " + strconv.FormatInt(op.Amo.Rid,10))
	}
	if !executed || ( executed && (oValue.Rid < op.Amo.Rid)){//Execute only if not executed before or executed before and operation request id > old operation request id
		if op.Type == PUTH{
			nVal  = oVal + nVal
			nVal = strconv.Itoa(int(hash(nVal)))
		}
		kv.store[op.Key]=nVal
		kv.rLog[op.Amo.Ip]=Rpair{op.Amo.Rid,oVal}
		reply.PreviousValue=oVal
		reply.Err=OK

		if Debug==1{
		oValue,_:=kv.rLog[op.Amo.Ip]
		fmt.Println("Command[ "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+op.Key+" , "+nVal +" , "+oVal+" ) = Seq ( "+strconv.Itoa(kv.tseq)+" ) == (Request)")
		fmt.Println("Command[ "+ strconv.Itoa(kv.me) +" , "+strconv.Itoa(kv.seq)+" ] = "+ op.Type +" ( "+op.Key+" , "+strconv.FormatInt(op.Amo.Ip,10) +" , "+strconv.FormatInt(oValue.Rid,10)+" ) = Seq ( "+strconv.Itoa(kv.tseq)+" )")
		}
	}else{
		reply.Err=OK
		reply.PreviousValue=oValue.Val
	}

}
	
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oValue,executed:=kv.rLog[args.Amo.Ip]
	
	if executed && oValue.Rid==args.Amo.Rid{
		reply.Err=OK
		reply.Value=oValue.Val
		return nil
	}	

	var op Op
	op=MakeOp(args.Amo,GET,args.Key,"")

	seq:=kv.Commit(op)
	reply.Err,reply.Value=kv.Execute(seq)//TODO test it
	kv.px.Done(seq)
	kv.seq=seq + 1


  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	//fmt.Println("Initiating Put("+strconv.Itoa(kv.me)+" , "+strconv.Itoa(kv.seq)+")!!!")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oValue,executed:=kv.rLog[args.Amo.Ip]
	
	if executed && oValue.Rid==args.Amo.Rid{
		reply.Err=OK
		reply.PreviousValue=oValue.Val
		return nil
	}

	var op Op
	ctype:=PUT
	if args.DoHash{
		ctype=PUTH
	}
	op=MakeOp(args.Amo,ctype,args.Key,args.Value)	
	
	seq:=kv.Commit(op)
	reply.Err,reply.PreviousValue=kv.Execute(seq)//TODO test it!
	kv.px.Done(seq)
	kv.seq = seq + 1
	
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
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

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
	kv.rLog = make(map[int64]Rpair)
	kv.store=make(map[string]string)
	kv.seq=0
	kv.execute=make(map[int]Op)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

