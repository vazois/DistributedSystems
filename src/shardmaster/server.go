package shardmaster

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
import "time"
import "reflect"
import "sort"
import "strconv"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
	currSeq int
	replyConfig Config
}

const Debug=0
const sdelay=10*time.Millisecond
const mdelay=2*time.Second

func WaitS(count int){
	if time.Duration(count)*sdelay < mdelay{
		time.Sleep(time.Duration(count)*sdelay)
	}else{
		time.Sleep(mdelay)
	}
}

func (sm *ShardMaster)CheckSeq(seq int){
	for sm.currSeq <= seq{
		WaitS(2)
	}
}

const (
	OK="OK"
	ErrGIDNotFound="ErrGIDNotFound"
	ErrGIDInUse="ErrGIDInUse"
)

type Err string
const (
	JOIN="JOIN"
	LEAVE="LEAVE"
	MOVE="MOVE"
	QUERY="QUERY"
)

type Operation string
type Op struct {
  // Your data here.
	Type Operation
	Args interface{}
}

func (sm *ShardMaster)lastConfig() (Config,int){
	pos:=len(sm.configs)-1
	return sm.configs[pos],pos
}

func MakeGroups(in map[int64][]string) map[int64][]string{
	out:=make(map[int64][]string)
	for key,value:=range in{
		out[key]=value
	}
	return out
}

func MakeShards(in [NShards]int64)[NShards]int64{
	var out [NShards]int64
	for key,value:=range in{
		out[key]=value
	}
	return out
}

type ShardGroup struct{
	GID int64
	Tail int
	Shards []int
}

type ShardGroupDecComp []ShardGroup
func (sg ShardGroupDecComp) Len() int { return len(sg) }
func (sg ShardGroupDecComp) Swap(i,j int) { sg[i], sg[j] = sg[j], sg[i] }
func (sg ShardGroupDecComp) Less(i,j int) bool { return sg[i].Tail > sg[j].Tail }

type ShardGroupIncComp []ShardGroup
func (sg ShardGroupIncComp) Len() int { return len(sg) }
func (sg ShardGroupIncComp) Swap(i,j int) { sg[i], sg[j] = sg[j], sg[i] }
func (sg ShardGroupIncComp) Less(i,j int) bool { return sg[i].Tail < sg[j].Tail }

func GroupShards(Groups map[int64][]string, Shards [NShards]int64, inc bool) []ShardGroup{
	sgs:=make(map[int64]*ShardGroup)
	for gid,_:=range Groups{
		sgs[gid] = &ShardGroup{gid,0,[]int{}}
	}
	
	for shard,gid:=range Shards{
		sg,_:= sgs[gid]
		sg.Tail++
		sg.Shards=append(sg.Shards,shard)
	}

	var shardGroups []ShardGroup
	for _,sg:=range sgs{
		shardGroups=append(shardGroups,*sg)
	}
	if inc{
		sort.Sort(ShardGroupIncComp(shardGroups))
	}else{
		sort.Sort(ShardGroupDecComp(shardGroups))
	}
	return shardGroups
}

func (sm *ShardMaster) Commit(op Op) int{
	seq:= sm.currSeq
	count:=1
	for{
		sm.px.Start(seq,op)
		time.Sleep(sdelay)
		
		var cop Op		
		for {
			commited,v:= sm.px.Status(seq)
			if commited{
				cop=v.(Op)
				break;
			}
			WaitS(count)
			count*=2
		}				
		
		if reflect.DeepEqual(cop, op){
			break;
		}else{
			seq++
		}
	}

	return seq
}

func (sm *ShardMaster) Execute(seq int)(Config,Err){
	var config Config
	var err Err
	for i:=sm.currSeq;i<=seq;i++{
		_,val:=sm.px.Status(i)
		eop:=val.(Op)
		if eop.Type == JOIN{
			sm.ExecuteJoin(&eop)
		}else if eop.Type == LEAVE{
			sm.ExecuteLeave(&eop)
		}else if eop.Type == MOVE{
			sm.ExecuteMove(&eop)
		}else if eop.Type == QUERY{
			config,err=sm.ExecuteQuery(&eop)
		}

	}

	return config,err
}

func (sm *ShardMaster) ExecuteJoin(op *Op) Err{
	//sm.mu.Lock()
	//defer sm.mu.Unlock()

	args:=op.Args.(JoinArgs)	
	config,num:=sm.lastConfig()

	//Check if group id exists
	_,exists:=config.Groups[args.GID]
	if exists{
		return ErrGIDInUse
	}

	//Prepare new configuration
	cNum:=num+1
	g:=MakeGroups(config.Groups)
	s:=MakeShards(config.Shards)
	g[args.GID]=args.Servers

	if len(g)==1{//Distribute all shards to a single group
		for i,_:=range s{
			s[i] = args.GID
		}
	}else{//Load balance shards to new group starting from the group with
		shardGroups:=GroupShards(config.Groups,config.Shards,false)
		shardsPerGroup:= NShards/len(g)
		
		for i:=0;i<shardsPerGroup;i++{
			shardGroup:= &shardGroups[i%len(shardGroups)]
			shard:=shardGroup.Shards[shardGroup.Tail - 1]
			shardGroup.Tail=shardGroup.Tail-1
			s[shard]=args.GID

			if Debug == 1{
			fmt.Println("( "+strconv.Itoa(i%len(shardGroups))+" , "+ strconv.Itoa(shard) +")")
			}
		}
	}
	
	nconfig:=Config{cNum,s,g}
	sm.configs = append(sm.configs,nconfig)

	if Debug==2{
	fmt.Println("["+strconv.Itoa(nconfig.Num)+"]")
	fmt.Println(len(nconfig.Groups))
	fmt.Println("<Join> { " + strconv.Itoa(sm.me) +" , " + strconv.FormatInt(args.GID,10)+" }")
	}

	return OK
}

func (sm *ShardMaster) ExecuteLeave(op *Op) Err{
	args:=op.Args.(LeaveArgs)
	config,num:=sm.lastConfig()
	
	_,exists:=config.Groups[args.GID]
	if !exists{
		return ErrGIDNotFound
	}	
	
	cNum:=num+1
	g:=MakeGroups(config.Groups)
	s:=MakeShards(config.Shards)
	delete(g,args.GID)

	if len(g) == 0{
		for si,_:=range s{
			s[si] = 0
		}
	}else{//Group shards in increasing order
		shardGroups:=GroupShards(config.Groups,config.Shards,true)
		var recallShards []int

		for shard,gid:=range s{
			if args.GID == gid{
				recallShards = append(recallShards,shard)
			}
		}
		
		total:=len(recallShards)
		j:=0
		for i:=0;i<total;i++{
			shard:=recallShards[i]
			shardGroup:= &shardGroups[j%len(shardGroups)]
			for shardGroup.GID == args.GID{
				j++
				shardGroup= &shardGroups[j%len(shardGroups)]
			}
			s[shard]=shardGroup.GID
			j++
		}
	}

	nconfig:=Config{cNum,s,g}
	sm.configs = append(sm.configs,nconfig)

	if Debug==2{
	fmt.Println("["+strconv.Itoa(nconfig.Num)+"]")
	fmt.Println(len(nconfig.Groups))
	fmt.Println("<Leave> { " + strconv.Itoa(sm.me) +" , " + strconv.FormatInt(args.GID,10)+" }")
	}
	return OK
}

func (sm *ShardMaster) ExecuteMove(op *Op) Err{
	args:=op.Args.(MoveArgs)
	config,num:=sm.lastConfig()

	_,exists:=config.Groups[args.GID]
	if !exists{
		return ErrGIDNotFound
	}

	cNum:=num+1
	g:=MakeGroups(config.Groups)
	s:=MakeShards(config.Shards)

	s[args.Shard] = args.GID
	nconfig:=Config{cNum,s,g}
	sm.configs = append(sm.configs,nconfig)

	if Debug==2{
	fmt.Println("<<<<Move>>>> { " + strconv.Itoa(sm.me) + " }")
	fmt.Println(nconfig.Num)
	fmt.Println(nconfig.Groups)
	}
	return OK	
}

func (sm *ShardMaster) ExecuteQuery(op *Op)(Config,Err){
	args:=op.Args.(QueryArgs)
	//config,num:=sm.lastConfig()
	config,_:=sm.lastConfig()
	if args.Num < 0 || args.Num >= len(sm.configs){
		return config , OK
	}else{
		return sm.configs[args.Num] , OK
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	if Debug==2{
	fmt.Println("[Join] { " + strconv.Itoa(sm.me) +" , " + strconv.FormatInt(args.GID,10)+" }")
	}

	op:=Op{JOIN, *args}

	sm.mu.Lock()	
		seq:=sm.Commit(op)
	sm.mu.Unlock()
	sm.CheckSeq(seq)
	
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	if Debug==2{
	fmt.Println("[Leave] { " + strconv.Itoa(sm.me) +" , " + strconv.FormatInt(args.GID,10)+" }")
	}

	op:=Op{LEAVE, *args}

	sm.mu.Lock()	
		seq:=sm.Commit(op)
	sm.mu.Unlock()
	sm.CheckSeq(seq)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	op:=Op{MOVE, *args}

	sm.mu.Lock()	
		seq:=sm.Commit(op)
	sm.mu.Unlock()
	sm.CheckSeq(seq)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	op:=Op{QUERY, *args}	

	sm.mu.Lock()	
		seq:=sm.Commit(op)
	sm.mu.Unlock()
	sm.CheckSeq(seq)
	reply.Config=sm.replyConfig
  return nil
}

func (sm *ShardMaster) CatchUp(){
	count:=2
	seq:=sm.currSeq
	for sm.dead == false{
		WaitS(count)
		commited,_:= sm.px.Status(seq)
		if commited{
			sm.mu.Lock()
			config,_:=sm.Execute(seq)
			sm.replyConfig = config
			sm.px.Done(seq)
			sm.currSeq=seq+1
			seq=sm.currSeq
			sm.mu.Unlock()
		}
	}

}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
  	gob.Register(MoveArgs{})
  	gob.Register(QueryArgs{})

  sm := new(ShardMaster)
  sm.me = me
	sm.currSeq = 0

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
	sm.replyConfig=sm.configs[0]
	go sm.CatchUp()

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
