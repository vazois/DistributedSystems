package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "reflect"

/*
	-When configuration changes after that a command may have been committed. Therefore server executes and replies to client after it replied to another group which now has
	stale data. Then the client moves to the new group and gets the value before its final put.
	- Tick serializes the execution. At some point in time it locks the server and applies configuration changes. Everything committed before that must execute before changing configuration.
	everything commited after that must be dropped.
	- If a command is commited before the configuration change it must be executed.
	- Possibly lost configuration change
*/

const Debug=6
var cu sync.Mutex

const (
	PUT="PUT"
	PUTH="PUTH"
	GET="GET"
	CCONFIG="CCONFIG"
	NOOP="NOOP"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}

const sdelay=10*time.Millisecond
const mdelay=1*time.Second
func WaitS(count int, msg string){
	if Debug == 4{
		fmt.Println("Waiting For ("+strconv.Itoa(count)+") rounds <"+ msg +">")
	}
	if time.Duration(count)*sdelay < mdelay{
		time.Sleep(time.Duration(count)*sdelay)
	}else{
		time.Sleep(mdelay)
	}
}

func (kv *ShardKV)CheckSeq(seq int, msg string){
	for kv.currSeq <= seq{
		//WaitS(1,msg)
		WaitS((rand.Int()%5 + 1),msg)
	}	
}

type Op struct {
	Type string
	Key string
	Value string
	DoHash bool
	
	OConfigNum int
	NConfigNum int

	Ip int64
	Rid int64
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
	currSeq int//current not commited sequence
	store map[string]string//kv store
	AMO map[string]GenReply//At  most once semantics

	config shardmaster.Config//current Configuration
	reconfig sync.Mutex//Block when reconfig is in progress
	configNum int

	kvversion map[int]map[string]string//old key-value store version
	amoversion map[int]map[string]GenReply//old AMO version
	cconfig map[string]GenReply//At most once re-configuration execute
}

func id(ip int64,rid int64) string{
	return strconv.FormatInt(ip,10)+strconv.FormatInt(rid,10)
}

func VerifyOwner(key string,mygid int64, config shardmaster.Config) bool{
	owner:=config.Shards[key2shard(key)]
	return owner == mygid
}

func RecallShards(oldConfig *shardmaster.Config, newConfig *shardmaster.Config, mygid int64)map[int64][]int{//Active Shard Displacement
	g2shards:=make(map[int64][]int)//GID ---> shards
	for shard,newGid:=range newConfig.Shards{
		oldGid:=oldConfig.Shards[shard]
		if newGid == mygid && oldGid!=mygid{
			g2shards[oldGid] = append(g2shards[oldGid],shard)//Ask shards from old group of servers
		}
	}
	return g2shards
}

func ReleaseKeys(shards []int,store map[string]string)map[string]string{
	smap:=make(map[int]bool)
	for _,shard:=range shards{
		smap[shard]=true
	}

	kvos:=make(map[string]string)
	for k,v:=range store{
		_,exists:=smap[key2shard(k)]
		if exists{
			kvos[k]=v
		}
	}
	return kvos
}


func SnapshotKVstore(store map[string]string)map[string]string{
	kvstore:=make(map[string]string)
	for k,v:=range store{
		kvstore[k]=v
	}
	return kvstore
}

func SnapshotReq(AMO map[string]GenReply)map[string]GenReply{
	amo:=make(map[string]GenReply)
	for id,genreply:=range AMO{
		if genreply.Err == ErrNoKey || genreply.Err == OK {
			amo[id]=genreply
		}
	}
	return amo
}

func (kv *ShardKV) HandOffKeys(args *ConfigArgs, reply *ConfigReply) error{
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kvv,kvv_exists:=kv.kvversion[args.Num]
	amov,amov_exists:=kv.amoversion[args.Num]
	if !kvv_exists || !amov_exists{
		reply.Err= ErrNotUpdated
		return nil
	}

	reply.Store=ReleaseKeys(args.Shards,kvv)
	reply.Reqs=SnapshotReq(amov)
	reply.Err=OK

	return nil
}

func (kv *ShardKV) Commit(op Op) int{
	kv.mu.Lock()
	seq:= kv.currSeq
	count:=1
	for{
		kv.px.Start(seq,op)
		time.Sleep(sdelay)	
		var cop Op		
		for {
			commited,v:= kv.px.Status(seq)
			if commited{
				count=1
				cop=v.(Op)
				break;
			}
			WaitS(count,"Commit")
			count*=2
		}

		if reflect.DeepEqual(cop, op){
			break;
		}else{
			seq++
		}
	}
	kv.mu.Unlock()

	return seq
}

func (kv *ShardKV) ExecuteGet(op *Op) GenReply{
	var reply GenReply
	reply.Rid = op.Rid

	if !VerifyOwner(op.Key,kv.gid,kv.config){
		reply.Err = ErrWrongGroup
		reply.Value=""
	}else{
		oval,exists:=kv.store[op.Key]
		reply.Value=oval
		if !exists{
			reply.Err = ErrNoKey
		}else{
			reply.Err = OK
		}
if Debug==3{
fmt.Println("E["+strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]"+op.Type+"("+strconv.FormatInt(op.Ip,10)+","+strconv.FormatInt(op.Rid,10)+","+op.Key+","+reply.Value+"){"+strconv.Itoa(kv.config.Num)+"}"+"~"+strconv.Itoa(key2shard(op.Key))+"~")
}
	}
	kv.mu.Lock()
	kv.AMO[id(op.Ip,op.Rid)] = reply
	kv.mu.Unlock()

	return reply
}

func (kv *ShardKV) ExecutePut(op *Op) GenReply{
	var reply GenReply
	reply.Rid = op.Rid

	if !VerifyOwner(op.Key,kv.gid,kv.config){//Check if my commited command is being executed after a configuration change that make me not owner of given key//
		reply.Err = ErrWrongGroup
		reply.Value=""
	}else{
		oval,_:=kv.store[op.Key]
		nval:= op.Value
		if op.DoHash{
			nval=oval + op.Value
			nval = strconv.Itoa(int(hash(nval)))
			kv.store[op.Key] = nval
		}else{
			kv.store[op.Key] = nval
		}
		reply.Value = oval
		reply.Err = OK
if Debug==3{
fmt.Println("E[" +strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]"+op.Type+"("+strconv.FormatInt(op.Ip,10)+","+strconv.FormatInt(op.Rid,10)+","+op.Key+","+kv.store[op.Key]+","+ reply.Value+"){"+strconv.Itoa(kv.config.Num)+"}"+"~"+strconv.Itoa(key2shard(op.Key))+"~")
}	

	}

	kv.mu.Lock()
	kv.AMO[id(op.Ip,op.Rid)] = reply	
	kv.mu.Unlock()
	
	return reply
}

func (kv *ShardKV) ExecuteCConfig(op Op){
	oldConfig:=kv.sm.Query(op.OConfigNum)
	newConfig:=kv.sm.Query(op.NConfigNum)
	for (oldConfig.Num!=op.OConfigNum || newConfig.Num!=op.NConfigNum){
		oldConfig=kv.sm.Query(op.OConfigNum)
		newConfig=kv.sm.Query(op.NConfigNum)
	}
	_,exists:=kv.cconfig[id(op.Ip,op.Rid)]//Ensure that request has not been executed previously//Reminder make sure transfer state of requests
	if exists || (newConfig.Num <= oldConfig.Num){
		return
	}

if Debug == 4{
	cu.Lock()
	fmt.Println("ECX["+strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]"+"{"+strconv.Itoa(oldConfig.Num)+"} to {"+strconv.Itoa(newConfig.Num)+"}")
	cu.Unlock()
}
	kv.mu.Lock()//TODO
	kv.kvversion[oldConfig.Num] = SnapshotKVstore(kv.store)
	kv.amoversion[oldConfig.Num] = SnapshotReq(kv.AMO)
	kv.mu.Unlock()//TODO

	g2shards:=RecallShards(&oldConfig,&newConfig,kv.gid)

	for oldGid,shards:=range g2shards{
		servers:=oldConfig.Groups[oldGid]
		if len(servers) > 0{
			var reply ConfigReply
			i:=0
			for kv.dead == false {
				args:=ConfigArgs{oldConfig.Num,shards}
				server:=servers[i%len(servers)]
				
				ok:=call(server,"ShardKV.HandOffKeys",args,&reply)//Try to retrieve keys from another server
				if ok && (reply.Err==OK){
					for k,v:=range reply.Store{
						kv.store[k]=v
					}
					for id,genreply:=range reply.Reqs{
						kv.AMO[id]=genreply
					}
					break
				}
				WaitS(10,"Change Config")
				i++
			}

		}
	}
	kv.config=newConfig
	kv.cconfig[id(op.Ip,op.Rid)] = GenReply{op.Rid,OKReconfig,OKReconfig}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	if Debug == 4{
	fmt.Println("W1["+strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]GET("+strconv.FormatInt(args.Ip,10)+","+strconv.FormatInt(args.Rid,10)+","+args.Key+")")
	}
	kv.reconfig.Lock()
	defer kv.reconfig.Unlock()
	kv.mu.Lock()
	genreply,exists:=kv.AMO[id(args.Ip,args.Rid)]//Ensure that request has not been executed previously//Reminder make sure transfer state of requests
	kv.mu.Unlock()
	if exists{
		reply.Err = genreply.Err
		reply.Value = genreply.Value
		return nil
	}

	op:=Op{GET,args.Key,"",false,-1,-1,args.Ip,args.Rid}//Create command to commit
	seq:=kv.Commit(op)//Commit command
	if Debug == 4{
	fmt.Println("W2["+strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]GET("+strconv.FormatInt(args.Ip,10)+","+strconv.FormatInt(args.Rid,10)+","+args.Key+")")
	}
	kv.CheckSeq(seq,"GET")//Check if command was committed

	kv.mu.Lock()
	reply.Err=kv.AMO[id(args.Ip,args.Rid)].Err
	reply.Value=kv.AMO[id(args.Ip,args.Rid)].Value
	kv.mu.Unlock()

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	if Debug == 4{
	fmt.Println("W1["+strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]PUT("+strconv.FormatInt(args.Ip,10)+","+strconv.FormatInt(args.Rid,10)+","+args.Key+")")
	}
	kv.reconfig.Lock()
	defer kv.reconfig.Unlock()
	kv.mu.Lock()
	genreply,exists:=kv.AMO[id(args.Ip,args.Rid)]//Check at most one execution of the command
	kv.mu.Unlock()
	if exists{
		reply.Err = genreply.Err
		reply.PreviousValue = genreply.Value
		return nil
	}
	
	opType:=PUT
	if args.DoHash{
		opType=PUTH
	}
	op:=Op{opType,args.Key,args.Value,args.DoHash,-1,-1,args.Ip,args.Rid}//Create command to commit
	seq:=kv.Commit(op)
	if Debug == 4{
	fmt.Println("W2["+strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]PUT("+strconv.FormatInt(args.Ip,10)+","+strconv.FormatInt(args.Rid,10)+","+args.Key+")")
	}
	kv.CheckSeq(seq,"PUT")//Check if command was committed

	kv.mu.Lock()
	reply.Err=kv.AMO[id(args.Ip,args.Rid)].Err
	reply.PreviousValue=kv.AMO[id(args.Ip,args.Rid)].Value
	kv.mu.Unlock()

  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//

func (kv *ShardKV) tick(){
	kv.reconfig.Lock()
	defer kv.reconfig.Unlock()
	newConfig:=kv.sm.Query(-1)
	for newConfig.Num > kv.config.Num{
		for i:=kv.config.Num;i<newConfig.Num;i++{
			op:=Op{CCONFIG,"","",false,i,i+1,int64(i),int64(i+1)}
			seq:=kv.Commit(op)
			kv.CheckSeq(seq,"TICK")
		}
		newConfig=kv.sm.Query(-1)
	}
}

func (kv *ShardKV) ExecLog(){
	count:=1
	for kv.dead == false{
		commited,val:= kv.px.Status(kv.currSeq);
		if commited{
			eop:=val.(Op)
	if Debug == 4{
	fmt.Println("EL[" +strconv.FormatInt(kv.gid,10)+","+strconv.Itoa(kv.me)+"]"+eop.Type+"("+strconv.FormatInt(eop.Ip,10)+","+strconv.FormatInt(eop.Rid,10)+"){"+strconv.Itoa(kv.currSeq)+"}")
	}
			if eop.Type == GET{
				kv.ExecuteGet(&eop)
			}else if eop.Type == PUT || eop.Type == PUTH{
				kv.ExecutePut(&eop)
			}else if eop.Type == CCONFIG{
				kv.ExecuteCConfig(eop)
			}
			kv.px.Done(kv.currSeq)
			kv.mu.Lock()
			kv.currSeq+=1
			kv.mu.Unlock()
			count=1
			WaitS(count,"OP EXEC")
		}else{
			if count > 128{//TODO
				op:=Op{}
				op.Type=NOOP
				op.Ip=-1
				op.Rid=-1
				kv.px.Start(kv.currSeq,op)
				WaitS(2,"NOOP")
				commited,_:=kv.px.Status(kv.currSeq)
				for !commited{
					commited,_=kv.px.Status(kv.currSeq)
					WaitS(10,"NOOP")
				}
				//kv.px.Done(kv.currSeq)
				//kv.currSeq+=1
				count=1
			}else{
				WaitS(count,"NEXT OP")
				count*=2
			}
		}
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
	gob.Register(PutArgs{})
	gob.Register(PutReply{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(ConfigArgs{})
	gob.Register(ConfigReply{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
	kv.currSeq=0
	kv.store=make(map[string]string)
	kv.AMO=make(map[string]GenReply)
	//kv.configNum=0
	kv.config=kv.sm.Query(0)
	kv.kvversion=make(map[int]map[string]string)
	kv.amoversion=make(map[int]map[string]GenReply)
	kv.cconfig=make(map[string]GenReply)
	
  // Don't call Join().

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  go kv.ExecLog()

  return kv
}
