package kvpaxos

import "net/rpc"
import "fmt"
import "strconv"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
	//ip string
	ip int64
	rid int64
}

const cdelay = 10*time.Millisecond

func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
	//ck.ip = strconv.FormatInt(nrand(), 10)//+strconv.FormatInt(nrand(), 10)
	ck.ip = nrand()
	ck.rid = 0
  return ck
}

func nrand() int64 {
   max := big.NewInt(int64(1) << 62)
   bigx, _ := rand.Int(rand.Reader, max)
   x := bigx.Int64()
   return x
}

func MakeRid(ip string, rid uint64)string{
	return ip+"_"+strconv.FormatUint(rid,10)
}

func WaitC(count int){
	if time.Duration(count)*cdelay < time.Second{
		time.Sleep(time.Duration(count)*cdelay)
	}else{
		time.Sleep(time.Second)
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
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
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

  fmt.Println(err)
  return false
}

func (ck *Clerk) MakeRid() string{
	//return ck.ip+"_"+strconv.FormatUint(ck.rid, 10)
	//return ""+"_"+strconv.FormatUint(ck.rid, 10)
	return ""
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
	//args:=GetArgs{key,MakeRid(ck.ip,ck.rid)}
	amo:=AMO{ck.ip,ck.rid}
	args:=GetArgs{key,amo}
	reply:=GetReply{PendingResponse,""}

	ok:=false
	count:= 1
	i:=0
	for {
		ok = call(ck.servers[i%len(ck.servers)], "KVPaxos.Get", &args, &reply)
		//if ok && (reply.Err == OK || reply.Err == ErrNoKey){
		if ok && (reply.Err == OK){
			ck.rid++
			return reply.Value
		}
		WaitC(count)
		count*=2
		i++
	}
	return ""
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
	//args:=&PutArgs{key,value,dohash,MakeRid(ck.ip,ck.rid)}
	amo:=AMO{ck.ip,ck.rid}
	args:=&PutArgs{key,value,dohash,amo}
	reply:=&PutReply{PendingResponse, ""}

	i:=0
	ok:=false
	count:=1
	for {
		ok = call(ck.servers[i%len(ck.servers)], "KVPaxos.Put", args, reply)
		if ok && reply.Err == OK{
			ck.rid++
			return reply.PreviousValue
		}
		WaitC(count)
		count*=2
		i++
	}
	
  return ""
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
