package shardkv
import "hash/fnv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrNotUpdated = "ErrNotUpdated"
  OKReconfig = "OKReconfig"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
	Ip int64
	Rid int64
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
	Ip int64
	Rid int64
}

type GetReply struct {
  Err Err
  Value string
}

type ConfigArgs struct {
	Num int
	Shards []int
}

type ConfigReply struct{
	Err Err
	Store map[string]string
	Reqs map[string]GenReply
}

type GenReply struct{
	Rid int64
	Err Err
	Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

