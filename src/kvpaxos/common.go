package kvpaxos

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  PendingResponse = "PendingResponse"
)
type Err string

const(
  PUT="Put"
  PUTH="PutHash"
  GET="Get"
)

type AMO struct{
	Ip int64
	Rid int64
}	

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
	//Ip int64
	//Rid int64
	Amo AMO
	//Rid string
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
	Amo AMO
	//Rid string
}

type GetReply struct {
  Err Err
  Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}
