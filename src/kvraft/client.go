package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)


// A client talks to the service through a Clerk with Put/Append/Get methods.
// The Clerk manages RPC interactions with the servers.
type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int    // clerks send RPCs to the server whose associated Raft is the leader
	client  int64  // the client invoking the request
	seqnum  int64  // providing linearizability
}

func nrand() int64 {
	maximum := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, maximum)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.client = nrand()
	ck.seqnum = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.seqnum++
	args := GetArgs{key, ck.client, ck.seqnum}
	reply := GetReply{}
	for {
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)

		// Retry if the clerk sends an RPC to the wrong leader, or it cannot reach the kvserver
		if !ok || reply.Err != OK {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqnum++
	args := PutAppendArgs{op, key, value, ck.client, ck.seqnum}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.leader].Call("KVServer."+op, &args, &reply)
		if !ok || reply.Err != OK {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
