package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int    // cache the current leader for the next RPC
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqnum++
	args := &QueryArgs{Num: num, Client: ck.client, SeqNum: ck.seqnum}

	for {
		var reply QueryReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == OK {
			return reply.Config
		}
		// try the next known server.
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqnum++
	args := &JoinArgs{Servers: servers, Client: ck.client, SeqNum: ck.seqnum}

	for {
		var reply JoinReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == OK {
			return
		}
		// try the next known server.
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqnum++
	args := &LeaveArgs{GIDs: gids, Client: ck.client, SeqNum: ck.seqnum}

	for {
		var reply LeaveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == OK {
			return
		}
		// try the next known server.
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqnum++
	args := &MoveArgs{shard, gid, ck.client, ck.seqnum}

	for {
		var reply MoveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err == OK {
			return
		}
		// try the next known server.
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
