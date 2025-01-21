package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"sync"
	"sync/atomic"
	"time"
)

type Shard struct {
	kvmap     map[string]string
	confignum int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	Client int64
	SeqNum int64
}

type OpReply struct {
	Err    Err
	Client int64
	SeqNum int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	shardClerk   *shardctrler.Clerk
	prevConfig   shardctrler.Config
	currConfig   shardctrler.Config

	shards       []*Shard
	lastSeq      map[int64]int64
	waitChs      map[int]chan OpReply
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	si := key2shard(args.Key)
	kv.mu.Lock()
	if kv.currConfig.Shards[si] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.SeqNum <= kv.lastSeq[args.Client] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{OpType: GetOp, Key: args.Key, Client: args.Client, SeqNum: args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	result := kv.waitChannel(op, index)
	reply.Err = result.Err
	reply.Value = kv.shards[si].kvmap[op.Key]
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	si := key2shard(args.Key)
	kv.mu.Lock()
	if kv.currConfig.Shards[si] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.SeqNum <= kv.lastSeq[args.Client] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, Client: args.Client, SeqNum: args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	result := kv.waitChannel(op, index)
	reply.Err = result.Err
}

func (kv *ShardKV) waitChannel(op Op, raftIndex int) OpReply {
	ch := make(chan OpReply, 1)
	kv.mu.Lock()
	kv.waitChs[raftIndex] = ch
	kv.mu.Unlock()

	var reply OpReply
	select {
	case reply = <-ch:
		if reply.Client != op.Client || reply.SeqNum != op.SeqNum {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go func(index int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.waitChs, index)
	}(raftIndex)

	return reply
}


func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			op := msg.Command.(Op)
			reply := OpReply{OK, op.Client, op.SeqNum}

			switch op.OpType {
			case GetOp, PutOp, AppendOp:
				kv.doClientRequest(&op, &reply)
			}

			if ch, ok := kv.waitChs[msg.CommandIndex]; ok {
				ch <- reply
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) doClientRequest(op *Op, reply *OpReply) {
	si := key2shard(op.Key)
	if kv.currConfig.Shards[si] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	if op.SeqNum > kv.lastSeq[op.Client] {
		if op.OpType == PutOp {
			kv.shards[si].kvmap[op.Key] = op.Value
		} else if op.OpType == AppendOp {
			kv.shards[si].kvmap[op.Key] += op.Value
		}
		kv.lastSeq[op.Client] = op.SeqNum
	}
}


func (kv *ShardKV) updateConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		kv.currConfig = kv.shardClerk.Query(-1)
		for si, gid := range kv.currConfig.Shards {
			if gid == kv.gid {
				kv.shards[si].confignum = kv.currConfig.Num
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}


// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.shards = make([]*Shard, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shards[i] = &Shard{kvmap: make(map[string]string), confignum: 0}
	}

	// Use something like this to talk to the shardctrler:
	kv.shardClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.lastSeq = make(map[int64]int64)
	kv.waitChs = make(map[int]chan OpReply)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.updateConfig()

	return kv
}
