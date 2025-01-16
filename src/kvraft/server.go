package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// The kvserver submits any Put/Append/Get operations to Raft,
// so that the Raft log holds a sequence of operations.
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

type RaftReply struct {
	Err    string
	Value  string
	Client int64
	SeqNum int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Snapshot (4B)
	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	kvmap   map[string]string
	lastSeq map[int64]int64
	waitChs map[int]chan RaftReply
}

// Get RPC handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if args.SeqNum <= kv.lastSeq[args.Client] {
		reply.Err = OK
		reply.Value = kv.kvmap[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// Enter an Op in the Raft log using rf.Start()
	op := Op{GetOp, args.Key, "", args.Client, args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	result := kv.waitChannel(op, index)
	reply.Err, reply.Value = result.Err, result.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if args.SeqNum <= kv.lastSeq[args.Client] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{PutOp, args.Key, args.Value, args.Client, args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	result := kv.waitChannel(op, index)
	reply.Err = result.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if args.SeqNum <= kv.lastSeq[args.Client] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{AppendOp, args.Key, args.Value, args.Client, args.SeqNum}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	result := kv.waitChannel(op, index)
	reply.Err = result.Err
}

// Wait for reply from the Raft library
func (kv *KVServer) waitChannel(op Op, raftIndex int) RaftReply {
	ch := make(chan RaftReply, 1)
	kv.mu.Lock()
	kv.waitChs[raftIndex] = ch
	kv.mu.Unlock()

	var reply RaftReply
	select {
	case reply = <-ch:
		if reply.Client != op.Client || reply.SeqNum != op.SeqNum {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(time.Millisecond * 150):
		reply = RaftReply{Err: ErrTimeout}
	}

	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.waitChs, raftIndex)
	}()
	return reply
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Whenever the key/value server detects that the Raft state size exceeds maxraftstate,
// it saves a snapshot by calling the Snapshot method of the Raft library.
func (kv *KVServer) createSnapshot(raftIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.kvmap) != nil || e.Encode(kv.lastSeq) != nil {
		return
	}
	kv.rf.Snapshot(raftIndex, w.Bytes())
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvmap map[string]string
	var lastSeq map[int64]int64

	if d.Decode(&kvmap) != nil || d.Decode(&lastSeq) != nil {
		return
	}
	kv.kvmap = kvmap
	kv.lastSeq = lastSeq
}

// A goroutine that keeps reading messages from the applyCh, connected to the Raft library.
// The server executes Op commands as Raft commits them, i.e. as they appear on the applyCh.
func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			// apply a commited command to the local state machine
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex

			op := msg.Command.(Op)  // type assertion
			if op.SeqNum > kv.lastSeq[op.Client] {
				if op.OpType == PutOp {
					kv.kvmap[op.Key] = op.Value
				} else if op.OpType == AppendOp {
					kv.kvmap[op.Key] += op.Value
				}
				kv.lastSeq[op.Client] = op.SeqNum
			}

			if ch, ok := kv.waitChs[msg.CommandIndex]; ok {
				reply := RaftReply{OK, "", op.Client, op.SeqNum}
				if op.OpType == GetOp {
					reply.Value = kv.kvmap[op.Key]
				}
				ch <- reply
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// 4B: install a snapshot
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvmap = make(map[string]string)
	kv.lastSeq = make(map[int64]int64)
	kv.waitChs = make(map[int]chan RaftReply)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
