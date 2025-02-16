package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type Shard struct {
	KVmap     map[string]string
	ConfigNum int
	Status    int
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

	// For configuration updates
	Config shardctrler.Config

	// For shard migration
	Shard   Shard
	ShardID int
	LastSeq map[int64]int64
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

	shards       []Shard
	lastSeq      map[int64]int64
	waitChs      map[int]chan OpReply
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	si := key2shard(args.Key)
	kv.mu.Lock()
	if kv.currConfig.Shards[si] != kv.gid || kv.shards[si].Status != Serving {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.SeqNum <= kv.lastSeq[args.Client] {
		reply.Value = kv.shards[si].KVmap[args.Key]
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

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.shards[si].KVmap[op.Key]; ok {
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	si := key2shard(args.Key)
	kv.mu.Lock()
	if kv.currConfig.Shards[si] != kv.gid || kv.shards[si].Status != Serving {
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

func (kv *ShardKV) MoveShard(args *MoveArgs, reply *MoveReply) {
	kv.mu.Lock()
	if kv.shards[args.ShardID].ConfigNum != args.Shard.ConfigNum || kv.shards[args.ShardID].Status != Waiting {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:  Join,
		Shard:   args.Shard,
		ShardID: args.ShardID,
		LastSeq: args.LastSeq,
	}
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
			case Update:
				kv.doConfigUpdate(&op)
			case Join:
				kv.doShardJoin(&op)
			case Leave:
				kv.doShardLeave(&op)
			}

			// save a snapshot if the local state size exceeds maxraftstate
			if kv.maxraftstate > 0 && kv.rf.GetStateSize() >= kv.maxraftstate {
				kv.createSnapshot(msg.CommandIndex)
			}

			if ch, ok := kv.waitChs[msg.CommandIndex]; ok {
				ch <- reply
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex > kv.lastApplied {
				kv.applySnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) doClientRequest(op *Op, reply *OpReply) {
	si := key2shard(op.Key)
	if kv.currConfig.Shards[si] != kv.gid || kv.shards[si].Status != Serving {
		reply.Err = ErrWrongGroup
		return
	}
	if op.SeqNum > kv.lastSeq[op.Client] {
		if op.OpType == PutOp {
			kv.shards[si].KVmap[op.Key] = op.Value
		} else if op.OpType == AppendOp {
			kv.shards[si].KVmap[op.Key] += op.Value
		}
		kv.lastSeq[op.Client] = op.SeqNum
	}
}

// Update the latest configuration by setting shard Status; not responsible data migration.
// If we lose a shard, stop serving requests to keys in that shard immediately.
// If we gain a shard, wait for the previous owner to send over the old shard data.
func (kv *ShardKV) doConfigUpdate(op *Op) {
	oldConfig := kv.currConfig
	newConfig := op.Config
	if newConfig.Num < oldConfig.Num {
		return
	}
	for si, oldgid := range oldConfig.Shards {
		newgid := newConfig.Shards[si]
		if oldgid == newgid {
			continue
		}
		if newgid == kv.gid {
			// gain a shard
			if oldgid == 0 {
				kv.shards[si].Status = Serving
			} else {
				kv.shards[si].Status = Waiting
			}
		} else if oldgid == kv.gid {
			// shard moves to another replica group
			kv.shards[si].Status = Leaving
		}
	}
	kv.prevConfig = oldConfig
	kv.currConfig = newConfig
}

func (kv *ShardKV) doShardJoin(op *Op) {
	si := op.ShardID
	if op.Shard.ConfigNum < kv.shards[si].ConfigNum {
		return
	}
	kv.shards[si].ConfigNum = op.Shard.ConfigNum
	kv.shards[si].KVmap = copyMap(op.Shard.KVmap)
	for client, seqnum := range op.LastSeq {
		if seq, ok := kv.lastSeq[client]; !ok || seq < seqnum {
			kv.lastSeq[client] = seqnum
		}
	}
	kv.shards[si].Status = Serving
}

func (kv *ShardKV) doShardLeave(op *Op) {
	si := op.ShardID
	if kv.shards[si].Status != Leaving {
		return
	}
	kv.shards[si].KVmap = make(map[string]string)
	kv.shards[si].ConfigNum = op.Shard.ConfigNum
	kv.shards[si].Status = Removed
}


func (kv *ShardKV) updateConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		if !kv.isUpdated() {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		configNum := kv.currConfig.Num
		kv.mu.Unlock()

		// ask the shard controller for the latest configuration
		newConfig := kv.shardClerk.Query(configNum + 1)
		if newConfig.Num != configNum + 1 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// detected a new configuration; send to Raft for replication
		op := Op{OpType: Update, Config: newConfig, Client: int64(kv.gid), SeqNum: int64(newConfig.Num)}
		index, _, isLeader := kv.rf.Start(op)
		if isLeader {
			result := kv.waitChannel(op, index)
			if result.Err == OK {
				kv.migrateShards()
			}
		}

		// poll the shard controller every 100 ms
		time.Sleep(100 * time.Millisecond)
	}
}

// Start the shard migration process when a new configuration is detected.
func (kv *ShardKV) migrateShards() {
	kv.mu.Lock()
	shardsToMove := make([]int, 0)
	for si, shard := range kv.shards {
		if shard.Status == Leaving && kv.prevConfig.Shards[si] == kv.gid {
			shardsToMove = append(shardsToMove, si)
		}
	}
	lastSeq := copyMap(kv.lastSeq)
	kv.mu.Unlock()

	for _, si := range shardsToMove {
		kv.mu.Lock()
		data := Shard{KVmap: copyMap(kv.shards[si].KVmap), ConfigNum: kv.shards[si].ConfigNum}
		gid := kv.currConfig.Shards[si]
		servers := kv.currConfig.Groups[gid]
		kv.mu.Unlock()

		args := MoveArgs{Shard: data, ShardID: si, LastSeq: lastSeq}
		go kv.sendShard(&args, servers)
	}
}

func (kv *ShardKV) sendShard(args *MoveArgs, group []string) {
	for !kv.killed() {
		for _, srv := range group {
			dest := kv.make_end(srv)
			reply := &MoveReply{}
			ok := dest.Call("ShardKV.MoveShard", args, reply)
			if ok && reply.Err == OK {
				go kv.removeShard(args.ShardID, args.Shard.ConfigNum)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) removeShard(shardID int, configNum int) {
	op := Op{OpType: Leave, ShardID: shardID, Shard: Shard{ConfigNum: configNum}}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.waitChannel(op, index)
	}
}

func (kv *ShardKV) isUpdated() bool {
	for si, shard := range kv.shards {
		if kv.currConfig.Shards[si] == kv.gid && shard.Status != Serving {
			return false
		}
	}
	return true
}


func (kv *ShardKV) createSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shards) != nil || e.Encode(kv.lastSeq) != nil {
		return
	}
	if e.Encode(kv.prevConfig) != nil || e.Encode(kv.currConfig) != nil {
		return
	}
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shards []Shard
	var lastSeq map[int64]int64
	var prevConfig shardctrler.Config
	var currConfig shardctrler.Config

	if d.Decode(&shards) != nil || d.Decode(&lastSeq) != nil {
		return
	}
	if d.Decode(&prevConfig) != nil || d.Decode(&currConfig) != nil {
		return
	}
	kv.shards = shards
	kv.lastSeq = lastSeq
	kv.prevConfig = prevConfig
	kv.currConfig = currConfig
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
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
	kv.shards = make([]Shard, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shards[i] = Shard{KVmap: make(map[string]string), ConfigNum: 0}
	}

	// Use something like this to talk to the shardctrler:
	kv.shardClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.lastSeq = make(map[int64]int64)
	kv.waitChs = make(map[int]chan OpReply)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applySnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.updateConfig()

	return kv
}
