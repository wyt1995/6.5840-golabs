package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)


// The shard controller is implemented as a fault-tolerant service using Raft.
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	configs []Config // indexed by config num

	// Your data here.
	lastSeq map[int64]int64
	waitChs map[int]chan OpReply
}


// The ShardCtrler submits any Join/Leave/Move/Query operation to Raft.
// Op and OpReply are used for communication via the applyCh.
type Op struct {
	// Your data here.
	OpType  string
	Client  int64
	SeqNum  int64

	// arguments sent by the client
	Servers map[int][]string // Join
	GIDs    []int            // Leave
	Shard   int              // Move
	GID     int              // Move
	Num     int              // Query
}

type OpReply struct {
	Err    Err
	Config Config // for Query RPC
	Client int64
	SeqNum int64
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SeqNum <= sc.lastSeq[args.Client] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	sc.mu.Unlock()

	op := Op{OpType: join, Client: args.Client, SeqNum: args.SeqNum, Servers: args.Servers}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	raftReply := sc.waitChannel(op, index)
	reply.Err = raftReply.Err
	reply.WrongLeader = raftReply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SeqNum <= sc.lastSeq[args.Client] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	sc.mu.Unlock()

	op := Op{OpType: leave, Client: args.Client, SeqNum: args.SeqNum, GIDs: args.GIDs}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	raftReply := sc.waitChannel(op, index)
	reply.Err = raftReply.Err
	reply.WrongLeader = raftReply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SeqNum <= sc.lastSeq[args.Client] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	sc.mu.Unlock()

	op := Op{OpType: move, Client: args.Client, SeqNum: args.SeqNum, Shard: args.Shard, GID: args.GID}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	raftReply := sc.waitChannel(op, index)
	reply.Err = raftReply.Err
	reply.WrongLeader = raftReply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.SeqNum <= sc.lastSeq[args.Client] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	sc.mu.Unlock()

	op := Op{OpType: query, Client: args.Client, SeqNum: args.SeqNum, Num: args.Num}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	raftReply := sc.waitChannel(op, index)
	reply.Err = raftReply.Err
	reply.WrongLeader = raftReply.Err == ErrWrongLeader
	reply.Config = raftReply.Config
}

func (sc *ShardCtrler) doJoin(op *Op) {
	cfg := sc.copyConfig()
	for gid, srv := range op.Servers {
		cfg.Groups[gid] = append([]string(nil), srv...)
	}
	sc.rebalance(&cfg)
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) doLeave(op *Op) {
	cfg := sc.copyConfig()
	for _, gid := range op.GIDs {
		delete(cfg.Groups, gid)
		for i := 0; i < NShards; i++ {
			if cfg.Shards[i] == gid {
				cfg.Shards[i] = 0
			}
		}
	}
	sc.rebalance(&cfg)
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) doMove(op *Op) {
	cfg := sc.copyConfig()
	if _, exists := cfg.Groups[op.GID]; exists && op.Shard >= 0 && op.Shard < NShards {
		cfg.Shards[op.Shard] = op.GID
	}
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) doQuery(op *Op) Config {
	if op.Num < 0 || op.Num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[op.Num]
}

// util functions
func (sc *ShardCtrler) copyConfig() Config {
	curr := sc.configs[len(sc.configs)-1]
	next := Config{
		Num:    curr.Num + 1,
		Shards: curr.Shards,
		Groups: make(map[int][]string),
	}
	for gid, srv := range curr.Groups {
		next.Groups[gid] = append([]string(nil), srv...)
	}
	return next
}

func (sc *ShardCtrler) shardCount(cfg *Config) map[int]int {
	count := make(map[int]int)
	for _, gid := range cfg.Shards {
		count[gid]++
	}
	return count
}

func (sc *ShardCtrler) rebalance(cfg *Config) {
	numGroups := len(cfg.Groups)
	if numGroups == 0 {
		cfg.Shards = [NShards]int{0}
		return
	}

	counts := sc.shardCount(cfg)
	gids := make([]int, 0, numGroups)
	for gid := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// handle unassigned shards
	if unassigned, exists := counts[0]; exists && unassigned > 0 {
		j := 0
		for i := 0; i < NShards; i++ {
			if cfg.Shards[i] == 0 {
				cfg.Shards[i] = gids[j]
				j = (j + 1) % len(gids)
			}
		}
		// recalculate counts after distributing unassigned shards
		counts = sc.shardCount(cfg)
	}

	// the target number of shards in each group
	avg := NShards / numGroups
	rem := NShards % numGroups
	target := make(map[int]int, numGroups)
	for i, gid := range gids {
		if i < rem {
			target[gid] = avg + 1
		} else {
			target[gid] = avg
		}
	}

	var overloaded, underloaded []int
	for _, gid := range gids {
		if counts[gid] > target[gid] {
			overloaded = append(overloaded, gid)
		} else if counts[gid] < target[gid] {
			underloaded = append(underloaded, gid)
		}
	}
	if len(overloaded) == 0 && len(underloaded) == 0 {
		return
	}
	sort.Ints(overloaded)
	sort.Ints(underloaded)

	for _, gid := range overloaded {
		excess := counts[gid] - target[gid]
		for i := 0; i < NShards && excess > 0; i++ {
			if cfg.Shards[i] == gid && len(underloaded) > 0 {
				dst := underloaded[0]
				cfg.Shards[i] = dst
				counts[gid]--
				counts[dst]++
				excess--
				if counts[dst] == target[dst] {
					underloaded = underloaded[1:]
				}
			}
		}
	}
}


func (sc *ShardCtrler) waitChannel(op Op, raftIndex int) OpReply {
	ch := make(chan OpReply, 1)
	sc.mu.Lock()
	sc.waitChs[raftIndex] = ch
	sc.mu.Unlock()

	var reply OpReply
	select {
	case reply = <-ch:
		if reply.Client != op.Client || reply.SeqNum != reply.SeqNum {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go func(index int) {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.waitChs, index)
	}(raftIndex)

	return reply
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)
		sc.mu.Lock()
		if op.SeqNum <= sc.lastSeq[op.Client] {
			sc.mu.Unlock()
			continue
		}
		sc.lastSeq[op.Client] = op.SeqNum

		reply := OpReply{Err: OK, Client: op.Client, SeqNum: op.SeqNum}
		switch op.OpType {
		case join:
			sc.doJoin(&op)
		case leave:
			sc.doLeave(&op)
		case move:
			sc.doMove(&op)
		case query:
			reply.Config = sc.doQuery(&op)
		}

		if ch, ok := sc.waitChs[msg.CommandIndex]; ok {
			ch <- reply
		}
		sc.mu.Unlock()
	}
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Shards = [NShards]int{}
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastSeq = make(map[int64]int64)
	sc.waitChs = make(map[int]chan OpReply)

	go sc.applier()

	return sc
}
