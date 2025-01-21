package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	Op     string // "Put" or "Append"
	Client int64
	SeqNum int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key    string
	Client int64
	SeqNum int64
}

type GetReply struct {
	Err   Err
	Value string
}
