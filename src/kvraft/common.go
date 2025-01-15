package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

const (
	GetOp    = "Get"
	PutOp    = "Put"
	AppendOp = "Append"
)

// Put or Append
type PutAppendArgs struct {
	Operation string
	Key       string
	Value     string
	Client    int64
	SeqNum    int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key    string
	Client int64
	SeqNum int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   string
	Value string
}
