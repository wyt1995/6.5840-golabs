package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Recent struct {
	request int
	value   string
}

type KVServer struct {
	mu    sync.Mutex
	kvmap map[string]string
	cache map[int64]Recent
	// cache the most recent request from each client to handle duplicate
	// assume that a client will make only one call to a Clerk at a time
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// duplicate detection
	if recent, exists := kv.cache[args.Client]; exists {
		if recent.request == args.Request {
			reply.Value = recent.value
			return
		}
	}

	value, ok := kv.kvmap[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if recent, exists := kv.cache[args.Client]; exists {
		if recent.request == args.Request {
			return
		}
	}

	kv.kvmap[args.Key] = args.Value
	kv.cache[args.Client] = Recent{args.Request, ""}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if recent, exists := kv.cache[args.Client]; exists {
		if recent.request == args.Request {
			reply.Value = recent.value
			return
		}
	}

	oldValue, ok := kv.kvmap[args.Key]
	if !ok {
		kv.kvmap[args.Key] = args.Value
	} else {
		kv.kvmap[args.Key] = oldValue + args.Value
	}
	reply.Value = oldValue
	kv.cache[args.Client] = Recent{args.Request, oldValue}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvmap = make(map[string]string)
	kv.cache = make(map[int64]Recent)

	return kv
}
