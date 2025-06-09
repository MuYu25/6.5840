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

type KVServer struct {
	mu     sync.Mutex
	Map    map[string]string
	record sync.Map
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.Map[args.Key]
	if ok {
		reply.Value = val
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
		return
	}
	res, ok := kv.record.Load(args.MessageID)
	if ok {
		reply.Value = res.(string)
		return
	}
	kv.mu.Lock()
	old := kv.Map[args.Key]
	kv.Map[args.Key] = args.Value
	reply.Value = old
	kv.mu.Unlock()
	kv.record.Store(args.MessageID, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
		return
	}
	res, ok := kv.record.Load(args.MessageID)
	if ok {
		reply.Value = res.(string)
		return
	}

	kv.mu.Lock()
	old := kv.Map[args.Key]
	kv.Map[args.Key] = old + args.Value
	reply.Value = old
	kv.mu.Unlock()
	kv.record.Store(args.MessageID, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.Map = make(map[string]string)
	// You may need initialization code here.

	return kv
}
