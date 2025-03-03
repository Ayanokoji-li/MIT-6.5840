package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VersionedValue struct {
	version rpc.Tversion
	value   string
}

type KVServer struct {
	mu         sync.Mutex
	kv_storage map[string]VersionedValue
	// Your definitions here.
}

func (kv *KVServer) updateStorage(key string, value string, cur_version rpc.Tversion) {
	kv.kv_storage[key] = VersionedValue{value: value, version: cur_version + 1}
}

func (kv *KVServer) getStorage(key string) (VersionedValue, bool) {
	vv, exist := kv.kv_storage[key]

	return vv, exist
}

func MakeKVServer() *KVServer {
	kv := &KVServer{kv_storage: make(map[string]VersionedValue)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	key := args.Key

	kv.mu.Lock()
	vv, exist := kv.getStorage(key)
	kv.mu.Unlock()

	if exist {
		reply.Value = vv.value
		reply.Version = vv.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}

	log.Printf("[Server]: handle Get rpc, args: %v reply: %v", args, reply)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	input_version := args.Version
	input_value := args.Value
	key := args.Key

	kv.mu.Lock()
	defer kv.mu.Unlock()
	vv, exist := kv.getStorage(key)

	if !exist {
		if input_version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
	} else {
		cur_version := vv.version

		if cur_version != input_version {
			reply.Err = rpc.ErrVersion
			return
		}
	}

	reply.Err = rpc.OK
	kv.updateStorage(key, input_value, input_version)
	log.Printf("[Server]: handle Put rpc, args: %v reply: %v", args, reply)
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
