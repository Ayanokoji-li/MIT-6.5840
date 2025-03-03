package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockName string
	holdVal  string
}

const (
	IDEL   = "idel"
	HOLDED = "holded"
)

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}

	ok := lk.ck.Put(l, IDEL, 0)
	for ok == rpc.ErrMaybe {
		ok = lk.ck.Put(l, IDEL, 0)
	}

	lk.lockName = l
	lk.holdVal = HOLDED + kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	log.Printf("[lock]: try to acquire lock %s", lk.lockName)
	success := false

	for !success {
		value, version, ok := lk.ck.Get(lk.lockName)

		success = (ok == rpc.OK && value == IDEL)

		if !success {
			continue
		}

		ok = lk.ck.Put(lk.lockName, lk.holdVal, version)

		success = ok == rpc.OK

		if !success && ok == rpc.ErrMaybe {
			value, _, ok := lk.ck.Get(lk.lockName)

			success = ok == rpc.OK && value == lk.holdVal
		}
	}

	log.Printf("[lock]: acquire lock %s successfully", lk.lockName)
}

func (lk *Lock) Release() {
	// Your code here
	log.Printf("[lock]: try to release lock %s", lk.lockName)
	success := false
	for !success {
		value, version, ok := lk.ck.Get(lk.lockName)

		success = (ok == rpc.OK && value == lk.holdVal)

		if !success {
			break
		}

		ok = lk.ck.Put(lk.lockName, IDEL, version)

		success = ok == rpc.OK

		if !success && ok == rpc.ErrMaybe {
			_, cur_version, ok := lk.ck.Get(lk.lockName)

			success = ok == rpc.OK && cur_version > version
		}
	}

	log.Printf("[lock]: release lock %s successfully", lk.lockName)

}
