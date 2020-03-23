package rawdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

type EmulateHook interface {
	SetChainDB(db ethdb.Reader)

	IsEmulateMode() bool

	HideBlock(hash common.Hash, number uint64) bool

	ReadEmulateHash(number uint64) (hash common.Hash, overrides bool)

	WriteEmulateHash(hash common.Hash, number uint64) (overrides bool)

	DeleteEmulateHash(number uint64) (overrides bool)

	HeadHeaderHash() common.Hash

	SetHeadHeaderHash(headHeaderHash common.Hash) (overrides bool)

	HeadBlockHash() common.Hash

	SetHeadBlockHash(headBlockHash common.Hash) (overrides bool)

	HeadFastBlockHash() common.Hash

	SetHeadFastBlockHash(headFastHash common.Hash) (overrides bool)

	IsBlockInEmulateRange(number uint64) bool

	PrefixKey(ret []byte) []byte

	PutExecutedBlock(hash common.Hash, number uint64)

	EmulateFrom() uint64

	SetEmulateFrom(uint64)
}

var GlobalEmulateHook EmulateHook = &EmptyEmulateHook{}

type EmptyEmulateHook struct {
}

func (e EmptyEmulateHook) SetEmulateFrom(uint64) {
}

func (e EmptyEmulateHook) SetChainDB(_ ethdb.Reader) {
}

func (e EmptyEmulateHook) HeadHeaderHash() common.Hash {
	return common.Hash{}
}

func (e EmptyEmulateHook) SetHeadHeaderHash(_ common.Hash) (overrides bool) {
	return false
}

func (e EmptyEmulateHook) HeadBlockHash() common.Hash {
	return common.Hash{}
}

func (e EmptyEmulateHook) SetHeadBlockHash(_ common.Hash) (overrides bool) {
	return false
}

func (e EmptyEmulateHook) HeadFastBlockHash() common.Hash {
	return common.Hash{}
}

func (e EmptyEmulateHook) SetHeadFastBlockHash(_ common.Hash) (overrides bool) {
	return false
}

func (e EmptyEmulateHook) IsEmulateMode() bool {
	return false
}

func (e EmptyEmulateHook) HideBlock(_ common.Hash, _ uint64) bool {
	return false
}

func (e EmptyEmulateHook) ReadEmulateHash(_ uint64) (hash common.Hash, overrides bool) {
	return common.Hash{}, false
}

func (e EmptyEmulateHook) WriteEmulateHash(_ common.Hash, _ uint64) (overrides bool) {
	return false
}

func (e EmptyEmulateHook) DeleteEmulateHash(_ uint64) (overrides bool) {
	return false
}

func (e EmptyEmulateHook) IsBlockInEmulateRange(_ uint64) bool {
	return false
}

func (e EmptyEmulateHook) PrefixKey(ret []byte) []byte {
	return ret
}

func (e EmptyEmulateHook) PutExecutedBlock(_ common.Hash, _ uint64) {
}

func (e EmptyEmulateHook) EmulateFrom() uint64 {
	return 0
}

type emulateHookImpl struct {
	emulateStartBlockNum uint64
	executedBlocks       map[common.Hash]struct{}
	emulateNumberHashMap map[uint64]common.Hash

	headHeaderHash    common.Hash
	headBlockHash     common.Hash
	headFastBlockHash common.Hash
}

func NewEmulateHook(emulateStartBlockNum uint64) EmulateHook {
	ec := &emulateHookImpl{
		emulateStartBlockNum: emulateStartBlockNum, // root must not be emulated
		executedBlocks:       make(map[common.Hash]struct{}),
		emulateNumberHashMap: make(map[uint64]common.Hash),

		headHeaderHash:    common.Hash{},
		headBlockHash:     common.Hash{},
		headFastBlockHash: common.Hash{},
	}

	return ec
}

func (ec *emulateHookImpl) SetChainDB(db ethdb.Reader) {
	number := ec.emulateStartBlockNum

	// read real canonical hash
	data, _ := db.Get(headerHashKey(number))
	if len(data) == 0 {
		data, _ = db.Ancient(freezerHashTable, number)
	}
	var hash common.Hash
	if len(data) == 0 {
		hash = common.Hash{}
	} else {
		hash = common.BytesToHash(data)
	}

	head := ReadBlock(db, hash, number)
	ec.repairIfNecessary(db, &head)

	hash = head.Hash()
	number = head.NumberU64()

	ec.headHeaderHash = hash
	ec.headBlockHash = hash
	ec.headFastBlockHash = hash

	if ec.emulateStartBlockNum != number {
		log.Error("!!!Setting emulate mode to different number!!!", "number", number)
	}
	ec.emulateStartBlockNum = number
}

func (ec *emulateHookImpl) repairIfNecessary(db ethdb.Reader, head **types.Block) {
	var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	for {
		root := (*head).Root()
		if root != (common.Hash{}) && root != emptyRoot {
			hash := common.BytesToHash(root[:])

			enc, err := db.Get(hash[:])
			if err != nil || enc == nil {
				// continue seeking root
			} else {
				return // succeed
			}
		}

		// Otherwise rewind one block and recheck state availability there
		block := ReadBlock(db, (*head).ParentHash(), (*head).NumberU64()-1)
		if block == nil {
			panic(fmt.Errorf("missing block %d [%x]", (*head).NumberU64()-1, (*head).ParentHash()))
		}
		*head = block
	}
}

func (ec *emulateHookImpl) IsEmulateMode() bool {
	return true
}

func (ec *emulateHookImpl) IsBlockInEmulateRange(number uint64) bool {
	if number > ec.emulateStartBlockNum {
		return true
	}
	return false
}

func (ec *emulateHookImpl) HideBlock(hash common.Hash, number uint64) bool {
	if ec.IsBlockInEmulateRange(number) {
		if _, ok := ec.executedBlocks[hash]; !ok {
			return true
		}
	}
	return false
}

func (ec *emulateHookImpl) ReadEmulateHash(number uint64) (hash common.Hash, overrides bool) {
	if !ec.IsBlockInEmulateRange(number) {
		return common.Hash{}, false
	}

	if h, ok := ec.emulateNumberHashMap[number]; ok {
		return h, true
	}
	return common.Hash{}, true
}

func (ec *emulateHookImpl) WriteEmulateHash(hash common.Hash, number uint64) (overrides bool) {
	if !ec.IsBlockInEmulateRange(number) {
		return false
	}

	ec.emulateNumberHashMap[number] = hash
	return true
}

func (ec *emulateHookImpl) DeleteEmulateHash(number uint64) (overrides bool) {
	if !ec.IsBlockInEmulateRange(number) {
		return false
	}

	delete(ec.emulateNumberHashMap, number)
	return true
}

func (ec *emulateHookImpl) HeadHeaderHash() common.Hash {
	return ec.headHeaderHash
}

func (ec *emulateHookImpl) SetHeadHeaderHash(headHeaderHash common.Hash) (overrides bool) {
	ec.headHeaderHash = headHeaderHash
	return true
}

func (ec *emulateHookImpl) HeadBlockHash() common.Hash {
	return ec.headBlockHash
}

func (ec *emulateHookImpl) SetHeadBlockHash(headBlockHash common.Hash) (overrides bool) {
	ec.headBlockHash = headBlockHash
	return true
}

func (ec *emulateHookImpl) HeadFastBlockHash() common.Hash {
	return ec.headFastBlockHash
}

func (ec *emulateHookImpl) SetHeadFastBlockHash(headFastHash common.Hash) (overrides bool) {
	ec.headFastBlockHash = headFastHash
	return true
}

func (ec *emulateHookImpl) PrefixKey(ret []byte) []byte {
	return append([]byte("msra-emulate-"), ret...)
}

func (ec *emulateHookImpl) PutExecutedBlock(hash common.Hash, number uint64) {
	if ec.IsBlockInEmulateRange(number) {
		ec.executedBlocks[hash] = struct{}{}
	} else {
		log.Warn("PutExecutedBlock called outside of emulate range", "hash=", hash, "number=", number)
	}
}

func (ec *emulateHookImpl) EmulateFrom() uint64 {
	return ec.emulateStartBlockNum
}

func (ec *emulateHookImpl) SetEmulateFrom(from uint64) {
	ec.emulateStartBlockNum = from
}
