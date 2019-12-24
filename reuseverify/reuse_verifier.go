package reuseverify

import (
	"bytes"
	"encoding/json"
	"github.com/ethereum/go-ethereum/cmpreuse"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"time"
)

type IBlockProcessor interface {
	ApplyTransaction(tx *types.Transaction) (receipt *types.Receipt, err error)
}

/* blockEnv -- Flyweight Pattern: The environment needed for block processors */

type blockEnv struct {
	config  *params.ChainConfig
	bc      core.ChainContext
	header  *types.Header
	gp      *core.GasPool
	usedGas *uint64
	cfg     vm.Config
	hash    common.Hash
}

/* BlockProcessor */

type BlockProcessor struct {
	*blockEnv
	statedb *state.StateDB
}

func (p *BlockProcessor) Prepare(i int, tx *types.Transaction) {
	p.statedb.Prepare(tx.Hash(), p.hash, i)
}

func (p *BlockProcessor) ApplyTransaction(tx *types.Transaction) (receipt *types.Receipt, err error) {
	receipt, err = core.ApplyTransaction(p.config, p.bc, nil, p.gp, p.statedb, p.header, tx, p.usedGas, p.cfg)
	return
}

/* RWRecordBlockProcessor */

type RWRecordBlockProcessor struct {
	*blockEnv
	statedb *state.StateDB
}

func NewRWRecordBlockProcessor(e *blockEnv, statedb *state.StateDB) *RWRecordBlockProcessor {
	db := state.NewRWStateDB(statedb)
	db.EnableFeeToCoinbase = true // to make exact state

	return &RWRecordBlockProcessor{
		blockEnv: e,
		statedb:  db,
	}
}

func (p *RWRecordBlockProcessor) Prepare(i int, tx *types.Transaction) {
	p.statedb.SetRWMode(true)
	p.statedb.Prepare(tx.Hash(), p.hash, i)
}

func (p *RWRecordBlockProcessor) ApplyTransaction(tx *types.Transaction) (receipt *types.Receipt, err error) {
	receipt, err = core.ApplyTransaction(p.config, p.bc, nil, p.gp, p.statedb, p.header, tx, p.usedGas, p.cfg)
	return
}

func (p *RWRecordBlockProcessor) getRWRecord() *cache.RWRecord {
	rstate, rchain, wstate, radd, _ := p.statedb.RWRecorder().RWDump()
	rw := cache.NewRWRecord(rstate, rchain, wstate, make(state.ObjectMap), radd, false)
	return rw
}

/* ReuseVerifier */

type ReuseVerifier struct {
	config *params.ChainConfig // Chain configuration options
	bc     *core.BlockChain    // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards

	exitCh chan struct{}
}

func NewReuseVerifier(config *params.ChainConfig, bc *core.BlockChain, engine consensus.Engine) *ReuseVerifier {
	return &ReuseVerifier{
		config: config,
		bc:     bc,
		engine: engine,

		exitCh: make(chan struct{}),
	}
}

func (v *ReuseVerifier) Close() {
	close(v.exitCh)
}

func (v *ReuseVerifier) createEnv(block *types.Block, cfg vm.Config) *blockEnv {
	return &blockEnv{
		config:  v.config,
		bc:      v.bc,
		header:  block.Header(),
		gp:      new(core.GasPool).AddGas(block.GasLimit()),
		usedGas: new(uint64),
		cfg:     cfg,
		hash:    block.Hash(),
	}
}

func (v *ReuseVerifier) TestBlock(block *types.Block, statedb *state.StateDB, cfg vm.Config) bool {
	header := block.Header()

	// NOTE: DAOFork not considered

	testDb := statedb.Copy()

	p1 := NewRWRecordBlockProcessor(v.createEnv(block, cfg), statedb.Copy())
	p0 := BlockProcessor{v.createEnv(block, cfg), statedb}

	for i, tx := range block.Transactions() {
		p1.Prepare(i, tx)
		receipt1, err := p1.ApplyTransaction(tx)
		if err != nil {
			log.Info("ReuseVerifier processBlock p1 Error", "err", err.Error(),
				"block", block.NumberU64(), "i", i, "txHash", tx.Hash())
			return false
		}

		rw := p1.getRWRecord()

		if !cmpreuse.CheckRChain(rw, v.bc, header) {
			log.Info("ReuseVerifier !CheckRChain",
				"block", block.NumberU64(), "i", i, "txHash", tx.Hash())
			jsonStr, err := json.Marshal(rw.Dump())
			if err == nil {
				log.Info("CheckRChain miss record dump: " + string(jsonStr))
			}
		}

		if !cmpreuse.CheckRState(rw, statedb, true) {
			log.Info("ReuseVerifier !CheckRState",
				"block", block.NumberU64(), "i", i, "txHash", tx.Hash())

			jsonStr, err := json.Marshal(rw.Dump())
			if err == nil {
				log.Info("CheckRState miss record dump: " + string(jsonStr))
			}
		}

		p0.Prepare(i, tx)
		receipt0, err := p0.ApplyTransaction(tx)
		if err != nil {
			log.Info("ReuseVerifier processBlock p0 Error", "err", err.Error(),
				"block", block.NumberU64(), "i", i, "txHash", tx.Hash())
			return false
		}

		var root0 []byte
		var root1 []byte

		if v.config.IsByzantium(header.Number) {
			root0 = state.IntermediateRootCalc(p0.statedb).Bytes()
			root1 = state.IntermediateRootCalc(p1.statedb).Bytes()
		} else {
			root0 = receipt0.PostState
			root1 = receipt1.PostState
		}

		if root0 == nil {
			log.Info("ReuseVerifier root0 empty")
		}

		if bytes.Compare(root0, root1) != 0 {
			log.Info("ReuseVerifier root mismatch",
				"block", block.NumberU64(), "i", i, "txHash", tx.Hash())
			return false
		}

		cmpreuse.ApplyRWRecord(testDb, rw, cmpreuse.AlwaysFalse)

		testDb.Finalise(true)
		root2 := state.IntermediateRootCalc(testDb).Bytes()

		if bytes.Compare(root0, root2) != 0 {
			log.Info("ReuseVerifier applied root mismatch",
				"block", block.NumberU64(), "i", i, "txHash", tx.Hash())
			return false
		}
	}

	return true
}

func (v *ReuseVerifier) DoBlocksTest(startInclusive, endExclusive uint64) {
	var lastState *state.StateDB = nil
	var lastHash common.Hash

	for i := startInclusive - 1; i < endExclusive; i += 1 {
		select {
		case <-v.exitCh:
			log.Info("DoBlocksTest shutdown", "i", i)

			return
		default:
			block := v.bc.GetBlockByNumber(i)

			if block == nil {
				lastState = nil
				lastHash = common.Hash{}

				log.Warn("DoBlocksTest Block not found", "block", i)
				time.Sleep(1 * time.Second)
				continue
			}

			if lastHash != (common.Hash{}) && block.ParentHash() != lastHash {
				log.Error("DoBlocksTest ParentHash mismatch", "block", "block", block.NumberU64(), "hash", block.Hash(), "lastHash", lastHash)
			}

			curState, curErr := v.bc.StateAt(block.Root())
			if lastState != nil {
				stateDB := lastState
				if len(block.Transactions()) == 0 {
					log.Info("DoBlocksTest on Empty", "block", block.NumberU64(), "hash", block.Hash())
				} else {
					if !v.TestBlock(block, stateDB, *v.bc.GetVMConfig()) {
						log.Warn("🤮 DoBlocksTest failed", "block", block.NumberU64(), "hash", block.Hash(), "curState", curErr == nil)
					} else {
						log.Info("DoBlocksTest Succeed", "block", block.NumberU64(), "hash", block.Hash(), "curState", curErr == nil)

						v.myFinializeBlock(block, stateDB)
						root0 := stateDB.IntermediateRoot(v.config.IsEIP158(block.Number()))
						if root0 != block.Root() {
							log.Error("DoBlocksTest block root mismatch", "block", block.NumberU64(), "hash", block.Hash())
						}
					}
				}
			} else if i >= startInclusive {
				log.Warn("DoBlocksTest skipped no statedb", "block", block.NumberU64(), "hash", block.Hash())
				time.Sleep(1 * time.Second)
			}

			lastHash = block.Hash()
			if curErr != nil {
				lastState = nil
			} else {
				lastState = curState
			}
		}
	}
}

var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

func (v *ReuseVerifier) myFinializeBlock(block *types.Block, stateDB *state.StateDB) {
	header := block.Header()

	// Select the correct block reward based on chain progression
	blockReward := ethash.FrontierBlockReward
	if v.config.IsByzantium(header.Number) {
		blockReward = ethash.ByzantiumBlockReward
	}
	if v.config.IsConstantinople(header.Number) {
		blockReward = ethash.ConstantinopleBlockReward
	}
	// Accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)
	for _, uncle := range block.Uncles() {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		stateDB.AddBalance(uncle.Coinbase, r)

		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}
	stateDB.AddBalance(header.Coinbase, reward)
}
