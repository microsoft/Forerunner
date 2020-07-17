package optipreplayer

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
)

const (
	commitInterruptNone int32 = iota
	commitInterruptNewHead
	commitInterruptResubmit
	commitInterruptExit
)

var (
	defaultGasLimit = 9000000
)

// preplayers is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing cache.Result.
type Executor struct {
	id      string
	config  *params.ChainConfig
	engine  consensus.Engine
	chainDb ethdb.Database
	chain   *core.BlockChain

	trigger *Trigger
	current *environment // An environment for current running cycle.

	txnOrder   map[common.Hash]int
	rawPending map[common.Address]types.Transactions

	executionOrder []*types.Transaction

	pendingTxsMap map[common.Hash]*types.Transaction // Snapshot of pending transactions
	resultMap     map[common.Hash]*cache.ExtraResult // Snapshot of preplreay cache.Result
	currentState  *cache.CurrentState
	resultCh      chan *cache.ExtraResult

	RoundID uint64

	EnableReuseTracer bool

	enableAgg   bool
	enablePause bool

	basicPreplay bool
	addrNotCopy  map[common.Address]struct{}
}

func NewExecutor(id string, config *params.ChainConfig, engine consensus.Engine, chain *core.BlockChain, chainDb ethdb.Database, txnOrder map[common.Hash]int,
	rawPending map[common.Address]types.Transactions, currentState *cache.CurrentState, trigger *Trigger, resultCh chan *cache.ExtraResult,
	enableAgg bool, enablePause bool, basicPreplay bool, addrNotCopy map[common.Address]struct{}) *Executor {
	executor := &Executor{
		id:             id,
		config:         config,
		engine:         engine,
		chain:          chain,
		chainDb:        chainDb,
		trigger:        trigger,
		txnOrder:       txnOrder,
		rawPending:     rawPending,
		executionOrder: []*types.Transaction{},
		pendingTxsMap:  make(map[common.Hash]*types.Transaction),
		resultMap:      make(map[common.Hash]*cache.ExtraResult),
		currentState:   currentState,
		resultCh:       resultCh,
		enableAgg:      enableAgg,
		enablePause:    enablePause,
		basicPreplay:   basicPreplay,
		addrNotCopy:    addrNotCopy,
	}

	for _, txs := range rawPending {
		for _, tx := range txs {
			executor.pendingTxsMap[tx.Hash()] = tx
		}
	}

	return executor
}

// environment is the preplayers's current environment and holds all of the current state information.
type environment struct {
	signer types.Signer

	state     *state.StateDB // apply state changes here
	ancestors mapset.Set     // ancestor isFetchEnd (used for checking uncle parent validity)
	family    mapset.Set     // family isFetchEnd (used for checking uncle invalidity)
	uncles    mapset.Set     // uncle isFetchEnd
	tcount    int            // Tx count in cycle
	gasPool   *core.GasPool  // available gas used to pack transactions

	Header   *types.Header        `json:"header"`
	Txs      []*types.Transaction `json:"txs"`
	Receipts []*types.Receipt     `json:"receipts"`
}

// makeCurrent creates a new environment for the current cycle.
func (e *Executor) makeCurrent(parent *types.Block, header *types.Header) error {

	var statedb *state.StateDB
	statedbInstance, err := e.chain.StateAt(parent.Root())
	if statedbInstance == nil {
		panic(fmt.Sprintf("Get nil statedb parent: %d-%s-%s", parent.NumberU64(), parent.Hash().Hex(), parent.Root().Hex()))
	}
	if e.chain.GetVMConfig().MSRAVMSettings.EnablePreplay {
		statedb = state.NewRWStateDB(statedbInstance)
		if !e.basicPreplay {
			statedb.SetAllowObjCopy(false)
		}
		statedb.SetAddrNotCopy(e.addrNotCopy)
		if e.chain.GetVMConfig().MSRAVMSettings.ParallelizeReuse {
			statedb.SetCopyForShare(true)
		}
	} else {
		statedb = statedbInstance
	}
	if err != nil {
		return err
	}
	env := &environment{
		signer:    types.NewEIP155Signer(e.config.ChainID),
		state:     statedb,
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
		Header:    header,
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range e.chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}

	// Keep track of transactions which return errors so they can be removed
	env.tcount = 0
	e.current = env
	return nil
}

// commitUncle adds the given block to uncle block isFetchEnd, returns error if failed to add.
func (e *Executor) commitUncle(env *environment, uncle *types.Header) error {
	hash := uncle.Hash()
	if env.uncles.Contains(hash) {
		return errors.New("uncle not unique")
	}
	if env.Header.ParentHash == uncle.ParentHash {
		return errors.New("uncle is sibling")
	}
	if !env.ancestors.Contains(uncle.ParentHash) {
		return errors.New("uncle's parent unknown")
	}
	if env.family.Contains(hash) {
		return errors.New("uncle already included")
	}
	env.uncles.Add(uncle.Hash())
	return nil
}

func (e *Executor) commitTransaction(tx *types.Transaction, coinbase common.Address) (*types.Receipt, error) {
	var err error
	var receipt *types.Receipt
	if e.chain.GetVMConfig().MSRAVMSettings.EnablePreplay {
		// snapshot-revert will be done in PreplayTransaction if it's necessary
		vmconfig := *e.chain.GetVMConfig()
		if e.EnableReuseTracer {
			vmconfig.MSRAVMSettings.EnableReuseTracer = true
		}
		receipt, err = e.chain.Cmpreuse.PreplayTransaction(e.config, e.chain, &coinbase, e.current.gasPool,
			e.current.state, e.current.Header, tx, &e.current.Header.GasUsed, vmconfig, e.RoundID, nil,
			0, e.basicPreplay, e.enablePause)
	} else {
		snap := e.current.state.Snapshot()
		receipt, err = core.ApplyTransaction(e.config, e.chain, &coinbase, e.current.gasPool, e.current.state, e.current.Header, tx, &e.current.Header.GasUsed, vm.Config{})
		if err != nil {
			e.current.state.RevertToSnapshot(snap)
		}
	}
	if err != nil {
		return nil, err
	}

	e.current.Txs = append(e.current.Txs, tx)
	e.current.Receipts = append(e.current.Receipts, receipt)

	return receipt, nil
}

func (e *Executor) GetTransactionBlockHashAndNumber(hash common.Hash) (common.Hash, uint64) {
	tx, blockHash, blockNumber, _ := rawdb.ReadTransaction(e.chainDb, hash)
	if tx == nil {
		return common.Hash{}, 0
	}
	return blockHash, blockNumber
}

func (e *Executor) GetTransactionReceipt(hash common.Hash) (*types.Receipt, uint64) {
	tx, blockHash, blockNumber, index := rawdb.ReadTransaction(e.chainDb, hash)
	if tx == nil {
		return nil, 0
	}
	receipts := e.chain.GetReceiptsByHash(blockHash)
	if len(receipts) <= int(index) {
		return nil, 0
	}

	receipt := receipts[index]
	return receipt, blockNumber
}

// func (e *Executor) GetTransaction(hash common.Hash) *types.Transaction {
// 	for _, tx := range e.currentState.Txs {
// 		if tx.Hash() == hash {
// 			return tx
// 		}
// 	}

// 	return nil
// }

func (e *Executor) IsBalanceInsufficient(tx *types.Transaction, coinbase common.Address) bool {
	msg, err := tx.AsMessage(types.MakeSigner(e.config, e.current.Header.Number))
	if err != nil {
		return false
	}
	sender := vm.AccountRef(msg.From())
	if e.current.state.GetBalance(sender.Address()).Cmp(new(big.Int).Add(msg.Value(), new(big.Int).Mul(new(big.Int).SetUint64(tx.Gas()), tx.GasPrice()))) < 0 {
		return true
	}
	return false
}

func (e *Executor) StateAndHeaderByNumber(number uint64) (*state.StateDB, *types.Header, error) {
	// Pending state is only known by the miner
	//if rpc.BlockNumber(number) == rpc.PendingBlockNumber {
	//	block, state := e.eth.Miner().Pending()
	//	return state, block.Header(), nil
	//}
	// Otherwise resolve the block number and return its state
	header := e.chain.GetHeaderByNumber(number)
	if header == nil {
		return nil, nil, errors.New("no such header")
	}

	stateDb, err := e.chain.StateAt(header.Root)
	return stateDb, header, err
}

func (e *Executor) GetTransactionNonce(address common.Address) uint64 {
	nonce := e.current.state.GetNonce(address)
	return nonce
}

func (e *Executor) prepreplay(rawTxs map[common.Address]types.Transactions, coinbase common.Address) map[common.Address]types.Transactions {
	pendingCnt := 0
	txsCnt := 0
	inCnt := 0
	// outCnt := 0
	preplayCnt := 0
	// log.Info("Test Prepreplay from ", fmt.Sprintf("%d-%d-%d-0", txsCnt, pendingCnt, preplayCnt), len(rawTxs))
	// receipts := e.chain.GetReceiptsByHash(e.currentState.RawHash)
	for _, txs := range e.rawPending {
		pendingCnt += len(txs)
	}
	for _, txs := range rawTxs {
		txsCnt += len(txs)
	}
	// Copy cache.Result
	resTxs := map[common.Address]types.Transactions{}

	if !e.trigger.IsTxsInDetector {
		for from, txs := range e.rawPending {
			txsCopy := types.Transactions{}
			for _, tx := range txs {
				txCopy := &types.Transaction{}
				Copy(txCopy, tx)
				txsCopy = append(txsCopy, tx)
			}
			resTxs[from] = txsCopy
		}
	}

	// No special case, all nounce different
	alreadyInTxs := map[common.Hash]*types.Transaction{}
	for _, tx := range e.currentState.Txs {
		alreadyInTxs[tx.Hash()] = tx
	}
	e.currentState.Txs = nil

	// Judge each raw query txs
	for acc := range rawTxs {
		for _, tx := range rawTxs[acc] {

			// Fetch next transaction by order

			// Initialize
			txHash := tx.Hash()
			// from := acc
			// from, _ := types.Sender(e.current.signer, tx)

			// 1-1. Hash on chain, May have some problem, need snapshot
			if _, ok := alreadyInTxs[txHash]; ok {
				inCnt++
				e.resultMap[txHash] = &cache.ExtraResult{
					TxHash:       txHash,
					Status:       "already in",
					Reason:       "already in",
					BlockNumber:  e.currentState.Number,
					Confirmation: int64(e.currentState.Number) - int64(e.currentState.Number) + 1,
				}
				continue
			}

			if e.trigger.IsTxsInDetector {
				continue
			}

			// 1-2. Nounce on chain
			// currentNonce := e.GetTransactionNonce(from)
			// if currentNonce > tx.Nonce() {
			// 	outCnt++
			// 	e.resultMap[txHash] = &cache.ExtraResult{
			// 		TxHash: txHash,
			// 		Status: "will not in",
			// 		Reason: "outdated",
			// 	}
			// 	continue
			// }

			// 2-1. In pending pool
			if _, ok := e.pendingTxsMap[txHash]; ok {
				preplayCnt++
				continue
			}

			// 3 & 4. Entry
			// if txsWithSameAddress, ok := resTxs[acc]; ok && len(txsWithSameAddress) > 0 {

			// 	lenTxsWithSameAddress := len(txsWithSameAddress)
			// 	lastNonce := txsWithSameAddress[lenTxsWithSameAddress-1].Nonce()

			// 	// 3. Replace
			// 	if (lastNonce >= tx.Nonce()) && (lenTxsWithSameAddress-1-int(lastNonce-tx.Nonce()) > 0) {
			// 		// Have same nonce & address

			// 		txWithSameNonceAndAddress := txsWithSameAddress[lenTxsWithSameAddress-1-int(lastNonce-tx.Nonce())]

			// 		// The large already in
			// 		if new(big.Int).Div(new(big.Int).Mul(txWithSameNonceAndAddress.GasPrice(), big.NewInt(110)), big.NewInt(100)).Cmp(tx.GasPrice()) > 0 {
			// 			e.resultMap[txHash] = &cache.ExtraResult{
			// 				TxHash: txHash,
			// 				Status: "will not in",
			// 				Reason: "replace failed",
			// 			}
			// 			continue
			// 		}

			// 		// Check Balance
			// 		if e.IsBalanceInsufficient(tx, coinbase) {
			// 			e.resultMap[txHash] = &cache.ExtraResult{
			// 				TxHash: txHash,
			// 				Status: "will not in",
			// 				Reason: "insufficient balance",
			// 			}
			// 			continue
			// 		}

			// 		// Modify cache.Result txs
			// 		resTxs[acc][lenTxsWithSameAddress-1-int(lastNonce-tx.Nonce())] = tx
			// 		continue
			// 	}

			// 	// 4. Add
			// 	if (lenTxsWithSameAddress == 0 && tx.Nonce() > currentNonce) || (lenTxsWithSameAddress != 0 && tx.Nonce() > txsWithSameAddress[lenTxsWithSameAddress-1].Nonce()+1) {

			// 		// Nounce not ready
			// 		e.resultMap[txHash] = &cache.ExtraResult{
			// 			TxHash: txHash,
			// 			Status: "will not in",
			// 			Reason: "nonce not ready",
			// 		}
			// 		continue
			// 	}

			// 	if e.IsBalanceInsufficient(tx, coinbase) {
			// 		e.resultMap[txHash] = &cache.ExtraResult{
			// 			TxHash: txHash,
			// 			Status: "will not in",
			// 			Reason: "insufficient balance",
			// 		}
			// 		continue
			// 	}

			// 	resTxs[acc] = append(resTxs[acc], tx)
			// } else {

			// 	if e.IsBalanceInsufficient(tx, coinbase) {
			// 		e.resultMap[txHash] = &cache.ExtraResult{
			// 			TxHash: txHash,
			// 			Status: "will not in",
			// 			Reason: "insufficient balance",
			// 		}
			// 		continue
			// 	}
			// 	// Create new
			// 	resTxs[acc] = types.Transactions{}
			// 	resTxs[acc] = append(resTxs[acc], tx)
			// }

		}
	}

	// log.Info("Test Prepreplay ", fmt.Sprintf("%d-%d-%d-%d", txsCnt, pendingCnt, preplayCnt, resCnt), len(rawTxs))
	// Finish Combine
	return resTxs
}

// rawTxs is raw query txs
func (e *Executor) preplay(rawTxs map[common.Address]types.Transactions, coinbase common.Address) bool {
	// Short circuit if current is nil
	if e.current == nil {
		return false
	}

	if e.trigger.IsBlockCntDrive {
		e.current.gasPool = new(core.GasPool).AddGas((uint64)(defaultGasLimit))
	} else {
		e.current.gasPool = new(core.GasPool).AddGas((uint64)(defaultGasLimit * 100000))
	}

	var cachedTxs []*types.Transaction

	// Track global
	currentBlockNumber := e.currentState.Number + 1
	preplayedTxsCnt := uint64(0)
	preplayedBlkCnt := uint64(0)

	// Track rawTxs
	unpreplayedRawTxsList := map[common.Hash]uint64{}

	for _, txs := range rawTxs {
		for _, tx := range txs {
			unpreplayedRawTxsList[tx.Hash()] = uint64(1)
		}
	}

	// Collect partial pending + rawTxs
	calTxs := map[uint64]types.Transactions{}
	inCalTxs := map[common.Hash]uint64{}

	// PriceRk
	priceExist := map[uint64]uint64{}

	// Calculate Extra Data
	rangePrice := map[uint64]*cache.Range{}
	rankPrice := map[uint64]*cache.Rank{}

	// 1. Prepreplay
	log.Debug("Start preplay prepreplay", "currentState", e.trigger.Name)
	resTxs := e.prepreplay(rawTxs, coinbase)
	log.Debug("End preplay prepreplay", "currentState", e.trigger.Name)

	txs := types.NewTransactionsByPriceAndNonce(e.current.signer, resTxs, e.txnOrder, false)
	txsCnt := uint64(0)
	for _, txs := range resTxs {
		txsCnt += uint64(len(txs))
	}

	for acc := range rawTxs {
		for _, tx := range rawTxs[acc] {

			txHash := tx.Hash()
			gasPrice := tx.GasPriceU64()

			priceExist[gasPrice] = uint64(1)

			if _, ok := e.resultMap[txHash]; !ok {
				continue
			}

			// Have cache.Result then delete
			e.executionOrder = append(e.executionOrder, tx)
			delete(unpreplayedRawTxsList, txHash)

			if _, ok := calTxs[gasPrice]; !ok {

				calTxs[gasPrice] = types.Transactions{tx}
				inCalTxs[txHash] = uint64(1)
				// Initial rankvim
				rankPrice[gasPrice] = &cache.Rank{
					SmTxNum:      0,
					SmTotGasUsed: 0,
				}

			} else {
				// Duplicate problem due to the cache mechanism
				if _, inTx := inCalTxs[txHash]; !inTx {
					calTxs[gasPrice] = append(calTxs[gasPrice], tx)
					inCalTxs[txHash] = uint64(1)
				}
			}
		}
	}

	var priceList []uint64
	for key := range priceExist {
		priceList = append(priceList, key)
	}

	sort.Slice(priceList, func(i, j int) bool {
		return priceList[i] > priceList[j]
	})

	lastPrice := uint64(0)
	if e.trigger.IsPriceRankDrive && len(priceList) > 0 {
		if uint64(len(priceList)) <= e.trigger.PreplayedPriceRankLimit {
			lastPrice = priceList[len(priceList)-1]
		} else {
			lastPrice = priceList[e.trigger.PreplayedPriceRankLimit]
		}

	}

	log.Debug("Start preplay preplay", "currentState", e.trigger.Name)

	// 2. Preplay
	for {
		if e.enablePause {
			e.chain.MSRACache.PauseForProcess()
		}

		// 2.0 Detector no need to preplay
		if e.trigger.IsTxsInDetector {
			break
		}

		// 2.1 Prejudge
		if e.trigger.PreplayedTxsNumLimit == 0 {
			// log.Info("Surprise ", fmt.Sprintf("%d", len(unpreplayedRawTxsList)), len(unpreplayedRawTxsList))
		}
		// All rawTxs finish
		if len(unpreplayedRawTxsList) == 0 {
			// log.Info("Surprise!!!!!!!")
			break
		}

		tx := txs.Peek()

		// ??? revice later, why a defined parameter
		// Cached txs, Strategy 1
		if (e.current.gasPool.Gas() < params.TxGas) || (tx == nil && len(cachedTxs) != 0) {
			currentBlockNumber++
			preplayedBlkCnt++

			if e.trigger.IsBlockCntDrive && preplayedBlkCnt >= e.trigger.PreplayedBlockNumberLimit {
				break
			}
			// e.current.Header.Number = new(big.Int).Add(e.current.Header.Number, common.Big1)
			// e.current.Header.Time = big.NewInt(time.Now().Unix())
			// log.Info("preplay commit add poll",
			// 	"currentState", fmt.Sprintf("%s-%d", e.trigger.Name, len(unpreplayedRawTxsList)))

			if e.trigger.IsBlockCntDrive {
				e.current.gasPool = new(core.GasPool).AddGas((uint64)(defaultGasLimit))
			} else {
				e.current.gasPool = new(core.GasPool).AddGas((uint64)(defaultGasLimit * 100000))
			}
			// e.current.gasPool = new(core.GasPool).AddGas((uint64)(defaultGasLimit))
			log.Info("Preplay update gas pool ", fmt.Sprintf("%d", e.current.gasPool.Gas()), len(unpreplayedRawTxsList))

			for _, cachedTx := range cachedTxs {
				txs.Push(cachedTx)
			}
			cachedTxs = []*types.Transaction{}
		}
		// e.current.gasPool = new(core.GasPool).AddGas(e.current.Header.GasLimit)

		// 2.2 Start Preplay

		tx = txs.Peek()
		if tx == nil {
			// log.Info("No transaction in pending pool anymore, preplay end.")
			break
		}

		txHash := tx.Hash()
		gasPrice := tx.GasPriceU64()
		from, _ := types.Sender(e.current.signer, tx)

		if e.trigger.IsPriceRankDrive && gasPrice < lastPrice {
			txs.Pop()
			continue
		}

		if _, ok := calTxs[gasPrice]; !ok {

			calTxs[gasPrice] = []*types.Transaction{tx}
			inCalTxs[txHash] = uint64(1)
			// Initial rankvim
			rankPrice[gasPrice] = &cache.Rank{
				SmTxNum:      0,
				SmTotGasUsed: 0,
			}

		} else {
			// Duplicate problem due to the cache mechanism, No cache now
			if _, inTx := inCalTxs[txHash]; !inTx {
				calTxs[gasPrice] = append(calTxs[gasPrice], tx)
				inCalTxs[txHash] = uint64(1)
			}
		}

		// Already have cache.Result, it is possible for some already "in" txs
		if _, ok := e.resultMap[txHash]; ok {
			delete(unpreplayedRawTxsList, txHash)
			e.executionOrder = append(e.executionOrder, tx)

			// Drive
			preplayedTxsCnt++
			if e.checkDrive(txsCnt, preplayedTxsCnt) {
				break
			}

			txs.Shift()
			continue
		}

		// 2.3 Preplay

		// Start executing the transaction
		e.current.state.Prepare(txHash, common.Hash{}, e.current.tcount)

		// log.Info("preplay commit",
		// 	"currentState", fmt.Sprintf("%s-%d", e.trigger.Name, len(unpreplayedRawTxsList)))

		receipt, err := e.commitTransaction(tx, coinbase)

		// log.Info("preplay commit end",
		// 	"currentState", fmt.Sprintf("%s-%d", e.trigger.Name, len(unpreplayedRawTxsList)))

		// 2.4 Analyze cache.Result
		switch err {
		case core.ErrGasLimitReached:
			// Pop the current out-of-gas transaction without shifting in the next from the account
			if tx.Gas() < e.current.Header.GasLimit {
				cachedTxs = append(cachedTxs, tx)
				txs.Pop()

				// Strategy 2
				// txs.Push(tx)
				// e.current.gasPool = new(core.GasPool).AddGas(e.current.Header.GasLimit)
				// continue

			} else {
				e.resultMap[txHash] = &cache.ExtraResult{
					TxHash: txHash,
					Status: "will not in",
					Reason: "gas limit exceed",
				}

				txs.Shift()
			}
			// log.Info("Receipt ", fmt.Sprintf("%s", txHash.String()), len(unpreplayedRawTxsList))

		case vm.ErrInsufficientBalance:
			txsWithSameAddress := rawTxs[from]
			for _, txWithSameAddress := range txsWithSameAddress {
				if txWithSameAddress.Nonce() >= tx.Nonce() {
					e.resultMap[txWithSameAddress.Hash()] = &cache.ExtraResult{
						TxHash: txWithSameAddress.Hash(),
						Status: "will not in",
						Reason: "balance will insufficient",
					}
				}
			}
			txs.Shift()

			// case core.ErrNonceTooLow:
			// 	// NewFrame head notification data race between the transaction pool and miner, shift
			// 	log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			// 	txs.Shift()

			// case core.ErrNonceTooHigh:
			// 	// Reorg notification data race between the transaction pool and miner, skip account =
			// 	log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			// 	txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			e.current.tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)

			// out of gas....
			e.resultMap[txHash] = &cache.ExtraResult{
				TxHash: txHash,
				Status: "will not in",
				Reason: err.Error(),
			}
			txs.Shift()
		}

		if err == nil {

			// No error, Only inpending should consider this
			rankPrice[gasPrice].SmTxNum = rankPrice[gasPrice].SmTxNum + 1
			rankPrice[gasPrice].SmTotGasUsed = rankPrice[gasPrice].SmTotGasUsed + receipt.GasUsed

			if _, ok := rangePrice[gasPrice]; !ok {
				// Initial cache.Range
				rangePrice[gasPrice] = &cache.Range{
					Min:      currentBlockNumber,
					Max:      currentBlockNumber,
					GasPrice: gasPrice,
					// TxHashs:  []common.Hash{tx.Hash()},
				}
			} else {

				rangePrice[gasPrice].UpdateMin(currentBlockNumber)
				rangePrice[gasPrice].UpdateMax(currentBlockNumber)
				// rangePrice[gasPrice].TxHashs = append(rangePrice[gasPrice].TxHashs, tx.Hash())
			}

			e.resultMap[txHash] = &cache.ExtraResult{
				TxHash:      txHash,
				Status:      "will in",
				Receipt:     receipt,
				BlockNumber: currentBlockNumber,
			}

		}

		if _, ok := e.resultMap[txHash]; ok {

			// Update unpreplayed raw txs
			delete(unpreplayedRawTxsList, txHash)
			e.executionOrder = append(e.executionOrder, tx)

			// Drive
			preplayedTxsCnt++
			if e.checkDrive(txsCnt, preplayedTxsCnt) {
				break
			}
		}
	}
	log.Debug("End preplay preplay", "currentState", e.trigger.Name)

	// Will in
	var keys []uint64
	for key := range calTxs {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] > keys[j]
	})

	txNumCnt := uint64(0)
	totGasUsedCnt := uint64(0)
	priceRk := uint64(0)

	for _, price := range keys {
		tmpTxNumCnt := txNumCnt
		tmpTotGasUsedCnt := totGasUsedCnt
		priceRk = priceRk + 1

		for _, tx := range calTxs[price] {
			txHash := tx.Hash()
			txPrice := tx.GasPriceU64()
			txResult, ok := e.resultMap[txHash]
			if !ok {
				if e.trigger.IsTxsNumDrive && e.trigger.PreplayedTxsNumLimit == 0 {
					log.Info("Empty error!!!!")
				}
				continue
			}
			txResult.Rank = rankPrice[txPrice].Copy(txHash)
			txResult.Rank.GtTxNum = txNumCnt
			txResult.Rank.GtTotGasUsed = totGasUsedCnt
			txResult.Rank.PriceRk = priceRk

			if txResult.Reason == "" {
				txResult.Range = rangePrice[txPrice].Copy(txHash)
			}

			if e.enableAgg {
				e.resultCh <- txResult
			}

			// Only count the tx tha will in
			if txResult.Reason == "" {
				tmpTxNumCnt = tmpTxNumCnt + txResult.Rank.SmTxNum
				tmpTotGasUsedCnt = tmpTotGasUsedCnt + txResult.Rank.SmTotGasUsed
			}
		}

		txNumCnt = tmpTxNumCnt
		totGasUsedCnt = tmpTotGasUsedCnt
	}

	return true
}

func (e *Executor) checkDrive(txsCnt uint64, preplayedTxsCnt uint64) bool {
	if e.trigger.IsTxsNumDrive || e.trigger.IsTxsRatioDrive {

		if e.trigger.IsTxsNumDrive && e.trigger.PreplayedTxsNumLimit != 0 && preplayedTxsCnt >= e.trigger.PreplayedTxsNumLimit {
			return true
		}
		if e.trigger.IsTxsRatioDrive && preplayedTxsCnt >= uint64(float64(txsCnt)*e.trigger.PreplayedTxsRatioLimit) {
			return true
		}
	}
	return false
}

// commit generates several new sealing tasks based on the parent block.
func (e *Executor) commit(coinbase common.Address, parent *types.Block, header *types.Header, rawTxs map[common.Address]types.Transactions) {
	if err := e.engine.Prepare(e.chain, header); err != nil {
		log.Error("Failed to prepare header for preplay", "err", err)
		return
	}
	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := e.config.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if e.config.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}
	// Could potentially happen if starting to mine in an odd state.
	err := e.makeCurrent(parent, header)
	if err != nil {
		log.Error("Failed to create preplay context", "err", err)
		return
	}
	// Create the current work task and check any fork transitions needed
	env := e.current
	if e.config.DAOForkSupport && e.config.DAOForkBlock != nil && e.config.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(env.state)
	}

	// rawTxsCopy := map[common.Address]types.Transactions{}
	// for from, txs := range rawTxs {
	// 	txsCopy := types.Transactions{}
	// 	for _, tx := range txs {
	// 		txCopy := &types.Transaction{}
	// 		Copy(txCopy, tx)
	// 		txsCopy = append(txsCopy, tx)
	// 	}
	// 	rawTxsCopy[from] = txsCopy
	// }

	// txs := types.NewTransactionsByPriceAndNonce(e.current.signer, rawTxsCopy)
	e.preplay(rawTxs, coinbase)
}

func (e *Executor) extractFinished() map[common.Hash]struct{} {
	res := make(map[common.Hash]struct{})
	for hash, result := range e.resultMap {
		if result.Status == "will in" {
			res[hash] = struct{}{}
		}
	}
	return res
}
