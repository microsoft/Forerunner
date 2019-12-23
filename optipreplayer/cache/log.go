package cache

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"math/big"
	"os"
	"time"
)

// LogBlockInfo define blockInfo log format
type LogBlockInfo struct {
	TxnApply      int64         `json:"apply"`
	BlockFinalize int64         `json:"finalize"`
	WaitReuse     int64         `json:"reuse"`
	WaitRealApply int64         `json:"realApply"`
	TxnFinalize   int64         `json:"txFinalize"`
	UpdatePair    int64         `json:"updatePair"`
	GetRW         int64         `json:"getRW"`
	SetDB         int64         `json:"setDB"`
	RunTx         int64         `json:"runTx"`
	NoListen      int           `json:"L"`
	Hit           int           `json:"H"`
	NoPreplay     int           `json:"N"`
	Miss          int           `json:"M"`
	Unknown       int           `json:"U"`
	FastHit       int           `json:"F"`
	Reuse         int           `json:"reuseCount"`
	ReuseGas      int           `json:"reuseGas"`
	ProcTime      int64         `json:"procTime"`
	RunMode       string        `json:"runMode"`
	TxnCount      int           `json:"txnCount"`
	Header        *types.Header `json:"header"`
}

// LogBlockCache define blockCache log format
type LogBlockCache struct {
	NoListen      []*types.Transaction `json:"L"`
	Error         []*LogBlockCacheItem `json:"E"`
	Hit           []*LogBlockCacheItem `json:"H"`
	NoPreplay     []*LogBlockCacheItem `json:"N"`
	Miss          []*LogBlockCacheItem `json:"M"`
	Unknown       []*LogBlockCacheItem `json:"U"`
	Timestamp     uint64               `json:"processTime"` // Generation Time
	TimestampNano uint64               `json:"processTimeNano"`
}

// LogBlockCacheItem define blockCacheItem log format
type LogBlockCacheItem struct {
	TxHash        common.Hash `json:"txHash"`
	ListenBody    *TxListen   `json:"listen"`
	Rounds        []uint64    `json:"rounds"`
	ReducedRounds []uint64    `json:"reducedRounds"`
}

// LogRWrecord define rwRecord log format
type LogRWrecord struct {
	TxHash        common.Hash    `json:"txHash"`
	RoundID       uint64         `json:"roundId"` // RoundID Info
	Receipt       *types.Receipt `json:"receipt"`
	RWrecord      *RWRecord      `json:"rwrecord"`
	Timestamp     uint64         `json:"timestamp"` // Generation Time
	TimestampNano uint64         `json:"timestampNano"`
	Filled        int64          `json:"filled"`
}

// LogPreplay define preplay log format
type LogPreplay struct {
	CurrentState *CurrentState     `json:"currentState"`
	Result       []*LogPreplayItem `json:"result"`
}

// LogPreplayItem define preplayItem log format
type LogPreplayItem struct {
	TxHash   common.Hash `json:"txHash"`
	RoundID  uint64      `json:"roundId"` // RoundID Info
	GasPrice *big.Int    `json:"gasPrice"`

	// Main Result
	Timestamp     uint64 `json:"timestamp"` // Generation Time
	TimestampNano uint64 `json:"timestampNano"`

	// Extra Result
	ExtraResult *ExtraResult `json:"extraResult"`

	// FlagStatus: 0 will in, 1 in, 2 will not in
	FlagStatus uint64 `json:"flagStatus"`

	// Filled
	Filled int64 `json:"filled"`
}

// LogBlockGround define blockGround log format
type LogBlockGround []*LogRWrecord

var (
	Apply         []time.Duration
	Finalize      time.Duration
	WaitReuse     []time.Duration
	WaitRealApply []time.Duration
	UpdatePair    []time.Duration
	TxFinalize    []time.Duration
	GetRW         []time.Duration
	FastGetRW     []time.Duration
	SetDB         []time.Duration
	RunTx         []time.Duration

	ReuseResult   []cmptypes.ReuseStatus
	ReuseGasCount uint64
	RWCmpCnt      []int64
)

func SumDuration(durations []time.Duration) (sum time.Duration) {
	for _, d := range durations {
		sum += d
	}
	return
}

func SumCount(counts []int64) (sum int64) {
	for _, c := range counts {
		sum += c
	}
	return
}

func ResetLogVar() {
	Apply = make([]time.Duration, 50)
	Finalize = 0
	WaitReuse = make([]time.Duration, 50)
	WaitRealApply = make([]time.Duration, 50)
	UpdatePair = make([]time.Duration, 50)
	TxFinalize = make([]time.Duration, 50)
	GetRW = make([]time.Duration, 50)
	FastGetRW = make([]time.Duration, 50)
	SetDB = make([]time.Duration, 50)
	RunTx = make([]time.Duration, 50)

	ReuseGasCount = 0
	RWCmpCnt = make([]int64, 50)
}

// LogPrint print v to filePath file
func (r *GlobalCache) LogPrint(filePath string, fileName string, v interface{}) {
	_, err := os.Stat(filePath)
	if err != nil {
		err := os.MkdirAll(filePath, os.ModePerm)
		if err != nil {
			log.Error("Error while make path", "err", err)
			return
		}
	}

	f, err := os.OpenFile(filePath+"/"+fileName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Error("Error while opening file", "err", err)
		return
	}
	defer f.Close()

	tmpJSON, err := json.Marshal(v)
	if err != nil {
		log.Error("Error while marshal preplay json", "err", err)
		return
	}

	if _, err := f.Write(append(tmpJSON, byte('\n'))); err != nil {
		log.Error("Error while output preplay json", "err", err)
		return
	}
}

// InfoPrint block info to block folder
func (r *GlobalCache) InfoPrint(block *types.Block, procTime time.Duration, cfg vm.Config) {

	var (
		sumApply         = SumDuration(Apply)
		sumWaitReuse     = SumDuration(WaitReuse)
		sumWaitRealApply = SumDuration(WaitRealApply)
		sumTxFinalize    = SumDuration(TxFinalize)
		sumUpdatePair    = SumDuration(UpdatePair)
		sumGetRW         = SumDuration(GetRW)
		//sumFastGetRW     = SumDuration(FastGetRW)
		sumSetDB         = SumDuration(SetDB)
		sumRunTx         = SumDuration(RunTx)

		sumCmpCount = SumCount(RWCmpCnt)
	)

	infoResult := &LogBlockInfo{
		TxnApply:      sumApply.Microseconds(),
		BlockFinalize: Finalize.Microseconds(),
		WaitReuse:     sumWaitReuse.Microseconds(),
		WaitRealApply: sumWaitRealApply.Microseconds(),
		TxnFinalize:   sumTxFinalize.Microseconds(),
		UpdatePair:    sumUpdatePair.Microseconds(),
		GetRW:         sumGetRW.Microseconds(),
		SetDB:         sumSetDB.Microseconds(),
		RunTx:         sumRunTx.Microseconds(),
		ReuseGas:      int(ReuseGasCount),
		ProcTime:      procTime.Nanoseconds(),
		TxnCount:      len(block.Transactions()),
		Header:        block.Header(),
	}

	processTimeNano := r.PeekBlockPre(block.Hash()).ListenTimeNano
	noInResultCnt := 0
	if len(ReuseResult) != 0 {
		for index, tx := range block.Transactions() {
			txListen := r.GetTxListen(tx.Hash())
			if txListen == nil || txListen.ListenTimeNano > processTimeNano {
				infoResult.NoListen++
			}
			switch ReuseResult[index] {
			case cmptypes.Hit:
				infoResult.Hit++
			case cmptypes.NoCache:
				infoResult.NoPreplay++
			case cmptypes.CacheNoIn:
				infoResult.Miss++
				noInResultCnt++
			case cmptypes.CacheNoMatch:
				infoResult.Miss++
			case cmptypes.Unknown:
				infoResult.Unknown++
			case cmptypes.FastHit:
				infoResult.FastHit++
				infoResult.Hit++
			}
		}
	}

	switch {
	case !cfg.MSRAVMSettings.EnablePreplay:
		infoResult.RunMode = "normal"
	case !cfg.MSRAVMSettings.CmpReuse:
		infoResult.RunMode = "only_preplay"
	default:
		infoResult.RunMode = "reuse"
		listenCnt := infoResult.TxnCount - infoResult.NoListen
		preplayCnt := infoResult.Hit + infoResult.Miss + infoResult.Unknown
		var listenRate, preplayRate, hitRate, MissRate, noInResultRate, unknownRate, fastHitRate, reuseGasRate float64
		if infoResult.TxnCount > 0 {
			listenRate = float64(listenCnt) / float64(infoResult.TxnCount)
			preplayRate = float64(preplayCnt) / float64(infoResult.TxnCount)
			hitRate = float64(infoResult.Hit) / float64(infoResult.TxnCount)
			MissRate = float64(infoResult.Miss) / float64(infoResult.TxnCount)
			noInResultRate = float64(noInResultCnt) / float64(infoResult.TxnCount)
			unknownRate = float64(infoResult.Unknown) / float64(infoResult.TxnCount)
			fastHitRate = float64(infoResult.FastHit) / float64(infoResult.TxnCount)
			reuseGasRate = float64(infoResult.ReuseGas) / float64(infoResult.Header.GasUsed)
		}
		context := []interface{}{
			"Total", fmt.Sprintf("%03d", infoResult.TxnCount),
			"Listen", fmt.Sprintf("%03d(%.2f)", listenCnt, listenRate),
			"Preplay", fmt.Sprintf("%03d(%.2f)", preplayCnt, preplayRate),
			"Hit", fmt.Sprintf("%03d(%.2f)", infoResult.Hit, hitRate),
			"Miss", fmt.Sprintf("%03d(%.2f)-%03d(%.2f)", infoResult.Miss, MissRate,
				noInResultCnt, noInResultRate),
			"Unknown", fmt.Sprintf("%03d(%.2f)", infoResult.Unknown, unknownRate),
			"FastHit", fmt.Sprintf("%03d(%.2f)", infoResult.FastHit, fastHitRate),
			"ReuseGas", fmt.Sprintf("%03d(%.2f)", infoResult.ReuseGas, reuseGasRate),
		}
		log.Info("BlockReuse", context...)

		context = []interface{}{"apply", common.PrettyDuration(sumApply), "finalize", common.PrettyDuration(Finalize)}
		if infoResult.TxnCount != 0 {
			context = append(context,
				"reuse/realApply", fmt.Sprintf("%.2f/%.2f", float64(sumWaitReuse)/float64(sumApply), float64(sumWaitRealApply)/float64(sumApply)),
				"finalize+update", fmt.Sprintf("%.2f", float64(sumTxFinalize+sumUpdatePair)/float64(sumApply)))
			if sumWaitReuse != 0 {
				context = append(context,
					"getRW", fmt.Sprintf("%.2f(%d)", float64(sumGetRW)/float64(sumWaitReuse), sumCmpCount),
					"setDB", fmt.Sprintf("%.2f", float64(sumSetDB)/float64(sumWaitReuse)))
			}
			if sumWaitRealApply != 0 {
				context = append(context,
					"runTx", fmt.Sprintf("%.2f", float64(sumRunTx)/float64(sumWaitRealApply)))
			}
		}
		log.Info("Time consuming for each section", context...)
	}

	filePath := fmt.Sprintf("%s/block/%s",
		logDir,
		block.Number().String())

	infoFileName := fmt.Sprintf("%s_%s_info_%d.json",
		block.Number().String(), block.Hash().String(), block.Header().Time)

	r.LogPrint(filePath, infoFileName, infoResult)
}

// CachePrint print reuse result of all txns in a block to block folder
func (r *GlobalCache) CachePrint(block *types.Block, reuseResult []cmptypes.ReuseStatus) {

	cacheResult := &LogBlockCache{
		NoListen:  []*types.Transaction{},
		Error:     []*LogBlockCacheItem{},
		Hit:       []*LogBlockCacheItem{},
		NoPreplay: []*LogBlockCacheItem{},
		Miss:      []*LogBlockCacheItem{},
		Unknown:   []*LogBlockCacheItem{},
	}

	listenTxCnt := uint64(0)

	blockPre := r.PeekBlockPre(block.Hash())
	if blockPre == nil {
		context := []interface{}{
			"number", block.NumberU64(), "Cannot found", "blockPre",
		}
		log.Info("BlockReuse", context...)
		return
	}

	cacheResult.Timestamp = blockPre.ListenTime
	cacheResult.TimestampNano = blockPre.ListenTimeNano
	processTimeNano := blockPre.ListenTimeNano

	if len(reuseResult) != 0 {
		reuseCnt := [6]uint64{}
		for index, tx := range block.Transactions() {
			reuseCnt[reuseResult[index]]++

			txListen := r.GetTxListen(tx.Hash())
			if txListen == nil || txListen.ListenTimeNano > processTimeNano {
				cacheResult.NoListen = append(cacheResult.NoListen, tx)
			} else {
				listenTxCnt++
			}

			txCache := &LogBlockCacheItem{}
			txCache.TxHash = tx.Hash()
			if txListen != nil {
				txCache.ListenBody = txListen
			} else {
				txCache.ListenBody = &TxListen{
					Tx: tx,
				}
			}

			txPreplay := r.GetTxPreplay(tx.Hash())
			if txPreplay != nil {

				txPreplay.Mu.Lock()

				var keys []uint64
				roundKeys := txPreplay.PreplayResults.Rounds.Keys()
				for _, raw := range roundKeys {
					keys = append(keys, raw.(uint64))
				}
				var reducedKeys []uint64
				for _, key := range keys {
					rawRound, _ := txPreplay.PreplayResults.Rounds.Peek(key)
					round := rawRound.(*PreplayResult)
					if round.Filled != -1 {
						continue
					}
					reducedKeys = append(reducedKeys, key)
				}
				txCache.Rounds = keys
				txCache.ReducedRounds = reducedKeys

				txPreplay.Mu.Unlock()
			} else {
				txCache.Rounds = nil
				txCache.ReducedRounds = nil
			}

			switch reuseResult[index] {
			case 0:
				cacheResult.Error = append(cacheResult.Error, txCache)

			case 1:
				cacheResult.Hit = append(cacheResult.Hit, txCache)

			case 2:
				cacheResult.NoPreplay = append(cacheResult.NoPreplay, txCache)

			case 3, 4:
				cacheResult.Miss = append(cacheResult.Miss, txCache)

			case 5:
				cacheResult.Unknown = append(cacheResult.Unknown, txCache)

			default:
				// Do nothing
			}

			// foundResult = append(foundResult, r.GetFound(tx.Hash()))
		}
	}

	filePath := fmt.Sprintf("%s/block/%s",
		logDir,
		block.Number().String())

	cacheFileName := fmt.Sprintf("%s_%s_cache_%d.json",
		block.Number().String(), block.Hash().String(), cacheResult.Timestamp)

	r.LogPrint(filePath, cacheFileName, cacheResult)
}

// GroundPrint print ground record of all txns in a block to block folder
func (r *GlobalCache) GroundPrint(block *types.Block) {

	groundResult := LogBlockGround{}

	for _, tx := range block.Transactions() {
		ground := r.GetGround(tx.Hash())
		if ground == nil {
			context := []interface{}{
				"number", block.NumberU64(), "Cannot found", "ground",
			}
			log.Info("BlockReuse", context...)
			continue
		}

		groundResult = append(groundResult, &LogRWrecord{
			TxHash:        ground.TxHash,
			RoundID:       0,
			Receipt:       ground.Receipt,
			RWrecord:      ground.RWrecord,
			Timestamp:     ground.RWTimeStamp,
			TimestampNano: ground.RWTimeStampNano,
			Filled:        -1,
		})
	}

	filePath := fmt.Sprintf("%s/block/%s",
		logDir,
		block.Number().String())

	groundFileName := fmt.Sprintf("%s_%s_ground.json",
		block.Number().String(), block.Hash().String())

	r.LogPrint(filePath, groundFileName, groundResult)
}

// PreplayPrint print one round preplay result to preplay folder
func (r *GlobalCache) PreplayPrint(RoundID uint64, executionOrder []*types.Transaction, currentState *CurrentState) {

	lowestPrice := int64(0)
	preplayResult := &LogPreplay{
		CurrentState: currentState,
		Result:       []*LogPreplayItem{},
	}
	// Disable RWrecord writing
	for _, tx := range executionOrder {
		txPreplay := r.GetTxPreplay(tx.Hash())
		if txPreplay == nil {
			log.Debug("[PreplayPrint] getTxPreplay Error", "txHash", tx.Hash())
			preplayResult.Result = append(preplayResult.Result, nil)
			continue
		}

		txPreplay.Mu.Lock()

		round, _ := txPreplay.PeekRound(RoundID)

		if round == nil {
			log.Debug("[PreplayPrint] getRoundID Error", "txHash", tx.Hash(), "roundID", RoundID)
			preplayResult.Result = append(preplayResult.Result, nil)

			txPreplay.Mu.Unlock()
			continue
		}

		if lowestPrice == 0 || (lowestPrice != 0 && txPreplay.GasPrice.Int64() < lowestPrice) {
			lowestPrice = txPreplay.GasPrice.Int64()
		}

		// append Log Preplay Item
		preplayResult.Result = append(preplayResult.Result, &LogPreplayItem{
			TxHash:        round.TxHash,
			RoundID:       round.RoundID,
			GasPrice:      round.GasPrice,
			Timestamp:     round.Timestamp,
			TimestampNano: round.TimestampNano,
			ExtraResult:   round.ExtraResult,
			FlagStatus:    round.FlagStatus,
			Filled:        round.Filled,
		})
		// force print hash

		// append Log RWreocrd Item
		r.RWrecordPrint(round)

		// if round.CurrentState != nil {
		// 	currentState = round.CurrentState
		// }

		txPreplay.Mu.Unlock()
	}

	// General Log
	if currentState != nil {
		filePath := fmt.Sprintf("%s/preplay/%d",
			logDir,
			currentState.Number)

		fileName := fmt.Sprintf("%d_%s_preplay_%d_%s_%d_%d.json",
			currentState.Number, currentState.Hash, RoundID, currentState.PreplayName, currentState.SnapshotTimestamp, lowestPrice)

		r.LogPrint(filePath, fileName, preplayResult)
	}
}

// RWrecordPrint append one round preplay rwrecord to rwrecord folder
func (r *GlobalCache) RWrecordPrint(round *PreplayResult) {

	txHash := round.TxHash.String()
	firstLayer := txHash[2:4]
	secondLayer := txHash[4:6]
	RWrecordResult := &LogRWrecord{
		TxHash:        round.TxHash,
		RoundID:       round.RoundID,
		Receipt:       round.Receipt,
		RWrecord:      round.RWrecord,
		Timestamp:     round.Timestamp,
		TimestampNano: round.TimestampNano,
		Filled:        round.Filled,
	}
	if RWrecordResult.RWrecord != nil {
		RWrecordResult.RWrecord.GetHash()
	}
	if RWrecordResult.Filled != -1 {
		RWrecordResult.Receipt = nil
		RWrecordResult.RWrecord = nil
	}

	filePath := fmt.Sprintf("%s/rwrecord/%s/%s",
		logDir,
		firstLayer,
		secondLayer)

	fileName := fmt.Sprintf("%s_rwrecord.json",
		round.TxHash.String())

	r.LogPrint(filePath, fileName, RWrecordResult)
}
