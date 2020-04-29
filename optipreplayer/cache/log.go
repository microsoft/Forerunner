package cache

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"math/big"
	"os"
	"sort"
	"time"
)

const k = 10

// LogBlockInfo define blockInfo log format
type LogBlockInfo struct {
	TxnApply        int64         `json:"apply"`
	BlockFinalize   int64         `json:"finalize"`
	WaitReuse       int64         `json:"reuse"`
	WaitRealApply   int64         `json:"realApply"`
	TxnFinalize     int64         `json:"txFinalize"`
	Update          int64         `json:"updatePair"`
	GetRW           int64         `json:"getRW"`
	SetDB           int64         `json:"setDB"`
	RunTx           int64         `json:"runTx"`
	NoListen        int           `json:"L"`
	NoPackage       int           `json:"Pa"`
	NoEnqueue       int           `json:"E"`
	NoPreplay       int           `json:"Pr"`
	Hit             int           `json:"H"`
	Miss            int           `json:"M"`
	Unknown         int           `json:"U"`
	AbortedTrace    int           `json:"aR"`
	AbortedMix      int           `json:"aM"`
	AbortedDelta    int           `json:"aD"`
	AbortedTrie     int           `json:"aT"`
	MixHit          int           `json:"MH"`
	AllDepMixHit    int           `json:"DMH"`
	AllDetailMixHit int           `json:"TMH"`
	PartialMixHit   int           `json:"PMH"`
	UnhitHeadCount  [10]int       `json:"UHC"`
	TrieHit         int           `json:"TH"`
	DeltaHit        int           `json:"EH"`
	TraceHit        int           `json:"RH"`
	ReuseGas        int           `json:"reuseGas"`
	ProcTime        int64         `json:"procTime"`
	RunMode         string        `json:"runMode"`
	TxnCount        int           `json:"txnCount"`
	Header          *types.Header `json:"header"`
}

type MissReporter interface {
	SetBlock(block *types.Block)
	SetNoPreplayTxn(txn *types.Transaction, enqueue uint64)
	SetMissTxn(txn *types.Transaction, miss *cmptypes.PreplayResTrieNode, value interface{}, txnType int)
	ReportMiss(noListen, noPackage, noEnqueue, noPreplay uint64)
}

// LogBlockCache define blockCache log format
type LogBlockCache struct {
	NoListen      []*types.Transaction `json:"L"`
	NoPackage     []*types.Transaction `json:"P"`
	Error         []*LogBlockCacheItem `json:"E"`
	NoPreplay     []*LogBlockCacheItem `json:"N"`
	Hit           []*LogBlockCacheItem `json:"H"`
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
	syncStart uint64
	toScreen  bool

	Process       time.Duration
	Apply         []time.Duration
	Finalize      time.Duration
	WaitReuse     []time.Duration
	WaitRealApply []time.Duration
	TxFinalize    []time.Duration
	Update        []time.Duration
	GetRW         []time.Duration
	SetDB         []time.Duration
	RunTx         []time.Duration

	ReuseGasCount uint64

	ReuseResult []*cmptypes.ReuseStatus

	WarmupMissTxnCount = make(map[string]int)
	AddrWarmupMiss     = make(map[string]int)
	AddrNoWarmup       = make(map[string]int)
	AddrWarmupHelpless = make(map[string]int)
	KeyWarmupMiss      = make(map[string]int)
	KeyNoWarmup        = make(map[string]int)
	KeyWarmupHelpless  = make(map[string]int)

	CumWarmupMissTxnCount = make(map[string]int)
	CumAddrWarmupMiss     = make(map[string]int)
	CumAddrNoWarmup       = make(map[string]int)
	CumAddrWarmupHelpless = make(map[string]int)
	CumKeyWarmupMiss      = make(map[string]int)
	CumKeyNoWarmup        = make(map[string]int)
	CumKeyWarmupHelpless  = make(map[string]int)

	cumApply         = metrics.NewRegisteredTimer("apply", nil)
	cumFinalize      = metrics.NewRegisteredTimer("finalize", nil)
	cumWaitReuse     = metrics.NewRegisteredTimer("apply.waitReuse", nil)
	cumWaitRealApply = metrics.NewRegisteredTimer("apply.waitRealApply", nil)
	cumTxFinalize    = metrics.NewRegisteredTimer("apply.txFinalize", nil)
	cumUpdate        = metrics.NewRegisteredTimer("apply.update", nil)
	cumGetRW         = metrics.NewRegisteredTimer("apply.waitReuse.getRW", nil)
	cumSetDB         = metrics.NewRegisteredTimer("apply.waitReuse.setDB", nil)
	cumRunTx         = metrics.NewRegisteredTimer("apply.waitRealApply.runTx", nil)

	blkCount      uint64
	txnCount      uint64
	listen        uint64
	Package       uint64
	enqueue       uint64
	preplay       uint64
	hit           uint64
	unknown       uint64
	mixHitCount   uint64
	trieHitCount  uint64
	deltaHitCount uint64
	traceHitCount uint64
	AbortedTrace  uint64
	AbortedMix    uint64
	AbortedDelta  uint64
	AbortedTrie   uint64
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

func ResetLogVar(size int) {
	WarmupMissTxnCount = make(map[string]int)
	AddrWarmupMiss = make(map[string]int)
	AddrNoWarmup = make(map[string]int)
	AddrWarmupHelpless = make(map[string]int)
	KeyWarmupMiss = make(map[string]int)
	KeyNoWarmup = make(map[string]int)
	KeyWarmupHelpless = make(map[string]int)

	Apply = make([]time.Duration, 0, size)
	Finalize = 0
	WaitReuse = make([]time.Duration, 0, size)
	WaitRealApply = make([]time.Duration, 0, size)
	TxFinalize = make([]time.Duration, 0, size)
	Update = make([]time.Duration, 0, size)
	GetRW = make([]time.Duration, 0, size)
	SetDB = make([]time.Duration, 0, size)
	RunTx = make([]time.Duration, 0, size)

	ReuseGasCount = 0
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
func (r *GlobalCache) InfoPrint(block *types.Block, cfg vm.Config, synced bool, reporter MissReporter, statedb *state.StateDB) {

	var (
		sumApply         = SumDuration(Apply)
		sumWaitReuse     = SumDuration(WaitReuse)
		sumWaitRealApply = SumDuration(WaitRealApply)
		sumTxFinalize    = SumDuration(TxFinalize)
		sumUpdate        = SumDuration(Update)
		sumGetRW         = SumDuration(GetRW)
		sumSetDB         = SumDuration(SetDB)
		sumRunTx         = SumDuration(RunTx)
	)

	infoResult := &LogBlockInfo{
		TxnApply:      sumApply.Microseconds(),
		BlockFinalize: Finalize.Microseconds(),
		WaitReuse:     sumWaitReuse.Microseconds(),
		WaitRealApply: sumWaitRealApply.Microseconds(),
		TxnFinalize:   sumTxFinalize.Microseconds(),
		Update:        sumUpdate.Microseconds(),
		GetRW:         sumGetRW.Microseconds(),
		SetDB:         sumSetDB.Microseconds(),
		RunTx:         sumRunTx.Microseconds(),
		ReuseGas:      int(ReuseGasCount),
		ProcTime:      Process.Nanoseconds(),
		TxnCount:      len(block.Transactions()),
		Header:        block.Header(),
	}

	processTimeNano := r.PeekBlockPre(block.Hash()).ListenTimeNano
	if len(ReuseResult) != 0 {
		for index, tx := range block.Transactions() {
			switch ReuseResult[index].BaseStatus {
			case cmptypes.NoPreplay:
				infoResult.NoPreplay++
				txEnqueue := r.GetTxEnqueue(tx.Hash())
				if txEnqueue == 0 || txEnqueue > processTimeNano {
					infoResult.NoEnqueue++
					txPackage := r.GetTxPackage(tx.Hash())
					if txPackage == 0 || txPackage > processTimeNano {
						infoResult.NoPackage++
						txListen := r.GetTxListen(tx.Hash())
						if txListen == nil || txListen.ListenTimeNano > processTimeNano {
							infoResult.NoListen++
						}
					}
				}
			case cmptypes.Hit:
				infoResult.Hit++
				switch ReuseResult[index].HitType {
				case cmptypes.MixHit:
					infoResult.MixHit++
					switch ReuseResult[index].MixHitStatus.MixHitType {
					case cmptypes.AllDepHit:
						infoResult.AllDepMixHit++
					case cmptypes.AllDetailHit:
						infoResult.AllDetailMixHit++
					case cmptypes.PartialHit:
						infoResult.PartialMixHit++
						unHitHead := ReuseResult[index].MixHitStatus.DepUnmatchedInHead
						if unHitHead < 9 {
							infoResult.UnhitHeadCount[unHitHead]++
						} else {
							infoResult.UnhitHeadCount[9]++
						}
					}
				case cmptypes.TrieHit:
					infoResult.TrieHit++
				case cmptypes.DeltaHit:
					infoResult.DeltaHit++
				case cmptypes.TraceHit:
					infoResult.TraceHit++
				}
			case cmptypes.Miss:
				infoResult.Miss++
			case cmptypes.Unknown:
				infoResult.Unknown++
				switch ReuseResult[index].AbortStage {
				case cmptypes.TraceCheck:
					infoResult.AbortedTrace++
				case cmptypes.MixCheck:
					infoResult.AbortedMix++
				case cmptypes.DeltaCheck:
					infoResult.AbortedDelta++
				case cmptypes.TrieCheck:
					infoResult.AbortedTrie++
				}
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
	}

	filePath := fmt.Sprintf("%s/block/%s",
		logDir,
		block.Number().String())

	infoFileName := fmt.Sprintf("%s_%s_info_%d.json",
		block.Number().String(), block.Hash().String(), block.Header().Time)

	r.LogPrint(filePath, infoFileName, infoResult)

	if syncStart == 0 && synced {
		syncStart = block.NumberU64()
	}
	if syncStart != 0 && block.NumberU64() >= syncStart+k {
		toScreen = true
	}

	if !cfg.MSRAVMSettings.IsEmulateMode && !toScreen {
		return
	}

	for word := range WarmupMissTxnCount {
		CumWarmupMissTxnCount[word] += WarmupMissTxnCount[word]
		CumAddrWarmupMiss[word] += AddrWarmupMiss[word]
		CumAddrNoWarmup[word] += AddrNoWarmup[word]
		CumAddrWarmupHelpless[word] += AddrWarmupHelpless[word]
		CumKeyWarmupMiss[word] += KeyWarmupMiss[word]
		CumKeyNoWarmup[word] += KeyNoWarmup[word]
		CumKeyWarmupHelpless[word] += KeyWarmupHelpless[word]
	}

	var keySort []string
	for word := range CumWarmupMissTxnCount {
		keySort = append(keySort, word)
	}
	sort.Strings(keySort)

	for _, word := range keySort {
		if CumAddrWarmupMiss[word]+CumAddrWarmupHelpless[word]+CumKeyWarmupMiss[word]+CumKeyWarmupHelpless[word] > 0 {
			context := []interface{}{"type", word, "cum txn miss", CumWarmupMissTxnCount[word], "txn miss", WarmupMissTxnCount[word]}
			if CumAddrWarmupMiss[word] > 0 {
				context = append(context, "cum addr miss-helpless",
					fmt.Sprintf("%d(%d)-%d", CumAddrWarmupMiss[word], CumAddrNoWarmup[word], CumAddrWarmupHelpless[word]))
				if AddrWarmupMiss[word] > 0 || AddrWarmupHelpless[word] > 0 {
					context = append(context, "addr miss-helpless",
						fmt.Sprintf("%d(%d)-%d", AddrWarmupMiss[word], AddrNoWarmup[word], AddrWarmupHelpless[word]))
				}
			}
			if CumKeyWarmupMiss[word] > 0 {
				context = append(context, "cum key miss",
					fmt.Sprintf("%d(%d)-%d", CumKeyWarmupMiss[word], CumKeyNoWarmup[word], CumKeyWarmupHelpless[word]))
				if KeyWarmupMiss[word] > 0 || KeyWarmupHelpless[word] > 0 {
					context = append(context, "key miss",
						fmt.Sprintf("%d(%d)-%d", KeyWarmupMiss[word], KeyNoWarmup[word], KeyWarmupHelpless[word]))
				}
			}
			log.Info("Warmup miss statistics", context...)
		}
	}

	cumApply.Update(sumApply)
	cumFinalize.Update(Finalize)
	cumWaitReuse.Update(sumWaitReuse)
	cumWaitRealApply.Update(sumWaitRealApply)
	cumTxFinalize.Update(sumTxFinalize)
	cumUpdate.Update(sumUpdate)
	cumGetRW.Update(sumGetRW)
	cumSetDB.Update(sumSetDB)
	cumRunTx.Update(sumRunTx)

	context := []interface{}{"apply", common.PrettyDuration(sumApply), "finalize", common.PrettyDuration(Finalize)}
	if len(block.Transactions()) != 0 {
		context = append(context,
			"reuse/realApply", fmt.Sprintf("%.2f/%.2f", float64(sumWaitReuse)/float64(sumApply), float64(sumWaitRealApply)/float64(sumApply)),
			"finalize+update", fmt.Sprintf("%.2f", float64(sumTxFinalize+sumUpdate)/float64(sumApply)))
		if sumWaitReuse != 0 {
			context = append(context,
				"getRW", fmt.Sprintf("%.2f", float64(sumGetRW)/float64(sumWaitReuse)),
				"setDB", fmt.Sprintf("%.2f", float64(sumSetDB)/float64(sumWaitReuse)))
		}
		if sumWaitRealApply != 0 {
			context = append(context,
				"runTx", fmt.Sprintf("%.2f", float64(sumRunTx)/float64(sumWaitRealApply)))
		}
	}
	log.Info("Time consumption detail", context...)

	context = []interface{}{"apply", common.PrettyDuration(cumApply.Sum()), "finalize", common.PrettyDuration(cumFinalize.Sum())}
	if len(block.Transactions()) != 0 {
		context = append(context,
			"reuse/realApply",
			fmt.Sprintf("%.2f/%.2f", float64(cumWaitReuse.Sum())/float64(cumApply.Sum()), float64(cumWaitRealApply.Sum())/float64(cumApply.Sum())),
			"finalize+update", fmt.Sprintf("%.2f", float64(cumTxFinalize.Sum()+cumUpdate.Sum())/float64(cumApply.Sum())))
		if sumWaitReuse != 0 {
			context = append(context,
				"getRW", fmt.Sprintf("%.2f", float64(cumGetRW.Sum())/float64(cumWaitReuse.Sum())),
				"setDB", fmt.Sprintf("%.2f", float64(cumSetDB.Sum())/float64(cumWaitReuse.Sum())))
		}
		if sumWaitRealApply != 0 {
			context = append(context,
				"runTx", fmt.Sprintf("%.2f", float64(cumRunTx.Sum())/float64(cumWaitRealApply.Sum())))
		}
	}
	log.Info("Cumulative time consumption detail", context...)

	if cfg.MSRAVMSettings.EnablePreplay && cfg.MSRAVMSettings.CmpReuse {
		listenCnt := infoResult.TxnCount - infoResult.NoListen
		packageCnt := infoResult.TxnCount - infoResult.NoPackage
		enqueueCnt := infoResult.TxnCount - infoResult.NoEnqueue
		preplayCnt := infoResult.TxnCount - infoResult.NoPreplay
		var listenRate, packageRate, enqueueRate, preplayRate, hitRate, missRate, unknownRate, mixHitRate, trieHitRate, deltaHitRate, traceHitRate float64
		var reuseGasRate float64
		if infoResult.TxnCount > 0 {
			listenRate = float64(listenCnt) / float64(infoResult.TxnCount)
			packageRate = float64(packageCnt) / float64(infoResult.TxnCount)
			enqueueRate = float64(enqueueCnt) / float64(infoResult.TxnCount)
			preplayRate = float64(preplayCnt) / float64(infoResult.TxnCount)
			hitRate = float64(infoResult.Hit) / float64(infoResult.TxnCount)
			missRate = float64(infoResult.Miss) / float64(infoResult.TxnCount)
			unknownRate = float64(infoResult.Unknown) / float64(infoResult.TxnCount)
			mixHitRate = float64(infoResult.MixHit) / float64(infoResult.TxnCount)
			trieHitRate = float64(infoResult.TrieHit) / float64(infoResult.TxnCount)
			deltaHitRate = float64(infoResult.DeltaHit) / float64(infoResult.TxnCount)
			traceHitRate = float64(infoResult.TraceHit) / float64(infoResult.TxnCount)
			reuseGasRate = float64(infoResult.ReuseGas) / float64(infoResult.Header.GasUsed)
		}
		context := []interface{}{
			"Total", fmt.Sprintf("%03d", infoResult.TxnCount),
			"Listen", fmt.Sprintf("%03d(%.2f)", listenCnt, listenRate),
			"Package", fmt.Sprintf("%03d(%.2f)", packageCnt, packageRate),
			"Enqueue", fmt.Sprintf("%03d(%.2f)", enqueueCnt, enqueueRate),
			"Preplay", fmt.Sprintf("%03d(%.2f)", preplayCnt, preplayRate),
			"Hit", fmt.Sprintf("%03d(%.2f)", infoResult.Hit, hitRate),
			"MixHit", fmt.Sprintf("%03d(%.2f)-[AllDep:%03d|AllDetail:%03d|Mix:%03d]", infoResult.MixHit, mixHitRate,
				infoResult.AllDepMixHit, infoResult.AllDetailMixHit, infoResult.PartialMixHit),
			"MixUnhitHead", fmt.Sprint(infoResult.UnhitHeadCount),
		}
		if infoResult.TrieHit > 0 || infoResult.DeltaHit > 0 || infoResult.TraceHit > 0 {
			context = append(context, "RH-DH-TH", fmt.Sprintf("%03d(%.2f)-%03d(%.2f)-%03d(%.2f)",
				infoResult.TraceHit, traceHitRate, infoResult.DeltaHit, deltaHitRate, infoResult.TrieHit, trieHitRate))
		}
		if infoResult.Miss > 0 {
			context = append(context, "Miss", fmt.Sprintf("%03d(%.2f)", infoResult.Miss, missRate))
		}
		if infoResult.Unknown > 0 {
			context = append(context, "Unknown", fmt.Sprintf("%03d(%.2f)", infoResult.Unknown, unknownRate))
			context = append(context, "AbortStage(R-M-D-T)", fmt.Sprintf("%03d-%03d-%03d-%03d",
				infoResult.AbortedTrace, infoResult.AbortedMix, infoResult.AbortedDelta, infoResult.AbortedTrie))
		}
		context = append(context, "ReuseGas", fmt.Sprintf("%d(%.2f)", infoResult.ReuseGas, reuseGasRate))

		log.Info("Block reuse", context...)

		blkCount++
		txnCount += uint64(infoResult.TxnCount)
		listen += uint64(listenCnt)
		Package += uint64(packageCnt)
		enqueue += uint64(enqueueCnt)
		preplay += uint64(preplayCnt)
		hit += uint64(infoResult.Hit)
		unknown += uint64(infoResult.Unknown)
		mixHitCount += uint64(infoResult.MixHit)
		trieHitCount += uint64(infoResult.TrieHit)
		deltaHitCount += uint64(infoResult.DeltaHit)
		traceHitCount += uint64(infoResult.TraceHit)
		mixHitRate = float64(mixHitCount) / float64(txnCount)
		trieHitRate = float64(trieHitCount) / float64(txnCount)
		deltaHitRate = float64(deltaHitCount) / float64(txnCount)
		traceHitRate = float64(traceHitCount) / float64(txnCount)
		AbortedTrace += uint64(infoResult.AbortedTrace)
		AbortedMix += uint64(infoResult.AbortedMix)
		AbortedDelta += uint64(infoResult.AbortedDelta)
		AbortedTrie += uint64(infoResult.AbortedTrie)
		log.Info("Cumulative block reuse", "block", blkCount, "txn", txnCount,
			"listen", fmt.Sprintf("%d(%.3f)", listen, float64(listen)/float64(txnCount)),
			"package", fmt.Sprintf("%d(%.3f)", Package, float64(Package)/float64(txnCount)),
			"enqueue", fmt.Sprintf("%d(%.3f)", enqueue, float64(enqueue)/float64(txnCount)),
			"preplay", fmt.Sprintf("%d(%.3f)", preplay, float64(preplay)/float64(txnCount)),
			"hit", fmt.Sprintf("%d(%.3f)", hit, float64(hit)/float64(txnCount)),
			"unknown", fmt.Sprintf("%d(%.3f)", unknown, float64(unknown)/float64(txnCount)),
			"RH-MH-DH-TH", fmt.Sprintf("%03d(%.2f)-%03d(%.2f)-%03d(%.2f)-%03d(%.2f)",
				traceHitCount, traceHitRate, mixHitCount, mixHitRate, deltaHitCount, deltaHitRate, trieHitCount, trieHitRate),
			"AR-AM-AD-AT", fmt.Sprintf("%03d-%03d-%03d-%03d",
				AbortedTrace, AbortedMix, AbortedDelta, AbortedTrie),
		)

		var (
			enqueues      = make([]uint64, 0)
			noPreplayTxns types.Transactions
			nodes         = make([]*cmptypes.PreplayResTrieNode, 0)
			values        = make([]interface{}, 0)
			missTxns      types.Transactions
		)
		for index, txn := range block.Transactions() {
			if ReuseResult[index].BaseStatus == cmptypes.NoPreplay {
				txEnqueue := r.GetTxEnqueue(txn.Hash())
				if txEnqueue > 0 && txEnqueue <= processTimeNano {
					enqueues = append(enqueues, txEnqueue)
					noPreplayTxns = append(noPreplayTxns, txn)
				}
			}
			if ReuseResult[index].BaseStatus == cmptypes.Miss {
				if node := ReuseResult[index].MissNode; node != nil {
					nodes = append(nodes, node)
					values = append(values, ReuseResult[index].MissValue)
					missTxns = append(missTxns, txn)
				} else {
					log.Error("Detect miss with nil node")
				}
			}
		}
		if len(noPreplayTxns) > 0 || len(missTxns) > 0 {
			reporter.SetBlock(block)
			for i, txn := range missTxns {
				var txnType int
				if txn.To() == nil {
					txnType = 1
				} else {
					if statedb.GetCodeSize(*txn.To()) != 0 {
						txnType = 2
					}
				}
				reporter.SetMissTxn(txn, nodes[i], values[i], txnType)
			}
			for index, txn := range noPreplayTxns {
				reporter.SetNoPreplayTxn(txn, enqueues[index])
			}
		}
		reporter.ReportMiss(txnCount-listen, listen-Package, Package-enqueue, enqueue-preplay)
	}
}

// CachePrint print reuse result of all txns in a block to block folder
func (r *GlobalCache) CachePrint(block *types.Block, reuseResult []*cmptypes.ReuseStatus) {

	cacheResult := &LogBlockCache{
		NoListen:  []*types.Transaction{},
		NoPackage: []*types.Transaction{},
		Error:     []*LogBlockCacheItem{},
		NoPreplay: []*LogBlockCacheItem{},
		Hit:       []*LogBlockCacheItem{},
		Miss:      []*LogBlockCacheItem{},
		Unknown:   []*LogBlockCacheItem{},
	}

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
		for index, tx := range block.Transactions() {
			txListen := r.GetTxListen(tx.Hash())
			if txListen == nil || txListen.ListenTimeNano > processTimeNano {
				cacheResult.NoListen = append(cacheResult.NoListen, tx)
			}
			txPackage := r.GetTxPackage(tx.Hash())
			if txPackage == 0 || txPackage > processTimeNano {
				cacheResult.NoPackage = append(cacheResult.NoPackage, tx)
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
					round, _ := txPreplay.PeekRound(key)
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

			switch reuseResult[index].BaseStatus {
			case cmptypes.Fail:
				cacheResult.Error = append(cacheResult.Error, txCache)

			case cmptypes.NoPreplay:
				cacheResult.NoPreplay = append(cacheResult.NoPreplay, txCache)

			case cmptypes.Hit:
				cacheResult.Hit = append(cacheResult.Hit, txCache)

			case cmptypes.Miss:
				cacheResult.Miss = append(cacheResult.Miss, txCache)

			case cmptypes.Unknown:
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
