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
	lru "github.com/hashicorp/golang-lru"
	"math/big"
	"os"
	"sort"
	"time"
)

const k = 10

// LogBlockInfo define blockInfo log format
type LogBlockInfo struct {
	TxnApply         int64 `json:"apply"`
	BlockFinalize    int64 `json:"finalize"`
	Reuse            int64 `json:"reuse"`
	RealApply        int64 `json:"realApply"`
	TxnFinalize      int64 `json:"txFinalize"`
	Update           int64 `json:"updatePair"`
	GetRW            int64 `json:"getRW"`
	SetDB            int64 `json:"setDB"`
	WaitRealApplyEnd int64 `json:"waitRealApplyEnd"`
	RunTx            int64 `json:"runTx"`
	WaitReuseEnd     int64 `json:"waitReuseEnd"`

	NoListen               int `json:"L"`
	NoListenAndNoEthermine int `json:"L&NoEthermine"`
	NoEnpool               int `json:"Epo"`
	NoEnpending            int `json:"Epe"`
	NoPackage              int `json:"Pa"`
	NoEnqueue              int `json:"Eq"`
	NoPreplay              int `json:"Pr"`
	Hit                    int `json:"H"`
	Miss                   int `json:"M"`
	Unknown                int `json:"U"`

	MixHit                int     `json:"MH"`
	AllDepMixHit          int     `json:"DMH"`
	AllDetailMixHit       int     `json:"TMH"`
	PartialDetailMixHit   int     `json:"PMH"`
	AllDeltaMixHit        int     `json:"AMH"`
	PartialDeltaMixHit    int     `json:"MMH"`
	UnhitHeadCount        [10]int `json:"UHC"`
	TraceHit              int     `json:"RH"`
	DeltaHit              int     `json:"EH"`
	TrieHit               int     `json:"TH"`
	AllDepTraceHit        int     `json:"DTH"`
	AllDetailTraceHit     int     `json:"TTH"`
	PartialDetailTraceHit int     `json:"PTH"`
	OpTraceHit            int     `json:"OTH"`

	TraceMiss   int `json:"TM"`
	MixMiss     int `json:"MM"`
	NoMatchMiss int `json:"NMM"`

	TxPreplayLock int `json:"tL"`
	AbortedTrace  int `json:"aR"`
	AbortedMix    int `json:"aM"`
	AbortedDelta  int `json:"aD"`
	AbortedTrie   int `json:"aT"`

	ReuseGas      uint64        `json:"reuseGas"`
	ProcTime      int64         `json:"procTime"`
	RunMode       string        `json:"runMode"`
	TxnCount      int           `json:"txnCount"`
	Header        *types.Header `json:"header"`
	TryPeekFailed uint64
}

type MissReporter interface {
	SetBlock(block *types.Block)
	SetNoPreplayTxn(txn *types.Transaction, enqueue uint64)
	SetMissTxn(txn *types.Transaction, miss *cmptypes.PreplayResTrieNode, value interface{}, txnType int)
	ReportMiss(noListen, noListenAndEthermine, noEnpool, noEnpending, noPackage, noEnqueue, noPreplay uint64)
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

type TxReuseStatusHistory struct {
	ProcessedCount   uint
	MixHitRoundIds   map[uint64]struct{}
	MixHitRWRcordIds map[uintptr]struct{}
	ReuseHitPathIds  map[uintptr]struct{}
}

func NewTxReuseStatusHistory() *TxReuseStatusHistory {
	return &TxReuseStatusHistory{}
}

func updateNTimesArray(newCount uint, NTimesArray []int64) {
	if newCount == 0 {
		panic("Should never be 0")
	}
	if newCount <= uint(len(NTimesArray)) {
		NTimesArray[newCount-1]++
		if newCount > 1 {
			NTimesArray[newCount-2]--
		}
	}

}

func (h *TxReuseStatusHistory) AddReuseStatus(status *cmptypes.ReuseStatus) {
	processedTxCount++
	h.ProcessedCount++
	updateNTimesArray(h.ProcessedCount, processedNTimesTxCount)

	// hack: determine whether the transaction is an external transfer
	if status.GasUsed == 21000 {
		return
	}

	if status.HitType == cmptypes.MixHit {
		if h.MixHitRoundIds == nil {
			h.MixHitRoundIds = make(map[uint64]struct{})
			if h.ReuseHitPathIds != nil {
				bothHitTxCount++
			}
		}
		oldCount := len(h.MixHitRoundIds)
		h.MixHitRoundIds[status.MixStatus.HitRoundID] = struct{}{}
		newCount := len(h.MixHitRoundIds)
		if newCount > oldCount {
			updateNTimesArray(uint(newCount), mixHitDistinctRoundNTimesTxCount)
		}
		if h.MixHitRWRcordIds == nil {
			h.MixHitRWRcordIds = make(map[uintptr]struct{})
		}
		oldCount = len(h.MixHitRWRcordIds)
		h.MixHitRWRcordIds[status.MixStatus.HitRWRecordID] = struct{}{}
		newCount = len(h.MixHitRWRcordIds)
		if newCount > oldCount {
			updateNTimesArray(uint(newCount), mixHitDistinctRWRecordNTimesTxCount)
		}
	}
	if status.HitType == cmptypes.TraceHit {
		if h.ReuseHitPathIds == nil {
			h.ReuseHitPathIds = make(map[uintptr]struct{})
			if h.MixHitRoundIds != nil {
				bothHitTxCount++
			}
		}
		oldCount := len(h.ReuseHitPathIds)
		h.ReuseHitPathIds[status.TraceStatus.HitPathId] = struct{}{}
		newCount := len(h.ReuseHitPathIds)
		if newCount > oldCount {
			updateNTimesArray(uint(newCount), traceHitDistinctPathNTimesTxCount)
		}
	}
}

var (
	ethermine = common.HexToAddress("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8")
	ToScreen  bool

	Process time.Duration

	Apply            []time.Duration
	Reuse            []time.Duration
	RealApply        []time.Duration
	TxFinalize       []time.Duration
	Update           []time.Duration
	GetRW            []time.Duration
	SetDB            []time.Duration
	WaitRealApplyEnd []time.Duration
	RunTx            []time.Duration
	WaitReuseEnd     []time.Duration

	Finalize       time.Duration
	WaitUpdateRoot time.Duration
	UpdateObj      time.Duration
	HashTrie       time.Duration

	ReuseGasCount uint64

	MaxLongExecutionCost      time.Duration
	MaxLongExecutionReuseCost time.Duration
	LongExecutionCost         time.Duration

	ReuseResult []*cmptypes.ReuseStatus

	TxReuseResultsCache, _ = lru.New(100000)

	// reuse info
	processedTxCount                    int64
	processedNTimesTxCount              = make([]int64, 6) // 1, 2, 3, 4, 5, >5
	mixHitDistinctRoundNTimesTxCount    = make([]int64, 6) // 1, 2, 3, 4, 5, > 5
	mixHitDistinctRWRecordNTimesTxCount = make([]int64, 6) // 1, 2, 3, 4, 5, > 5
	traceHitDistinctPathNTimesTxCount   = make([]int64, 6) // 1, 2, 3, 4, 5, > 5
	bothHitTxCount                      int64

	WarmupMissTxnCount   = make(map[string]int)
	AccountCreate        = make(map[string]int)
	AddrWarmupMiss       = make(map[string]int)
	AddrNoWarmup         = make(map[string]int)
	AddrCreateWarmupMiss = make(map[string]int)
	KeyWarmupMiss        = make(map[string]int)
	KeyNoWarmup          = make(map[string]int)
	KeyCreateWarmupMiss  = make(map[string]int)

	CumWarmupMissTxnCount   = make(map[string]int)
	CumAccountCreate        = make(map[string]int)
	CumAddrWarmupMiss       = make(map[string]int)
	CumAddrNoWarmup         = make(map[string]int)
	CumAddrCreateWarmupMiss = make(map[string]int)
	CumKeyWarmupMiss        = make(map[string]int)
	CumKeyNoWarmup          = make(map[string]int)
	CumKeyCreateWarmupMiss  = make(map[string]int)

	cumApply            time.Duration
	cumReuse            time.Duration
	cumRealApply        time.Duration
	cumTxFinalize       time.Duration
	cumUpdate           time.Duration
	cumGetRW            time.Duration
	cumSetDB            time.Duration
	cumWaitRealApplyEnd time.Duration
	cumRunTx            time.Duration
	cumWaitReuseEnd     time.Duration

	cumFinalize       time.Duration
	cumWaitUpdateRoot time.Duration
	cumUpdateObj      time.Duration
	cumHashTrie       time.Duration

	blkCount          uint64
	txnCount          uint64
	listen            uint64
	listenOrEthermine uint64
	enpool            uint64
	enpending         uint64
	Package           uint64
	enqueue           uint64
	preplay           uint64
	hit               uint64
	unknown           uint64
	miss              uint64

	mixHit                uint64
	allDepMixHit          uint64
	allDetailMixHit       uint64
	partialDetailMixHit   uint64
	allDeltaMixHit        uint64
	partialDeltaMixHit    uint64
	traceHit              uint64
	deltaHit              uint64
	trieHit               uint64
	allDepTraceHit        uint64
	allDetailTraceHit     uint64
	partialDetailTraceHit uint64
	opTraceHit            uint64

	txPreplayLock uint64
	abortedTrace  uint64
	abortedMix    uint64
	abortedDelta  uint64
	abortedTrie   uint64

	traceMiss   uint64
	mixMiss     uint64
	noMatchMiss uint64

	LockCount     [4]uint64
	tryPeekFailed uint64

	reuseGasUsed uint64
	totalGasUsed uint64
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
	AccountCreate = make(map[string]int)
	AddrWarmupMiss = make(map[string]int)
	AddrNoWarmup = make(map[string]int)
	AddrCreateWarmupMiss = make(map[string]int)
	KeyWarmupMiss = make(map[string]int)
	KeyNoWarmup = make(map[string]int)
	KeyCreateWarmupMiss = make(map[string]int)

	Apply = make([]time.Duration, 0, size)
	Reuse = make([]time.Duration, 0, size)
	RealApply = make([]time.Duration, 0, size)
	TxFinalize = make([]time.Duration, 0, size)
	Update = make([]time.Duration, 0, size)
	GetRW = make([]time.Duration, 0, size)
	SetDB = make([]time.Duration, 0, size)
	WaitRealApplyEnd = make([]time.Duration, 0, size)
	RunTx = make([]time.Duration, 0, size)
	WaitReuseEnd = make([]time.Duration, 0, size)

	Finalize = 0
	WaitUpdateRoot = 0
	UpdateObj = 0
	HashTrie = 0

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
func (r *GlobalCache) InfoPrint(block *types.Block, signer types.Signer, cfg vm.Config, synced bool, reporter MissReporter,
	statedb *state.StateDB) (noEnpoolSender map[common.Address]struct{}) {

	noEnpoolSender = make(map[common.Address]struct{})

	var (
		sumApply            = SumDuration(Apply)
		sumReuse            = SumDuration(Reuse)
		sumRealApply        = SumDuration(RealApply)
		sumTxFinalize       = SumDuration(TxFinalize)
		sumUpdate           = SumDuration(Update)
		sumGetRW            = SumDuration(GetRW)
		sumSetDB            = SumDuration(SetDB)
		sumWaitRealApplyEnd = SumDuration(WaitRealApplyEnd)
		sumRunTx            = SumDuration(RunTx)
		sumWaitReuseEnd     = SumDuration(WaitReuseEnd)
	)

	infoResult := &LogBlockInfo{
		TxnApply:         sumApply.Microseconds(),
		BlockFinalize:    Finalize.Microseconds(),
		Reuse:            sumReuse.Microseconds(),
		RealApply:        sumRealApply.Microseconds(),
		TxnFinalize:      sumTxFinalize.Microseconds(),
		Update:           sumUpdate.Microseconds(),
		GetRW:            sumGetRW.Microseconds(),
		SetDB:            sumSetDB.Microseconds(),
		WaitRealApplyEnd: sumWaitRealApplyEnd.Microseconds(),
		RunTx:            sumRunTx.Microseconds(),
		WaitReuseEnd:     sumWaitReuseEnd.Microseconds(),
		ReuseGas:         ReuseGasCount,
		ProcTime:         Process.Nanoseconds(),
		TxnCount:         len(block.Transactions()),
		Header:           block.Header(),
	}

	processTimeNano := r.PeekBlockPre(block.Hash()).ListenTimeNano
	if len(ReuseResult) != 0 {
		for index, tx := range block.Transactions() {
			if ReuseResult[index].TryPeekFailed {
				infoResult.TryPeekFailed++
			}
			switch ReuseResult[index].BaseStatus {
			case cmptypes.NoPreplay:
				infoResult.NoPreplay++
				txEnqueue := r.GetTxEnqueue(tx.Hash())
				if txEnqueue[0] == 0 || txEnqueue[0] > processTimeNano {
					infoResult.NoEnqueue++
					txPackage := r.GetTxPackage(tx.Hash())
					if txPackage == 0 || txPackage > processTimeNano {
						infoResult.NoPackage++
						txEnpending := r.GetTxEnpending(tx.Hash())
						if txEnpending == 0 || txEnpending > processTimeNano {
							infoResult.NoEnpending++
							txEnpool := r.GetTxEnpool(tx.Hash())
							if txEnpool == 0 || txEnpool > processTimeNano {
								infoResult.NoEnpool++
								txListen := r.GetTxListen(tx.Hash())
								if txListen == nil || txListen.ListenTimeNano > processTimeNano {
									infoResult.NoListen++
									if sender, _ := types.Sender(signer, tx); sender != ethermine {
										infoResult.NoListenAndNoEthermine++
									}
								} else {
									if tx.To() != nil && statedb.GetCodeSize(*tx.To()) == 0 {
										sender, _ := types.Sender(signer, tx)
										noEnpoolSender[sender] = struct{}{}
									}
								}
							}
						}
					}
				}
			case cmptypes.Hit:
				infoResult.Hit++
				switch ReuseResult[index].HitType {
				case cmptypes.MixHit:
					infoResult.MixHit++
					switch ReuseResult[index].MixStatus.MixHitType {
					case cmptypes.AllDepHit:
						infoResult.AllDepMixHit++
					case cmptypes.AllDetailHit:
						infoResult.AllDetailMixHit++
					case cmptypes.PartialHit:
						infoResult.PartialDetailMixHit++
						unHitHead := ReuseResult[index].MixStatus.DepUnmatchedInHead
						if unHitHead < 9 {
							infoResult.UnhitHeadCount[unHitHead]++
						} else {
							infoResult.UnhitHeadCount[9]++
						}
					case cmptypes.AllDeltaHit:
						infoResult.AllDeltaMixHit++
					case cmptypes.PartialDeltaHit:
						infoResult.PartialDeltaMixHit++
					}

				case cmptypes.TrieHit:
					infoResult.TrieHit++
				case cmptypes.DeltaHit:
					infoResult.DeltaHit++
				case cmptypes.TraceHit:
					infoResult.TraceHit++
					switch ReuseResult[index].TraceStatus.TraceHitType {
					case cmptypes.AllDepHit:
						infoResult.AllDepTraceHit++
					case cmptypes.AllDetailHit:
						infoResult.AllDetailTraceHit++
					case cmptypes.PartialHit:
						infoResult.PartialDetailTraceHit++
					case cmptypes.OpHit:
						infoResult.OpTraceHit++
					default:
						panic(fmt.Sprintf("Unknown TraceHitType %v", ReuseResult[index].TraceStatus.TraceHitType))
					}
				}
			case cmptypes.Miss:
				infoResult.Miss++
				switch ReuseResult[index].MissType {
				case cmptypes.TraceMiss:
					infoResult.TraceMiss++
				case cmptypes.NoMatchMiss:
					infoResult.NoMatchMiss++
				case cmptypes.MixMiss:
					infoResult.MixMiss++
				default:
					panic(fmt.Sprintf("Known miss type %v", ReuseResult[index].MissType))
				}
			case cmptypes.Unknown:
				infoResult.Unknown++
				switch ReuseResult[index].AbortStage {
				case cmptypes.TxPreplayLock:
					infoResult.TxPreplayLock++
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

	if logDir != "" {
		filePath := fmt.Sprintf("%s/block/%s",
			logDir,
			block.Number().String())

		infoFileName := fmt.Sprintf("%s_%s_info_%d.json",
			block.Number().String(), block.Hash().String(), block.Header().Time)

		r.LogPrint(filePath, infoFileName, infoResult)
	}

	if r.SyncStart == 0 && synced {
		r.SyncStart = block.NumberU64()
	}
	if r.SyncStart != 0 && block.NumberU64() >= r.SyncStart+k {
		ToScreen = true
	}

	if !cfg.MSRAVMSettings.IsEmulateMode && !ToScreen {
		return
	}

	for word := range WarmupMissTxnCount {
		log.Warn("                     there is a warmupmiss word !!!!!!!!!!!!!!!", "word", word)

		CumWarmupMissTxnCount[word] += WarmupMissTxnCount[word]
		CumAccountCreate[word] += AccountCreate[word]
		CumAddrWarmupMiss[word] += AddrWarmupMiss[word]
		CumAddrNoWarmup[word] += AddrNoWarmup[word]
		CumAddrCreateWarmupMiss[word] += AddrCreateWarmupMiss[word]
		CumKeyWarmupMiss[word] += KeyWarmupMiss[word]
		CumKeyNoWarmup[word] += KeyNoWarmup[word]
		CumKeyCreateWarmupMiss[word] += KeyCreateWarmupMiss[word]
	}

	var keySort []string
	for word := range CumWarmupMissTxnCount {
		keySort = append(keySort, word)
	}
	sort.Strings(keySort)

	for _, word := range keySort {
		if CumWarmupMissTxnCount[word] > 0 {
			context := []interface{}{"type", word,
				"cum txn miss", CumWarmupMissTxnCount[word], "txn miss", WarmupMissTxnCount[word],
				"cum addr create", CumAccountCreate[word], "addr create", AccountCreate[word],
			}
			if CumAddrWarmupMiss[word] > 0 {
				if cfg.MSRAVMSettings.WarmupMissDetail {
					context = append(context, "cum addr miss-create",
						fmt.Sprintf("%d(%d)-%d", CumAddrWarmupMiss[word], CumAddrNoWarmup[word], CumAddrCreateWarmupMiss[word]))
				} else {
					context = append(context, "cum addr miss-create",
						fmt.Sprintf("%d-%d", CumAddrWarmupMiss[word], CumAddrCreateWarmupMiss[word]))
				}
			}
			if AddrWarmupMiss[word] > 0 {
				if cfg.MSRAVMSettings.WarmupMissDetail {
					context = append(context, "addr miss-create",
						fmt.Sprintf("%d(%d)-%d", AddrWarmupMiss[word], AddrNoWarmup[word], AddrCreateWarmupMiss[word]))
				} else {
					context = append(context, "addr miss-create",
						fmt.Sprintf("%d-%d", AddrWarmupMiss[word], AddrCreateWarmupMiss[word]))
				}
			}
			if CumKeyWarmupMiss[word] > 0 {
				if cfg.MSRAVMSettings.WarmupMissDetail {
					context = append(context, "cum key miss-create",
						fmt.Sprintf("%d(%d)-%d", CumKeyWarmupMiss[word], CumKeyNoWarmup[word], CumKeyCreateWarmupMiss[word]))
				} else {
					context = append(context, "cum key miss-create",
						fmt.Sprintf("%d-%d", CumKeyWarmupMiss[word], CumKeyCreateWarmupMiss[word]))
				}
			}
			if KeyWarmupMiss[word] > 0 {
				if cfg.MSRAVMSettings.WarmupMissDetail {
					context = append(context, "key miss-create",
						fmt.Sprintf("%d(%d)-%d", KeyWarmupMiss[word], KeyNoWarmup[word], KeyCreateWarmupMiss[word]))
				} else {
					context = append(context, "key miss-create",
						fmt.Sprintf("%d-%d", KeyWarmupMiss[word], KeyCreateWarmupMiss[word]))
				}
			}
			log.Info("Warmup miss statistics", context...)
		}
	}

	cumApply += sumApply
	cumReuse += sumReuse
	cumRealApply += sumRealApply
	cumTxFinalize += sumTxFinalize
	cumUpdate += sumUpdate
	cumGetRW += sumGetRW
	cumSetDB += sumSetDB
	cumWaitRealApplyEnd += sumWaitRealApplyEnd
	cumRunTx += sumRunTx
	cumWaitReuseEnd += sumWaitReuseEnd

	cumFinalize += Finalize
	cumWaitUpdateRoot += WaitUpdateRoot
	cumUpdateObj += UpdateObj
	cumHashTrie += HashTrie

	context := []interface{}{"apply", common.PrettyDuration(sumApply)}
	if sumApply != 0 {
		context = append(context,
			"reuse/apply", fmt.Sprintf("%.2f", float64(sumReuse)/float64(sumApply)),
			"realApply/apply", fmt.Sprintf("%.2f", float64(sumRealApply)/float64(sumApply)),
			"finalize/apply", fmt.Sprintf("%.2f", float64(sumTxFinalize)/float64(sumApply)),
			"update/apply", fmt.Sprintf("%.2f", float64(sumUpdate)/float64(sumApply)),
		)
		if sumReuse != 0 {
			context = append(context,
				"getRW/reuse", fmt.Sprintf("%.2f", float64(sumGetRW)/float64(sumReuse)),
				"setDB/reuse", fmt.Sprintf("%.2f", float64(sumSetDB)/float64(sumReuse)),
			)
			if cfg.MSRAVMSettings.ParallelizeReuse {
				context = append(context,
					"waitRealApplyEnd/reuse", fmt.Sprintf("%.2f", float64(sumWaitRealApplyEnd)/float64(sumReuse)),
				)
			}
		}
		if sumRealApply != 0 && cfg.MSRAVMSettings.ParallelizeReuse {
			context = append(context,
				"runTx/realApply", fmt.Sprintf("%.2f", float64(sumRunTx)/float64(sumRealApply)),
				"waitReuseEnd/realApply", fmt.Sprintf("%.2f", float64(sumWaitReuseEnd)/float64(sumRealApply)),
			)
		}
	}
	if Finalize != 0 {
		context = append(context,
			"finalize", common.PrettyDuration(Finalize),
			"waitUpdateRoot/finalize", fmt.Sprintf("%.2f", float64(WaitUpdateRoot)/float64(Finalize)),
			"updateObj/finalize", fmt.Sprintf("%.2f", float64(UpdateObj)/float64(Finalize)),
			"hashTrie/finalize", fmt.Sprintf("%.2f", float64(HashTrie)/float64(Finalize)),
		)
	}
	log.Info("Time consumption detail", context...)

	context = []interface{}{"apply", common.PrettyDuration(cumApply)}
	if cumApply != 0 {
		context = append(context,
			"reuse/apply", fmt.Sprintf("%.2f", float64(cumReuse)/float64(cumApply)),
			"realApply/apply", fmt.Sprintf("%.2f", float64(cumRealApply)/float64(cumApply)),
			"finalize/apply", fmt.Sprintf("%.2f", float64(cumTxFinalize)/float64(cumApply)),
			"update/apply", fmt.Sprintf("%.2f", float64(cumUpdate)/float64(cumApply)),
		)
		if cumReuse != 0 {
			context = append(context,
				"getRW/reuse", fmt.Sprintf("%.2f", float64(cumGetRW)/float64(cumReuse)),
				"setDB/reuse", fmt.Sprintf("%.2f", float64(cumSetDB)/float64(cumReuse)),
			)
			if cfg.MSRAVMSettings.ParallelizeReuse {
				context = append(context,
					"waitRealApplyEnd/reuse", fmt.Sprintf("%.2f", float64(cumWaitRealApplyEnd)/float64(cumReuse)),
				)
			}
		}
		if cumRealApply != 0 && cfg.MSRAVMSettings.ParallelizeReuse {
			context = append(context,
				"runTx/realApply", fmt.Sprintf("%.2f", float64(cumRunTx)/float64(cumRealApply)),
				"waitReuseEnd/realApply", fmt.Sprintf("%.2f", float64(cumWaitReuseEnd)/float64(cumRealApply)),
			)
		}
	}
	if cumFinalize != 0 {
		context = append(context,
			"finalize", common.PrettyDuration(cumFinalize),
			"waitUpdateRoot/finalize", fmt.Sprintf("%.2f", float64(cumWaitUpdateRoot)/float64(cumFinalize)),
			"updateObj/finalize", fmt.Sprintf("%.2f", float64(cumUpdateObj)/float64(cumFinalize)),
			"hashTrie/finalize", fmt.Sprintf("%.2f", float64(cumHashTrie)/float64(cumFinalize)),
		)
	}
	log.Info("Cumulative time consumption detail", context...)

	if cfg.MSRAVMSettings.EnablePreplay && cfg.MSRAVMSettings.CmpReuse {
		context = []interface{}{"Total", fmt.Sprintf("%03d", infoResult.TxnCount)}

		listenCnt := infoResult.TxnCount - infoResult.NoListen
		enpoolCnt := infoResult.TxnCount - infoResult.NoEnpool
		enpendingCnt := infoResult.TxnCount - infoResult.NoEnpending
		packageCnt := infoResult.TxnCount - infoResult.NoPackage
		enqueueCnt := infoResult.TxnCount - infoResult.NoEnqueue
		preplayCnt := infoResult.TxnCount - infoResult.NoPreplay

		if infoResult.TxnCount > 0 {
			var listenRate, enpoolRate, enpendingRate, packageRate, enqueueRate, preplayRate, hitRate, missRate, unknownRate,
			mixHitRate, trieHitRate, deltaHitRate, traceHitRate float64
			var reuseGasRate float64

			listenRate = float64(listenCnt) / float64(infoResult.TxnCount)
			enpoolRate = float64(enpoolCnt) / float64(infoResult.TxnCount)
			enpendingRate = float64(enpendingCnt) / float64(infoResult.TxnCount)
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

			context = append(context, "Total", fmt.Sprintf("%03d", infoResult.TxnCount),
				"Listen", fmt.Sprintf("%03d(%.2f)", listenCnt, listenRate),
				"Enpool", fmt.Sprintf("%03d(%.2f)", enpoolCnt, enpoolRate),
				"Enpending", fmt.Sprintf("%03d(%.2f)", enpendingCnt, enpendingRate),
				"Package", fmt.Sprintf("%03d(%.2f)", packageCnt, packageRate),
				"Enqueue", fmt.Sprintf("%03d(%.2f)", enqueueCnt, enqueueRate),
				"Preplay", fmt.Sprintf("%03d(%.2f)", preplayCnt, preplayRate),
				"Hit", fmt.Sprintf("%03d(%.2f)", infoResult.Hit, hitRate),
			)
			if infoResult.MixHit > 0 {
				context = append(context, "MixHit", fmt.Sprintf("%03d(%.2f)-[AllDep:%03d|AllDetail:%03d|PartialDetail:%03d|AllDelta:%03d|PartialDelta:%03d]", infoResult.MixHit, mixHitRate,
					infoResult.AllDepMixHit, infoResult.AllDetailMixHit, infoResult.PartialDetailMixHit, infoResult.AllDeltaMixHit, infoResult.PartialDeltaMixHit),
					"MixUnhitHead", fmt.Sprint(infoResult.UnhitHeadCount))
			}
			if infoResult.TraceHit > 0 {
				context = append(context, "TraceHit", fmt.Sprintf("%03d(%.2f)-[AllDep:%03d|AllDetail:%03d|PartialDetail:%03d|Op:%03d]",
					infoResult.TraceHit, traceHitRate, infoResult.AllDepTraceHit, infoResult.AllDetailTraceHit, infoResult.PartialDetailTraceHit, infoResult.OpTraceHit))
			}
			if infoResult.TrieHit > 0 || infoResult.DeltaHit > 0 {
				context = append(context, "DH-TH", fmt.Sprintf("%03d(%.2f)-%03d(%.2f)",
					infoResult.DeltaHit, deltaHitRate, infoResult.TrieHit, trieHitRate))
			}
			if infoResult.Miss > 0 {
				context = append(context, "Miss", fmt.Sprintf("%03d(%.2f)-[TraceMiss:%03d|MixMiss:%03d|NoMatchMiss:%03d]",
					infoResult.Miss, missRate, infoResult.TraceMiss, infoResult.MixMiss, infoResult.NoMatchMiss))
			}
			if infoResult.Unknown > 0 {
				context = append(context, "Unknown", fmt.Sprintf("%03d(%.2f)", infoResult.Unknown, unknownRate),
					"TxPreplayLock", infoResult.TxPreplayLock)
				if cfg.MSRAVMSettings.ParallelizeReuse {
					context = append(context, "AbortStage(R-M-D-T)", fmt.Sprintf("%03d-%03d-%03d-%03d",
						infoResult.AbortedTrace, infoResult.AbortedMix, infoResult.AbortedDelta, infoResult.AbortedTrie))
				}
			}

			context = append(context, "ReuseGas", fmt.Sprintf("%d(%.2f)", infoResult.ReuseGas, reuseGasRate))
		}

		log.Info("Block reuse", context...)

		blkCount++
		txnCount += uint64(infoResult.TxnCount)
		listen += uint64(listenCnt)
		listenOrEthermine += uint64(infoResult.TxnCount - infoResult.NoListenAndNoEthermine)
		enpool += uint64(enpoolCnt)
		enpending += uint64(enpendingCnt)
		Package += uint64(packageCnt)
		enqueue += uint64(enqueueCnt)
		preplay += uint64(preplayCnt)
		hit += uint64(infoResult.Hit)
		miss += uint64(infoResult.Miss)
		unknown += uint64(infoResult.Unknown)

		mixHit += uint64(infoResult.MixHit)
		allDepMixHit += uint64(infoResult.AllDepMixHit)
		allDetailMixHit += uint64(infoResult.AllDetailMixHit)
		partialDetailMixHit += uint64(infoResult.PartialDetailMixHit)
		allDeltaMixHit += uint64(infoResult.AllDeltaMixHit)
		partialDeltaMixHit += uint64(infoResult.PartialDeltaMixHit)
		traceHit += uint64(infoResult.TraceHit)
		allDepTraceHit += uint64(infoResult.AllDepTraceHit)
		allDetailTraceHit += uint64(infoResult.AllDetailTraceHit)
		partialDetailTraceHit += uint64(infoResult.PartialDetailTraceHit)
		opTraceHit += uint64(infoResult.OpTraceHit)
		deltaHit += uint64(infoResult.DeltaHit)
		trieHit += uint64(infoResult.TrieHit)

		traceMiss += uint64(infoResult.TraceMiss)
		mixMiss += uint64(infoResult.MixMiss)
		noMatchMiss += uint64(infoResult.NoMatchMiss)

		txPreplayLock += uint64(infoResult.TxPreplayLock)
		abortedTrace += uint64(infoResult.AbortedTrace)
		abortedMix += uint64(infoResult.AbortedMix)
		abortedDelta += uint64(infoResult.AbortedDelta)
		abortedTrie += uint64(infoResult.AbortedTrie)

		reuseGasUsed += infoResult.ReuseGas
		totalGasUsed += infoResult.Header.GasUsed

		tryPeekFailed += infoResult.TryPeekFailed

		context = []interface{}{
			"block", blkCount, "txn", txnCount,
			"listen", fmt.Sprintf("%d(%.3f)", listen, float64(listen)/float64(txnCount)),
			"enpool", fmt.Sprintf("%d(%.3f)", enpool, float64(enpool)/float64(txnCount)),
			"enpending", fmt.Sprintf("%d(%.3f)", enpending, float64(enpending)/float64(txnCount)),
			"package", fmt.Sprintf("%d(%.3f)", Package, float64(Package)/float64(txnCount)),
			"enqueue", fmt.Sprintf("%d(%.3f)", enqueue, float64(enqueue)/float64(txnCount)),
			"preplay", fmt.Sprintf("%d(%.3f)", preplay, float64(preplay)/float64(txnCount)),
			"hit", fmt.Sprintf("%d(%.3f)", hit, float64(hit)/float64(txnCount)),
		}
		if mixHit > 0 {
			context = append(context, "MixHit", fmt.Sprintf("%d(%.3f)-[AllDep:%d|AllDetail:%d|PartialDetail:%d|AllDelta:%d|PartialDelta:%d]",
				mixHit, float64(mixHit)/float64(txnCount), allDepMixHit, allDetailMixHit,
				partialDetailMixHit, allDeltaMixHit, partialDeltaMixHit))
		}
		if traceHit > 0 {
			context = append(context, "TraceHit", fmt.Sprintf("%03d(%.3f)-[AllDep:%03d|AllDetail:%03d|PartialDetail:%03d|Op:%03d(%.3f)]",
				traceHit, float64(traceHit)/float64(txnCount), allDepTraceHit, allDetailTraceHit, partialDetailTraceHit, opTraceHit, float64(opTraceHit)/float64(txnCount)))
		}

		context = append(context, "DH-TH", fmt.Sprintf("%d(%.3f)-%d(%.3f)",
			deltaHit, float64(deltaHit)/float64(txnCount), trieHit, float64(trieHit)/float64(txnCount)),
			"miss", fmt.Sprintf("%d(%.3f)-[TraceMiss:%03d|MixMiss:%03d|NoMatchMiss:%03d]", miss, float64(miss)/float64(txnCount),
				traceMiss, mixMiss, noMatchMiss),
			"gasUsed", fmt.Sprintf("%d(%.3f)", reuseGasUsed, float64(reuseGasUsed)/float64(totalGasUsed)))

		if unknown > 0 {
			context = append(context, "unknown", fmt.Sprintf("%d(%.3f)", unknown, float64(unknown)/float64(txnCount)),
				"txPreplayLock", txPreplayLock)
			if cfg.MSRAVMSettings.ParallelizeReuse {
				context = append(context, "abortStage(R-M-D-T)", fmt.Sprintf("%d-%d-%d-%d",
					abortedMix, abortedTrace, abortedDelta, abortedTrie))
			}
		}
		log.Info("Cumulative block reuse", context...)

		for _, rr := range ReuseResult {
			// collect cumulative hit info
			var txHistory *TxReuseStatusHistory
			th, ok := TxReuseResultsCache.Get(rr.TxHash)
			if ok {
				txHistory = th.(*TxReuseStatusHistory)
			} else {
				txHistory = NewTxReuseStatusHistory()
				TxReuseResultsCache.Add(rr.TxHash, txHistory)
			}
			txHistory.AddReuseStatus(rr)
		}

		log.Info("Cumulative reuse hit", "txs", processedTxCount,
			"processedDist", fmt.Sprintf("[1|2|3|4|5|>5]=[%v,%v,%v,%v,%v,%v]", processedNTimesTxCount[0], processedNTimesTxCount[1], processedNTimesTxCount[2],
				processedNTimesTxCount[3], processedNTimesTxCount[4], processedNTimesTxCount[5]),
			"mixRoundDist", fmt.Sprintf("[1|2|3|4|5|>5]=[%v,%v,%v,%v,%v,%v]", mixHitDistinctRoundNTimesTxCount[0], mixHitDistinctRoundNTimesTxCount[1], mixHitDistinctRoundNTimesTxCount[2],
				mixHitDistinctRoundNTimesTxCount[3], mixHitDistinctRoundNTimesTxCount[4], mixHitDistinctRoundNTimesTxCount[5]),
			"mixRWDist", fmt.Sprintf("[1|2|3|4|5|>5]=[%v,%v,%v,%v,%v,%v]", mixHitDistinctRWRecordNTimesTxCount[0], mixHitDistinctRWRecordNTimesTxCount[1], mixHitDistinctRWRecordNTimesTxCount[2],
				mixHitDistinctRWRecordNTimesTxCount[3], mixHitDistinctRWRecordNTimesTxCount[4], mixHitDistinctRWRecordNTimesTxCount[5]),
			"tracePathDist", fmt.Sprintf("[1|2|3|4|5|>5]=[%v,%v,%v,%v,%v,%v]", traceHitDistinctPathNTimesTxCount[0], traceHitDistinctPathNTimesTxCount[1], traceHitDistinctPathNTimesTxCount[2],
				traceHitDistinctPathNTimesTxCount[3], traceHitDistinctPathNTimesTxCount[4], traceHitDistinctPathNTimesTxCount[5]),
			"bothHitTxCount", bothHitTxCount,
		)

		log.Info("Tries lock", "count", fmt.Sprintf("%d-%d-%d-%d", LockCount[0], LockCount[1], LockCount[2], LockCount[3]), "tryPeekFailed",
			tryPeekFailed)

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
				if txEnqueue[0] > 0 && txEnqueue[0] <= processTimeNano {
					enqueues = append(enqueues, txEnqueue[1])
					noPreplayTxns = append(noPreplayTxns, txn)
				}
			}
			if ReuseResult[index].BaseStatus == cmptypes.Miss {
				if node := ReuseResult[index].MissNode; node != nil {
					nodes = append(nodes, node)
					values = append(values, ReuseResult[index].MissValue)
					missTxns = append(missTxns, txn)
				} else {
					if !cfg.MSRAVMSettings.NoOverMatching && !cfg.MSRAVMSettings.NoMemoization { // when over-matching is disabled, mixtree is turned off, the node will be nil
						//log.Error("Detect miss with nil node")
					}
				}
			}
		}

		totalSnapGet := r.AccountSnapGetType[0] + r.AccountSnapGetType[1] + r.AccountSnapGetType[2]
		log.Info("How to get snap", "totalSnapGet", totalSnapGet, "stateObject", r.AccountSnapGetType[0],
			"lru", r.AccountSnapGetType[1], "Trie", r.AccountSnapGetType[2])

		if !cfg.MSRAVMSettings.NoReuse {
			if len(noPreplayTxns) > 0 || len(missTxns) > 0 {
				reporter.SetBlock(block)
				for index, txn := range noPreplayTxns {
					reporter.SetNoPreplayTxn(txn, enqueues[index])
				}
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
			}
			reporter.ReportMiss(txnCount-listen, listenOrEthermine-listen, listen-enpool, enpool-enpending, enpending-Package, Package-enqueue, enqueue-preplay)
		}
	}
	return noEnpoolSender
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

			txPreplay := r.PeekTxPreplayInNonProcess(tx.Hash())
			if txPreplay != nil {
				txPreplay.RLockRound()
				txCache.Rounds = txPreplay.KeysOfRound()
				for _, key := range txCache.Rounds {
					round, _ := txPreplay.PeekRound(key)
					if round.Filled != -1 {
						continue
					}
					txCache.ReducedRounds = append(txCache.ReducedRounds, key)
				}
				txPreplay.RUnlockRound()
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
		txPreplay := r.PeekTxPreplayInNonProcess(tx.Hash())
		if txPreplay == nil {
			log.Debug("[PreplayPrint] getTxPreplay Error", "txHash", tx.Hash())
			preplayResult.Result = append(preplayResult.Result, nil)
			continue
		}

		txPreplay.RLockRound()

		round, _ := txPreplay.PeekRound(RoundID)
		if round == nil {
			log.Debug("[PreplayPrint] getRoundID Error", "txHash", tx.Hash(), "roundID", RoundID)
			preplayResult.Result = append(preplayResult.Result, nil)
			txPreplay.RUnlockRound()
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

		txPreplay.RUnlockRound()
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
