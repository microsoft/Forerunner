// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package optipreplayer

// type Aggregator struct {
// 	executorNum  int
// 	filepath     string
// 	resultChs    []chan *cache.PreplayResult
// 	resultFinish []bool
// 	mergeCh      chan *cache.PreplayResult
// 	mergeFinish  bool
// 	results      map[common.Hash]cache.Totals
// 	wg           *sync.WaitGroup
// 	txsMap       map[common.Hash]*types.Transaction
// 	currentState *cache.CurrentState
// 	writer       *writer.Writer

// 	enable bool
// }

// func NewAggregator(strategy string, executorNum int, txs types.Transactions, currentState *cache.CurrentState, filePath string) *Aggregator {
// 	aggregator := &Aggregator{
// 		executorNum:  executorNum,
// 		resultChs:    make([]chan *cache.PreplayResult, executorNum),
// 		resultFinish: make([]bool, executorNum),
// 		results:      make(map[common.Hash]cache.Totals),
// 		wg:           &sync.WaitGroup{},
// 		txsMap:       make(map[common.Hash]*types.Transaction),
// 		currentState: currentState,
// 		filepath:     filePath,
// 		enable:       false,
// 	}

// 	if aggregator.enable {
// 		aggregator.mergeCh = make(chan *cache.PreplayResult, 100000)
// 		aggregator.mergeFinish = false

// 		for _, tx := range txs {
// 			aggregator.txsMap[tx.Hash()] = tx
// 		}

// 		for i := 0; i < aggregator.executorNum; i++ {
// 			aggregator.resultChs[i] = make(chan *cache.PreplayResult, 1000)
// 			aggregator.resultFinish[i] = false
// 		}

// 		for hash := range aggregator.txsMap {
// 			aggregator.results[hash] = cache.Totals{}
// 		}

// 		aggregator.writer = writer.NewWriter(fmt.Sprintf(aggregator.filepath+"/%s-%s-%d-%d.json", strategy, currentState.PreplayID, currentState.Number, currentState.SnapshotTimestamp))

// 		aggregator.writer.Start()
// 		go aggregator.fetchLoop()
// 	} else {
// 		aggregator.mergeCh = make(chan *cache.PreplayResult, 1)
// 		aggregator.mergeFinish = false

// 		for i := 0; i < aggregator.executorNum; i++ {
// 			aggregator.resultChs[i] = make(chan *cache.PreplayResult, 1)
// 			aggregator.resultFinish[i] = false
// 		}
// 	}

// 	go aggregator.isFetchEnd()

// 	return aggregator
// }

// func (a *Aggregator) setResultChFinish(i int) {
// 	a.resultFinish[i] = true
// }

// func (a *Aggregator) isFetchEnd() {
// 	for {
// 		time.Sleep(10 * time.Microsecond)
// 		isFetchEnd := true
// 		for i := 0; i < a.executorNum; i++ {
// 			isFetchEnd = (isFetchEnd && a.resultFinish[i] && len(a.resultChs[i]) == 0)
// 		}
// 		if isFetchEnd {
// 			a.mergeFinish = true
// 			break
// 		}
// 	}
// }

// func (a *Aggregator) fetchLoop() {
// 	for i := 0; i < a.executorNum; i++ {
// 		go func(i int) {
// 			for {
// 				if len(a.resultChs[i]) == 0 {
// 					if a.resultFinish[i] {
// 						break
// 					}
// 					time.Sleep(10 * time.Microsecond)
// 					continue
// 				}
// 				result := <-a.resultChs[i]
// 				a.mergeCh <- result
// 			}
// 		}(i)
// 	}
// }

// func (a *Aggregator) mergeLoop() {
// 	checkIdx := 0
// 	for {
// 		if len(a.mergeCh) == 0 {
// 			if a.mergeFinish {
// 				if a.enable {
// 					a.writer.SetFinish()
// 					a.writer.Close()
// 				}
// 				break
// 			}
// 			time.Sleep(10 * time.Microsecond)
// 			continue
// 		}

// 		if a.enable {
// 			result := <-a.mergeCh
// 			if _, ok := a.results[result.TxHash]; !ok {
// 				a.results[result.TxHash] = cache.Totals{}
// 			}
// 			a.results[result.TxHash] = append(a.results[result.TxHash], result.ToTotal())
// 			if len(a.results[result.TxHash]) == a.executorNum {
// 				totalResult := &TotalResult{
// 					CurrentState: a.currentState,
// 					TxHash:       result.TxHash,
// 					Tx:           a.txsMap[result.TxHash],
// 					Totals:       a.results[result.TxHash],
// 				}
// 				totalResult.Simplify()

// 				checkIdx++
// 				totalResult.CheckIdx = checkIdx

// 				a.writer.SendMsg(totalResult.ToJSON())
// 				delete(a.results, result.TxHash)
// 			}
// 		}
// 	}
// }

// type TotalResult struct {
// 	CurrentState *cache.CurrentState `json:"currentState"`
// 	TxHash       common.Hash         `json:"txhash"`
// 	Tx           *types.Transaction  `json:"tx"`
// 	Totals       cache.Totals        `json:"results"`
// 	CheckIdx     int                 `json:"checkIdx"`
// }

// func max(a int, b int) int {
// 	if a > b {
// 		return a
// 	}
// 	return b
// }

// func (t *TotalResult) Simplify() {
// 	lenTotals := len(t.Totals)
// 	dealed := make([]bool, lenTotals)
// 	newTotals := cache.Totals{}

// 	for i := 0; i < lenTotals; i++ {
// 		dealed[i] = false
// 	}

// 	for i := 0; i < lenTotals; i++ {
// 		if dealed[i] {
// 			continue
// 		}
// 		dealed[i] = true
// 		total := t.Totals[i]
// 		total.Weight++
// 		for j := i + 1; j < lenTotals; j++ {
// 			if t.Totals[i].Equal(t.Totals[j]) {
// 				dealed[j] = true
// 				total.Weight++
// 			}
// 		}
// 		newTotals = append(newTotals, total)
// 	}

//for idx := range newTotals {
//	newTotals[idx].Index = idx
//
//	if idx == 0 {
//		newTotals[idx].BaseIndex = -1
//		if newTotals[idx].Result.Receipt == nil {
//			newTotals[idx].Result.Receipt = &Receipt{}
//		}
//		if newTotals[idx].Result.Range == nil {
//			newTotals[idx].Result.Range = &Range{}
//		}
//		continue
//	}
//
//	newTotals[idx].BaseIndex = 0
//	newTotals[idx].Delta = &Delta{
//		Receipt: &DeltaReceipt{},
//		Range: &DeltaRange{},
//	}
//
//	if newTotals[idx].Result.Status != newTotals[0].Result.Status {
//		newTotals[idx].Delta.Status = "rs" + newTotals[idx].Result.Status
//	}
//
//	if newTotals[idx].Result.Confirmation != newTotals[0].Result.Confirmation {
//		newTotals[idx].Delta.Confirmation = "ri" + strconv.FormatUint(newTotals[idx].Result.Confirmation, 10)
//	}
//
//	if newTotals[idx].Result.BlockNumber != newTotals[0].Result.BlockNumber {
//		newTotals[idx].Delta.BlockNumber = "ri" + strconv.FormatUint(newTotals[idx].Result.BlockNumber, 10)
//	}
//
//	if newTotals[idx].Result.Reason != newTotals[0].Result.Reason {
//		newTotals[idx].Delta.Reason = "rs" + newTotals[idx].Result.Reason
//	}
//
//	// Receipt
//	if newTotals[idx].Result.Receipt == nil {
//		newTotals[idx].Result.Receipt = &Receipt{}
//	}
//
//	if newTotals[idx].Result.Receipt.ContractAddress != newTotals[0].Result.Receipt.ContractAddress {
//		newTotals[idx].Delta.Receipt.ContractAddress = "rs" + newTotals[idx].Result.Receipt.ContractAddress.String()
//	}
//	if newTotals[idx].Result.Receipt.GasUsed != newTotals[0].Result.Receipt.GasUsed {
//		newTotals[idx].Delta.Receipt.GasUsed = "rs" + strconv.FormatUint(newTotals[idx].Result.Receipt.GasUsed, 10)
//	}
//
//	lenLogsidx := len(newTotals[idx].Result.Receipt.Logs)
//	lenLogs0 := len(newTotals[0].Result.Receipt.Logs)
//	newTotals[idx].Delta.Receipt = &DeltaReceipt{
//		Logs: map[int]*DeltaLog{},
//	}
//
//	for i := 0; i < max(lenLogs0, lenLogsidx); i++ {
//		newTotals[idx].Delta.Receipt.Logs[i] = &DeltaLog{
//			Topics: map[int]string{},
//		}
//
//		if i >= lenLogs0 {
//			newTotals[idx].Delta.Receipt.Logs[i].Address = "rs" + newTotals[idx].Result.Receipt.Logs[i].Address.String()
//			newTotals[idx].Delta.Receipt.Logs[i].Data = "rs" + base64.URLEncoding.EncodeToString(newTotals[idx].Result.Receipt.Logs[i].Data)
//			for j, topic := range newTotals[idx].Result.Receipt.Logs[i].Topics {
//				newTotals[idx].Delta.Receipt.Logs[i].Topics[j] = "rs" + topic.String()
//			}
//			continue
//		}
//
//		if i >= lenLogsidx {
//			newTotals[idx].Delta.Receipt.Logs[i] = &DeltaLog{}
//			continue
//		}
//
//		if newTotals[idx].Result.Receipt.Logs[i].Equal(newTotals[0].Result.Receipt.Logs[i]) {
//			delete(newTotals[idx].Delta.Receipt.Logs, i)
//			continue
//		}
//
//		if newTotals[idx].Result.Receipt.Logs[i].Address != newTotals[0].Result.Receipt.Logs[i].Address {
//			newTotals[idx].Delta.Receipt.Logs[i].Address = "rs" + newTotals[idx].Result.Receipt.Logs[i].Address.String()
//		}
//		if !bytes.Equal(newTotals[idx].Result.Receipt.Logs[i].Data, newTotals[0].Result.Receipt.Logs[i].Data) {
//			newTotals[idx].Delta.Receipt.Logs[i].Data = "rs" + base64.URLEncoding.EncodeToString(newTotals[idx].Result.Receipt.Logs[i].Data)
//		}
//
//		lenTopicsidx := len(newTotals[idx].Result.Receipt.Logs[i].Topics)
//		lenTopics0 := len(newTotals[0].Result.Receipt.Logs[i].Topics)
//
//		for j := 0; j< max(lenTopicsidx, lenTopics0); j++ {
//			if j >= lenTopics0 {
//				newTotals[idx].Delta.Receipt.Logs[i].Topics[j] = "rs" + newTotals[idx].Result.Receipt.Logs[i].Topics[j].String()
//				continue
//			}
//
//			if j >= lenTopicsidx {
//				newTotals[idx].Delta.Receipt.Logs[i].Topics[j] = "rs"
//				continue
//			}
//
//			if newTotals[idx].Result.Receipt.Logs[i].Topics[j] != newTotals[0].Result.Receipt.Logs[i].Topics[j] {
//				newTotals[idx].Delta.Receipt.Logs[i].Topics[j] = "rs" + newTotals[idx].Result.Receipt.Logs[i].Topics[j].String()
//			}
//		}
//	}
//
//	if newTotals[idx].Result.Receipt.Equal(newTotals[0].Result.Receipt) {
//		newTotals[idx].Delta.Receipt = nil
//	}
//	// Receipt
//
//	// Range
//	if newTotals[idx].Result.Range == nil {
//		newTotals[idx].Result.Range = &Range{}
//	}
//	newTotals[idx].Delta.Range = &DeltaRange{}
//
//	if newTotals[idx].Result.Range.Max != newTotals[0].Result.Range.Max {
//		newTotals[idx].Delta.Range.Max = "ri" + strconv.FormatUint(newTotals[idx].Result.Range.Max, 10)
//	}
//	if newTotals[idx].Result.Range.Min != newTotals[0].Result.Range.Min {
//		newTotals[idx].Delta.Range.Min = "ri" + strconv.FormatUint(newTotals[idx].Result.Range.Min, 10)
//	}
//	if newTotals[idx].Result.Range.GasPrice != newTotals[0].Result.Range.GasPrice {
//		newTotals[idx].Delta.Range.GasPrice = "ri" + strconv.FormatUint(newTotals[idx].Result.Range.GasPrice, 10)
//	}
//
//	if newTotals[idx].Result.Range.Equal(newTotals[0].Result.Range) {
//		newTotals[idx].Delta.Range = nil
//	}
//	// Range
//
//	newTotals[idx].Result = nil
//}

// 	t.Totals = newTotals
// }

// func (t *TotalResult) ToJSON() []byte {
// 	currentJSON, err := json.MarshalIndent(t, "", "      ")
// 	if err != nil {
// 		return []byte{}
// 	}
// 	return currentJSON
// }
