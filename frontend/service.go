// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .

// Package frontend implements the gRPC handlers called by the wallets.
package frontend

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zcash/lightwalletd/common"
	"github.com/zcash/lightwalletd/parser"
	"github.com/zcash/lightwalletd/walletrpc"
)

type lwdStreamer struct {
	cache      *common.BlockCache
	chainName  string
	pingEnable bool
	walletrpc.UnimplementedCompactTxStreamerServer
}

// NewLwdStreamer constructs a gRPC context.
func NewLwdStreamer(cache *common.BlockCache, chainName string, enablePing bool) (walletrpc.CompactTxStreamerServer, error) {
	return &lwdStreamer{cache: cache, chainName: chainName, pingEnable: enablePing}, nil
}

// DarksideStreamer holds the gRPC state for darksidewalletd.
type DarksideStreamer struct {
	cache *common.BlockCache
	walletrpc.UnimplementedDarksideStreamerServer
}

// NewDarksideStreamer constructs a gRPC context for darksidewalletd.
func NewDarksideStreamer(cache *common.BlockCache) (walletrpc.DarksideStreamerServer, error) {
	return &DarksideStreamer{cache: cache}, nil
}

// Test to make sure Address is a single t address
func checkTaddress(taddr string) error {
	match, err := regexp.Match("\\At[a-zA-Z0-9]{34}\\z", []byte(taddr))
	if err != nil || !match {
		return errors.New("Invalid address")
	}
	return nil
}

// GetLatestBlock returns the height of the best chain, according to zcashd.
func (s *lwdStreamer) GetLatestBlock(ctx context.Context, placeholder *walletrpc.ChainSpec) (*walletrpc.BlockID, error) {
	latestBlock := s.cache.GetLatestHeight()

	if latestBlock == -1 {
		return nil, errors.New("Cache is empty. Server is probably not yet ready")
	}

	// TODO: also return block hashes here
	return &walletrpc.BlockID{Height: uint64(latestBlock)}, nil
}

// GetTaddressTxids is a streaming RPC that returns transaction IDs that have
// the given transparent address (taddr) as either an input or output.
func (s *lwdStreamer) GetTaddressTxids(addressBlockFilter *walletrpc.TransparentAddressBlockFilter, resp walletrpc.CompactTxStreamer_GetTaddressTxidsServer) error {
	if err := checkTaddress(addressBlockFilter.Address); err != nil {
		return err
	}

	if addressBlockFilter.Range == nil {
		return errors.New("Must specify block range")
	}
	if addressBlockFilter.Range.Start == nil {
		return errors.New("Must specify a start block height")
	}
	if addressBlockFilter.Range.End == nil {
		return errors.New("Must specify an end block height")
	}
	params := make([]json.RawMessage, 1)
	request := &common.ZcashdRpcRequestGetaddresstxids{
		Addresses: []string{addressBlockFilter.Address},
		Start:     addressBlockFilter.Range.Start.Height,
		End:       addressBlockFilter.Range.End.Height,
	}
	param, err := json.Marshal(request)
	if err != nil {
		return err
	}
	params[0] = param
	result, rpcErr := common.RawRequest("getaddresstxids", params)

	// For some reason, the error responses are not JSON
	if rpcErr != nil {
		return rpcErr
	}

	var txids []string
	err = json.Unmarshal(result, &txids)
	if err != nil {
		return err
	}

	timeout, cancel := context.WithTimeout(resp.Context(), 30*time.Second)
	defer cancel()

	for _, txidstr := range txids {
		txid, _ := hex.DecodeString(txidstr)
		// Txid is read as a string, which is in big-endian order. But when converting
		// to bytes, it should be little-endian
		tx, err := s.GetTransaction(timeout, &walletrpc.TxFilter{Hash: parser.Reverse(txid)})
		if err != nil {
			return err
		}
		if err = resp.Send(tx); err != nil {
			return err
		}
	}
	return nil
}

// GetBlock returns the compact block at the requested height. Requesting a
// block by hash is not yet supported.
func (s *lwdStreamer) GetBlock(ctx context.Context, id *walletrpc.BlockID) (*walletrpc.CompactBlock, error) {
	if id.Height == 0 && id.Hash == nil {
		return nil, errors.New("request for unspecified identifier")
	}

	// Precedence: a hash is more specific than a height. If we have it, use it first.
	if id.Hash != nil {
		// TODO: Get block by hash
		return nil, errors.New("GetBlock by Hash is not yet implemented")
	}
	cBlock, err := common.GetBlock(s.cache, int(id.Height))

	if err != nil {
		return nil, err
	}

	return cBlock, err
}

// GetBlockRange is a streaming RPC that returns blocks, in compact form,
// (as also returned by GetBlock) from the block height 'start' to height
// 'end' inclusively.
func (s *lwdStreamer) GetBlockRange(span *walletrpc.BlockRange, resp walletrpc.CompactTxStreamer_GetBlockRangeServer) error {
	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error)
	if span.Start == nil || span.End == nil {
		return errors.New("Must specify start and end heights")
	}

	go common.GetBlockRange(s.cache, blockChan, errChan, int(span.Start.Height), int(span.End.Height))

	for {
		select {
		case err := <-errChan:
			// this will also catch context.DeadlineExceeded from the timeout
			return err
		case cBlock := <-blockChan:
			err := resp.Send(cBlock)
			if err != nil {
				return err
			}
		}
	}
}

func (s *lwdStreamer) GetZECPrice(ctx context.Context, in *walletrpc.PriceRequest) (*walletrpc.PriceResponse, error) {
	// Check for prices before zcash was born
	if in == nil || in.Timestamp <= 1477551600 /* Zcash birthday: 2016-10-28*/ {
		return nil, errors.New("incorrect Timestamp")
	}

	if in.Currency != "USD" {
		return nil, errors.New("unsupported currency")
	}

	ts := time.Unix(int64(in.Timestamp), 0)
	price, timeFetched, err := common.GetHistoricalPrice(&ts)

	if err != nil {
		return nil, err
	}

	return &walletrpc.PriceResponse{Timestamp: timeFetched.Unix(), Price: price, Currency: "USD"}, nil
}

func (s *lwdStreamer) GetCurrentZECPrice(ctx context.Context, in *walletrpc.Empty) (*walletrpc.PriceResponse, error) {
	price, err := common.GetCurrentPrice()
	if err != nil {
		return nil, err
	}

	if price <= 0 {
		return nil, errors.New("no price available")
	}

	resp := &walletrpc.PriceResponse{Timestamp: time.Now().Unix(), Currency: "USD", Price: price}
	return resp, nil
}

// GetTreeState returns the note commitment tree state corresponding to the given block.
// See section 3.7 of the Zcash protocol specification. It returns several other useful
// values also (even though they can be obtained using GetBlock).
// The block can be specified by either height or hash.
func (s *lwdStreamer) GetTreeState(ctx context.Context, id *walletrpc.BlockID) (*walletrpc.TreeState, error) {
	if id.Height == 0 && id.Hash == nil {
		return nil, errors.New("request for unspecified identifier")
	}
	// The Zcash z_gettreestate rpc accepts either a block height or block hash
	params := make([]json.RawMessage, 1)
	var hashJSON []byte
	if id.Height > 0 {
		heightJSON, err := json.Marshal(strconv.Itoa(int(id.Height)))
		if err != nil {
			return nil, err
		}
		params[0] = heightJSON
	} else {
		// id.Hash is big-endian, keep in big-endian for the rpc
		hashJSON, err := json.Marshal(hex.EncodeToString(id.Hash))
		if err != nil {
			return nil, err
		}
		params[0] = hashJSON
	}
	var gettreestateReply common.ZcashdRpcReplyGettreestate
	for {
		result, rpcErr := common.RawRequest("z_gettreestate", params)
		if rpcErr != nil {
			return nil, rpcErr
		}
		err := json.Unmarshal(result, &gettreestateReply)
		if err != nil {
			return nil, err
		}
		if gettreestateReply.Sapling.Commitments.FinalState != "" {
			break
		}
		if gettreestateReply.Sapling.SkipHash == "" {
			break
		}
		hashJSON, err = json.Marshal(gettreestateReply.Sapling.SkipHash)
		if err != nil {
			return nil, err
		}
		params[0] = hashJSON
	}
	if gettreestateReply.Sapling.Commitments.FinalState == "" {
		return nil, errors.New("zcashd did not return treestate")
	}
	return &walletrpc.TreeState{
		Network: s.chainName,
		Height:  uint64(gettreestateReply.Height),
		Hash:    gettreestateReply.Hash,
		Time:    gettreestateReply.Time,
		Tree:    gettreestateReply.Sapling.Commitments.FinalState,
	}, nil
}

// GetTransaction returns the raw transaction bytes that are returned
// by the zcashd 'getrawtransaction' RPC.
func (s *lwdStreamer) GetTransaction(ctx context.Context, txf *walletrpc.TxFilter) (*walletrpc.RawTransaction, error) {
	if txf.Hash != nil {
		if len(txf.Hash) != 32 {
			return nil, errors.New("Transaction ID has invalid length")
		}
		leHashStringJSON, err := json.Marshal(hex.EncodeToString(parser.Reverse(txf.Hash)))
		if err != nil {
			return nil, err
		}
		params := []json.RawMessage{
			leHashStringJSON,
			json.RawMessage("1"),
		}
		result, rpcErr := common.RawRequest("getrawtransaction", params)

		// For some reason, the error responses are not JSON
		if rpcErr != nil {
			return nil, rpcErr
		}
		// Many other fields are returned, but we need only these two.
		var txinfo common.ZcashdRpcReplyGetrawtransaction
		err = json.Unmarshal(result, &txinfo)
		if err != nil {
			return nil, err
		}
		txBytes, err := hex.DecodeString(txinfo.Hex)
		if err != nil {
			return nil, err
		}
		return &walletrpc.RawTransaction{
			Data:   txBytes,
			Height: uint64(txinfo.Height),
		}, nil
	}

	if txf.Block != nil && txf.Block.Hash != nil {
		return nil, errors.New("Can't GetTransaction with a blockhash+num. Please call GetTransaction with txid")
	}
	return nil, errors.New("Please call GetTransaction with txid")
}

// GetLightdInfo gets the LightWalletD (this server) info, and includes information
// it gets from its backend zcashd.
func (s *lwdStreamer) GetLightdInfo(ctx context.Context, in *walletrpc.Empty) (*walletrpc.LightdInfo, error) {
	return common.GetLightdInfo()
}

// SendTransaction forwards raw transaction bytes to a zcashd instance over JSON-RPC
func (s *lwdStreamer) SendTransaction(ctx context.Context, rawtx *walletrpc.RawTransaction) (*walletrpc.SendResponse, error) {
	// sendrawtransaction "hexstring" ( allowhighfees )
	//
	// Submits raw transaction (binary) to local node and network.
	//
	// Result:
	// "hex"             (string) The transaction hash in hex

	// Construct raw JSON-RPC params
	params := make([]json.RawMessage, 1)
	txJSON, err := json.Marshal(hex.EncodeToString(rawtx.Data))
	if err != nil {
		return &walletrpc.SendResponse{}, err
	}
	params[0] = txJSON
	result, rpcErr := common.RawRequest("sendrawtransaction", params)

	var errCode int64
	var errMsg string

	// For some reason, the error responses are not JSON
	if rpcErr != nil {
		errParts := strings.SplitN(rpcErr.Error(), ":", 2)
		if len(errParts) < 2 {
			return nil, errors.New("SendTransaction couldn't parse error code")
		}
		errMsg = strings.TrimSpace(errParts[1])
		errCode, err = strconv.ParseInt(errParts[0], 10, 32)
		if err != nil {
			// This should never happen. We can't panic here, but it's that class of error.
			// This is why we need integration testing to work better than regtest currently does. TODO.
			return nil, errors.New("SendTransaction couldn't parse error code")
		}
	} else {
		errMsg = string(result)
	}

	// TODO these are called Error but they aren't at the moment.
	// A success will return code 0 and message txhash.
	return &walletrpc.SendResponse{
		ErrorCode:    int32(errCode),
		ErrorMessage: errMsg,
	}, nil
}

func getTaddressBalanceZcashdRpc(addressList []string) (*walletrpc.Balance, error) {
	for _, addr := range addressList {
		if err := checkTaddress(addr); err != nil {
			return &walletrpc.Balance{}, err
		}
	}
	params := make([]json.RawMessage, 1)
	addrList := &common.ZcashdRpcRequestGetaddressbalance{
		Addresses: addressList,
	}
	param, err := json.Marshal(addrList)
	if err != nil {
		return &walletrpc.Balance{}, err
	}
	params[0] = param

	result, rpcErr := common.RawRequest("getaddressbalance", params)
	if rpcErr != nil {
		return &walletrpc.Balance{}, rpcErr
	}
	var balanceReply common.ZcashdRpcReplyGetaddressbalance
	err = json.Unmarshal(result, &balanceReply)
	if err != nil {
		return &walletrpc.Balance{}, err
	}
	return &walletrpc.Balance{ValueZat: balanceReply.Balance}, nil
}

// GetTaddressBalance returns the total balance for a list of taddrs
func (s *lwdStreamer) GetTaddressBalance(ctx context.Context, addresses *walletrpc.AddressList) (*walletrpc.Balance, error) {
	return getTaddressBalanceZcashdRpc(addresses.Addresses)
}

// GetTaddressBalanceStream returns the total balance for a list of taddrs
func (s *lwdStreamer) GetTaddressBalanceStream(addresses walletrpc.CompactTxStreamer_GetTaddressBalanceStreamServer) error {
	addressList := make([]string, 0)
	for {
		addr, err := addresses.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		addressList = append(addressList, addr.Address)
	}
	balance, err := getTaddressBalanceZcashdRpc(addressList)
	if err != nil {
		return err
	}
	addresses.SendAndClose(balance)
	return nil
}

// Key is 32-byte txid (as a 64-character string), data is pointer to compact tx.
var mempoolMap *map[string]*walletrpc.CompactTx
var mempoolList []string

// Last time we pulled a copy of the mempool from zcashd.
var lastMempool time.Time

func (s *lwdStreamer) GetMempoolTx(exclude *walletrpc.Exclude, resp walletrpc.CompactTxStreamer_GetMempoolTxServer) error {
	if time.Now().Sub(lastMempool).Seconds() >= 2 {
		lastMempool = time.Now()
		// Refresh our copy of the mempool.
		params := make([]json.RawMessage, 0)
		result, rpcErr := common.RawRequest("getrawmempool", params)
		if rpcErr != nil {
			return rpcErr
		}
		err := json.Unmarshal(result, &mempoolList)
		if err != nil {
			return err
		}
		newmempoolMap := make(map[string]*walletrpc.CompactTx)
		if mempoolMap == nil {
			mempoolMap = &newmempoolMap
		}
		for _, txidstr := range mempoolList {
			if ctx, ok := (*mempoolMap)[txidstr]; ok {
				// This ctx has already been fetched, copy pointer to it.
				newmempoolMap[txidstr] = ctx
				continue
			}
			txidJSON, err := json.Marshal(txidstr)
			if err != nil {
				return err
			}
			// The "0" is because we only need the raw hex, which is returned as
			// just a hex string, and not even a json string (with quotes).
			params := []json.RawMessage{txidJSON, json.RawMessage("0")}
			result, rpcErr := common.RawRequest("getrawtransaction", params)
			if rpcErr != nil {
				// Not an error; mempool transactions can disappear
				continue
			}
			// strip the quotes
			var txStr string
			err = json.Unmarshal(result, &txStr)
			if err != nil {
				return err
			}

			// conver to binary
			txBytes, err := hex.DecodeString(txStr)
			if err != nil {
				return err
			}
			tx := parser.NewTransaction()
			txdata, err := tx.ParseFromSlice(txBytes)
			if len(txdata) > 0 {
				return errors.New("extra data deserializing transaction")
			}
			newmempoolMap[txidstr] = &walletrpc.CompactTx{}
			if tx.HasSaplingElements() {
				newmempoolMap[txidstr] = tx.ToCompact( /* height */ 0)
			}
		}
		mempoolMap = &newmempoolMap
	}
	excludeHex := make([]string, len(exclude.Txid))
	for i := 0; i < len(exclude.Txid); i++ {
		excludeHex[i] = hex.EncodeToString(parser.Reverse(exclude.Txid[i]))
	}
	for _, txid := range MempoolFilter(mempoolList, excludeHex) {
		tx := (*mempoolMap)[txid]
		if len(tx.Hash) > 0 {
			err := resp.Send(tx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Return the subset of items that aren't excluded, but
// if more than one item matches an exclude entry, return
// all those items.
func MempoolFilter(items, exclude []string) []string {
	sort.Slice(items, func(i, j int) bool {
		return items[i] < items[j]
	})
	sort.Slice(exclude, func(i, j int) bool {
		return exclude[i] < exclude[j]
	})
	// Determine how many items match each exclude item.
	nmatches := make([]int, len(exclude))
	// is the exclude string less than the item string?
	lessthan := func(e, i string) bool {
		l := len(e)
		if l > len(i) {
			l = len(i)
		}
		return e < i[0:l]
	}
	ei := 0
	for _, item := range items {
		for ei < len(exclude) && lessthan(exclude[ei], item) {
			ei++
		}
		match := ei < len(exclude) && strings.HasPrefix(item, exclude[ei])
		if match {
			nmatches[ei]++
		}
	}

	// Add each item that isn't uniquely excluded to the results.
	tosend := make([]string, 0)
	ei = 0
	for _, item := range items {
		for ei < len(exclude) && lessthan(exclude[ei], item) {
			ei++
		}
		match := ei < len(exclude) && strings.HasPrefix(item, exclude[ei])
		if !match || nmatches[ei] > 1 {
			tosend = append(tosend, item)
		}
	}
	return tosend
}

func getAddressUtxos(arg *walletrpc.GetAddressUtxosArg, f func(*walletrpc.GetAddressUtxosReply) error) error {
	for _, a := range arg.Addresses {
		if err := checkTaddress(a); err != nil {
			return err
		}
	}
	params := make([]json.RawMessage, 1)
	addrList := &common.ZcashdRpcRequestGetaddressutxos{
		Addresses: arg.Addresses,
	}
	param, err := json.Marshal(addrList)
	if err != nil {
		return err
	}
	params[0] = param
	result, rpcErr := common.RawRequest("getaddressutxos", params)
	if rpcErr != nil {
		return rpcErr
	}
	var utxosReply []common.ZcashdRpcReplyGetaddressutxos
	err = json.Unmarshal(result, &utxosReply)
	if err != nil {
		return err
	}
	n := 0
	for _, utxo := range utxosReply {
		if uint64(utxo.Height) < arg.StartHeight {
			continue
		}
		n++
		if arg.MaxEntries > 0 && uint32(n) > arg.MaxEntries {
			break
		}
		txidBytes, err := hex.DecodeString(utxo.Txid)
		if err != nil {
			return err
		}
		scriptBytes, err := hex.DecodeString(utxo.Script)
		if err != nil {
			return err
		}
		err = f(&walletrpc.GetAddressUtxosReply{
			Address:  utxo.Address,
			Txid:     parser.Reverse(txidBytes),
			Index:    int32(utxo.OutputIndex),
			Script:   scriptBytes,
			ValueZat: int64(utxo.Satoshis),
			Height:   uint64(utxo.Height),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *lwdStreamer) GetAddressUtxos(ctx context.Context, arg *walletrpc.GetAddressUtxosArg) (*walletrpc.GetAddressUtxosReplyList, error) {
	addressUtxos := make([]*walletrpc.GetAddressUtxosReply, 0)
	err := getAddressUtxos(arg, func(utxo *walletrpc.GetAddressUtxosReply) error {
		addressUtxos = append(addressUtxos, utxo)
		return nil
	})
	if err != nil {
		return &walletrpc.GetAddressUtxosReplyList{}, err
	}
	return &walletrpc.GetAddressUtxosReplyList{AddressUtxos: addressUtxos}, nil
}

func (s *lwdStreamer) GetAddressUtxosStream(arg *walletrpc.GetAddressUtxosArg, resp walletrpc.CompactTxStreamer_GetAddressUtxosStreamServer) error {
	err := getAddressUtxos(arg, func(utxo *walletrpc.GetAddressUtxosReply) error {
		return resp.Send(utxo)
	})
	if err != nil {
		return err
	}
	return nil
}

// This rpc is used only for testing.
var concurrent int64

func (s *lwdStreamer) Ping(ctx context.Context, in *walletrpc.Duration) (*walletrpc.PingResponse, error) {
	// This gRPC allows the client to create an arbitrary number of
	// concurrent threads, which could run the server out of resources,
	// so only allow if explicitly enabled.
	if !s.pingEnable {
		return nil, errors.New("Ping not enabled, start lightwalletd with --ping-very-insecure")
	}
	var response walletrpc.PingResponse
	response.Entry = atomic.AddInt64(&concurrent, 1)
	time.Sleep(time.Duration(in.IntervalUs) * time.Microsecond)
	response.Exit = atomic.AddInt64(&concurrent, -1)
	return &response, nil
}

// SetMetaState lets the test driver control some GetLightdInfo values.
func (s *DarksideStreamer) Reset(ctx context.Context, ms *walletrpc.DarksideMetaState) (*walletrpc.Empty, error) {
	match, err := regexp.Match("\\A[a-fA-F0-9]+\\z", []byte(ms.BranchID))
	if err != nil || !match {
		return nil, errors.New("Invalid branch ID")
	}

	match, err = regexp.Match("\\A[a-zA-Z0-9]+\\z", []byte(ms.ChainName))
	if err != nil || !match {
		return nil, errors.New("Invalid chain name")
	}
	err = common.DarksideReset(int(ms.SaplingActivation), ms.BranchID, ms.ChainName)
	if err != nil {
		return nil, err
	}
	mempoolMap = nil
	mempoolList = nil
	return &walletrpc.Empty{}, nil
}

// StageBlocksStream accepts a list of blocks from the wallet test code,
// and makes them available to present from the mock zcashd's GetBlock rpc.
func (s *DarksideStreamer) StageBlocksStream(blocks walletrpc.DarksideStreamer_StageBlocksStreamServer) error {
	for {
		b, err := blocks.Recv()
		if err == io.EOF {
			blocks.SendAndClose(&walletrpc.Empty{})
			return nil
		}
		if err != nil {
			return err
		}
		common.DarksideStageBlockStream(b.Block)
	}
}

// StageBlocks loads blocks from the given URL to the staging area.
func (s *DarksideStreamer) StageBlocks(ctx context.Context, u *walletrpc.DarksideBlocksURL) (*walletrpc.Empty, error) {
	if err := common.DarksideStageBlocks(u.Url); err != nil {
		return nil, err
	}
	return &walletrpc.Empty{}, nil
}

// StageBlocksCreate stages a set of synthetic (manufactured on the fly) blocks.
func (s *DarksideStreamer) StageBlocksCreate(ctx context.Context, e *walletrpc.DarksideEmptyBlocks) (*walletrpc.Empty, error) {
	if err := common.DarksideStageBlocksCreate(e.Height, e.Nonce, e.Count); err != nil {
		return nil, err
	}
	return &walletrpc.Empty{}, nil
}

// StageTransactionsStream adds the given transactions to the staging area.
func (s *DarksideStreamer) StageTransactionsStream(tx walletrpc.DarksideStreamer_StageTransactionsStreamServer) error {
	// My current thinking is that this should take a JSON array of {height, txid}, store them,
	// then DarksideAddBlock() would "inject" transactions into blocks as its storing
	// them (remembering to update the header so the block hash changes).
	for {
		transaction, err := tx.Recv()
		if err == io.EOF {
			tx.SendAndClose(&walletrpc.Empty{})
			return nil
		}
		if err != nil {
			return err
		}
		err = common.DarksideStageTransaction(int(transaction.Height), transaction.Data)
		if err != nil {
			return err
		}
	}
}

// StageTransactions loads blocks from the given URL to the staging area.
func (s *DarksideStreamer) StageTransactions(ctx context.Context, u *walletrpc.DarksideTransactionsURL) (*walletrpc.Empty, error) {
	if err := common.DarksideStageTransactionsURL(int(u.Height), u.Url); err != nil {
		return nil, err
	}
	return &walletrpc.Empty{}, nil
}

// ApplyStaged merges all staged transactions into staged blocks and all staged blocks into the active blockchain.
func (s *DarksideStreamer) ApplyStaged(ctx context.Context, h *walletrpc.DarksideHeight) (*walletrpc.Empty, error) {
	return &walletrpc.Empty{}, common.DarksideApplyStaged(int(h.Height))
}

// GetIncomingTransactions returns the transactions that were submitted via SendTransaction().
func (s *DarksideStreamer) GetIncomingTransactions(in *walletrpc.Empty, resp walletrpc.DarksideStreamer_GetIncomingTransactionsServer) error {
	// Get all of the incoming transactions we're received via SendTransaction()
	for _, txBytes := range common.DarksideGetIncomingTransactions() {
		err := resp.Send(&walletrpc.RawTransaction{Data: txBytes, Height: 0})
		if err != nil {
			return err
		}
	}
	return nil
}

// ClearIncomingTransactions empties the incoming transaction list.
func (s *DarksideStreamer) ClearIncomingTransactions(ctx context.Context, e *walletrpc.Empty) (*walletrpc.Empty, error) {
	common.DarksideClearIncomingTransactions()
	return &walletrpc.Empty{}, nil
}

// AddAddressUtxo adds a UTXO which will be returned by GetAddressUtxos() (above)
func (s *DarksideStreamer) AddAddressUtxo(ctx context.Context, arg *walletrpc.GetAddressUtxosReply) (*walletrpc.Empty, error) {
	utxosReply := common.ZcashdRpcReplyGetaddressutxos{
		Address:     arg.Address,
		Txid:        hex.EncodeToString(parser.Reverse(arg.Txid)),
		OutputIndex: int64(arg.Index),
		Script:      hex.EncodeToString(arg.Script),
		Satoshis:    uint64(arg.ValueZat),
		Height:      int(arg.Height),
	}
	err := common.DarksideAddAddressUtxo(utxosReply)
	return &walletrpc.Empty{}, err
}

// ClearAddressUtxo removes the list of cached utxo entries
func (s *DarksideStreamer) ClearAddressUtxo(ctx context.Context, arg *walletrpc.Empty) (*walletrpc.Empty, error) {
	err := common.DarksideClearAddressUtxos()
	return &walletrpc.Empty{}, err
}
