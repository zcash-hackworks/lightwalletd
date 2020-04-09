// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .
package frontend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/zcash/lightwalletd/common"
	"github.com/zcash/lightwalletd/walletrpc"
)

var (
	testT  *testing.T
	logger = logrus.New()
	step   int

	blocks    [][]byte // four test blocks
	rawTxData [][]byte
)

const (
	unitTestPath  = "unittestcache"
	unitTestChain = "unittestnet"
)

func testsetup() (walletrpc.CompactTxStreamerServer, *common.BlockCache) {
	os.RemoveAll(unitTestPath)
	cache := common.NewBlockCache(unitTestPath, unitTestChain, 380640, true)
	lwd, err := NewLwdStreamer(cache)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprint("NewLwdStreamer failed:", err))
		os.Exit(1)
	}
	return lwd, cache
}

func TestMain(m *testing.M) {
	output, err := os.OpenFile("test-log", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprint("Cannot open test-log:", err))
		os.Exit(1)
	}
	logger.SetOutput(output)
	common.Log = logger.WithFields(logrus.Fields{
		"app": "test",
	})

	// Several tests need test blocks; read all 4 into memory just once
	// (for efficiency).
	testBlocks, err := os.Open("../testdata/blocks")
	if err != nil {
		os.Stderr.WriteString(fmt.Sprint("Error:", err))
		os.Exit(1)
	}
	defer testBlocks.Close()
	scan := bufio.NewScanner(testBlocks)
	for scan.Scan() { // each line (block)
		block := scan.Bytes()
		// Enclose the hex string in quotes (to make it json, to match what's
		// returned by the RPC)
		block = []byte("\"" + string(block) + "\"")
		blocks = append(blocks, block)
	}

	testData, err := os.Open("../testdata/zip243_raw_tx")
	if err != nil {
		os.Stderr.WriteString(fmt.Sprint("Error:", err))
		os.Exit(1)
	}
	defer testData.Close()

	// Parse the raw transactions file
	rawTxData = [][]byte{}
	scan = bufio.NewScanner(testData)
	for scan.Scan() {
		dataLine := scan.Text()
		// Skip the comments
		if strings.HasPrefix(dataLine, "#") {
			continue
		}

		txData, err := hex.DecodeString(dataLine)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprint("Error:", err))
			os.Exit(1)
		}

		rawTxData = append(rawTxData, txData)
	}

	// Setup is done; run all tests.
	exitcode := m.Run()

	// cleanup
	os.Remove("test-log")
	os.RemoveAll(unitTestPath)

	os.Exit(exitcode)
}

func TestGetTransaction(t *testing.T) {
	// GetTransaction() will mostly be tested below via TestGetAddressTxids
	lwd, _ := testsetup()

	rawtx, err := lwd.GetTransaction(context.Background(),
		&walletrpc.TxFilter{})
	if err == nil {
		testT.Fatal("GetTransaction unexpectedly succeeded")
	}
	if err.Error() != "Please call GetTransaction with txid" {
		testT.Fatal("GetTransaction unexpected error message")
	}
	if rawtx != nil {
		testT.Fatal("GetTransaction non-nil rawtx returned")
	}

	rawtx, err = lwd.GetTransaction(context.Background(),
		&walletrpc.TxFilter{Block: &walletrpc.BlockID{Hash: []byte{}}})
	if err == nil {
		testT.Fatal("GetTransaction unexpectedly succeeded")
	}
	if err.Error() != "Can't GetTransaction with a blockhash+num. Please call GetTransaction with txid" {
		testT.Fatal("GetTransaction unexpected error message")
	}
	if rawtx != nil {
		testT.Fatal("GetTransaction non-nil rawtx returned")
	}
}

func getblockStub(method string, params []json.RawMessage) (json.RawMessage, error) {
	step++
	var height string
	err := json.Unmarshal(params[0], &height)
	if err != nil {
		testT.Fatal("could not unmarshal height")
	}
	if height != "380640" {
		testT.Fatal("unexpected getblock height", height)
	}

	// Test retry logic (for the moment, it's very simple, just one retry).
	switch step {
	case 1:
		return blocks[0], nil
	case 2:
		return nil, errors.New("getblock test error")
	}
	testT.Fatal("unexpected call to getblockStub")
	return nil, nil
}

func TestGetLatestBlock(t *testing.T) {
	testT = t
	common.RawRequest = getblockStub
	lwd, cache := testsetup()

	// This argument is not used (it may be in the future)
	req := &walletrpc.ChainSpec{}

	blockID, err := lwd.GetLatestBlock(context.Background(), req)
	if err == nil {
		t.Fatal("GetLatestBlock should have failed, empty cache")
	}
	if err.Error() != "Cache is empty. Server is probably not yet ready" {
		t.Fatal("GetLatestBlock incorrect error", err)
	}
	if blockID != nil {
		t.Fatal("unexpected blockID", blockID)
	}

	// This does zcashd rpc "getblock", calls getblockStub() above
	block, err := common.GetBlock(cache, 380640)
	if err != nil {
		t.Fatal("getBlockFromRPC failed", err)
	}
	if err = cache.Add(380640, block); err != nil {
		t.Fatal("cache.Add failed:", err)
	}
	blockID, err = lwd.GetLatestBlock(context.Background(), req)
	if err != nil {
		t.Fatal("lwd.GetLatestBlock failed", err)
	}
	if blockID.Height != 380640 {
		t.Fatal("unexpected blockID.height")
	}
	step = 0
}

// A valid address starts with "t", followed by 34 alpha characters;
// these should all be detected as invalid.
var addressTests = []string{
	"",                                      // too short
	"a",                                     // too short
	"t123456789012345678901234567890123",    // one byte too short
	"t12345678901234567890123456789012345",  // one byte too long
	"t123456789012345678901234567890123*",   // invalid "*"
	"s1234567890123456789012345678901234",   // doesn't start with "t"
	" t1234567890123456789012345678901234",  // extra stuff before
	"t1234567890123456789012345678901234 ",  // extra stuff after
	"\nt1234567890123456789012345678901234", // newline before
	"t1234567890123456789012345678901234\n", // newline after
}

func zcashdrpcStub(method string, params []json.RawMessage) (json.RawMessage, error) {
	step++
	switch method {
	case "getaddresstxids":
		var filter struct {
			Addresses []string `json: addresses`
			Start     float64  `json: start`
			End       float64  `json: end`
		}
		err := json.Unmarshal(params[0], &filter)
		if err != nil {
			testT.Fatal("could not unmarshal block filter")
		}
		if len(filter.Addresses) != 1 {
			testT.Fatal("wrong number of addresses")
		}
		if filter.Addresses[0] != "t1234567890123456789012345678901234" {
			testT.Fatal("wrong address")
		}
		if filter.Start != 20 {
			testT.Fatal("wrong start")
		}
		if filter.End != 30 {
			testT.Fatal("wrong end")
		}
		return []byte("[\"6732cf8d67aac5b82a2a0f0217a7d4aa245b2adb0b97fd2d923dfc674415e221\"]"), nil
	case "getrawtransaction":
		switch step {
		case 2:
			txstr := hex.EncodeToString(rawTxData[0])
			return []byte("{\"hex\": \"" + txstr + "\", \"height\": 1234567}"), nil
		case 4:
			// empty return value, should be okay
			return []byte(""), errors.New("-5: test getrawtransaction error")
		}
	}
	testT.Fatal("unexpected call to zcashdrpcStub")
	return nil, nil
}

type testgettx struct {
	walletrpc.CompactTxStreamer_GetAddressTxidsServer
}

func (tg *testgettx) Context() context.Context {
	return context.Background()
}

func (tg *testgettx) Send(tx *walletrpc.RawTransaction) error {
	if !bytes.Equal(tx.Data, []byte(hex.EncodeToString(rawTxData[0]))) {
		testT.Fatal("mismatch transaction data")
	}
	if tx.Height != 1234567 {
		testT.Fatal("unexpected transaction height", tx.Height)
	}
	return nil
}

func TestGetAddressTxids(t *testing.T) {
	testT = t
	common.RawRequest = zcashdrpcStub
	lwd, _ := testsetup()

	addressBlockFilter := &walletrpc.TransparentAddressBlockFilter{
		Range: &walletrpc.BlockRange{
			Start: &walletrpc.BlockID{Height: 20},
			End:   &walletrpc.BlockID{Height: 30},
		},
	}

	// Ensure that a bad address is detected
	for i, addressTest := range addressTests {
		addressBlockFilter.Address = addressTest
		err := lwd.GetAddressTxids(addressBlockFilter, &testgettx{})
		if err == nil {
			t.Fatal("GetAddressTxids should have failed on bad address, case", i)
		}
		if err.Error() != "Invalid address" {
			t.Fatal("GetAddressTxids incorrect error on bad address, case", i)
		}
	}

	// valid address
	addressBlockFilter.Address = "t1234567890123456789012345678901234"
	err := lwd.GetAddressTxids(addressBlockFilter, &testgettx{})
	if err != nil {
		t.Fatal("GetAddressTxids failed", err)
	}

	// this time GetTransaction() will return an error
	err = lwd.GetAddressTxids(addressBlockFilter, &testgettx{})
	if err == nil {
		t.Fatal("GetAddressTxids succeeded")
	}
	step = 0
}

func TestGetBlock(t *testing.T) {
	testT = t
	common.RawRequest = getblockStub
	lwd, _ := testsetup()

	_, err := lwd.GetBlock(context.Background(), &walletrpc.BlockID{})
	if err == nil {
		t.Fatal("GetBlock should have failed")
	}
	_, err = lwd.GetBlock(context.Background(), &walletrpc.BlockID{Height: 0})
	if err == nil {
		t.Fatal("GetBlock should have failed")
	}
	_, err = lwd.GetBlock(context.Background(), &walletrpc.BlockID{Hash: []byte{0}})
	if err == nil {
		t.Fatal("GetBlock should have failed")
	}
	if err.Error() != "GetBlock by Hash is not yet implemented" {
		t.Fatal("GetBlock hash unimplemented error message failed")
	}

	// getblockStub() case 1: return error
	block, err := lwd.GetBlock(context.Background(), &walletrpc.BlockID{Height: 380640})
	if err != nil {
		t.Fatal("GetBlock failed:", err)
	}
	if block.Height != 380640 {
		t.Fatal("GetBlock returned unexpected block:", err)
	}
	// getblockStub() case 2: return error
	block, err = lwd.GetBlock(context.Background(), &walletrpc.BlockID{Height: 380640})
	if err == nil {
		t.Fatal("GetBlock should have failed")
	}
	if block != nil {
		t.Fatal("GetBlock returned unexpected non-nil block")
	}
	step = 0
}

type testgetbrange struct {
	walletrpc.CompactTxStreamer_GetAddressTxidsServer
}

func (tg *testgetbrange) Context() context.Context {
	return context.Background()
}

func (tg *testgetbrange) Send(cb *walletrpc.CompactBlock) error {
	return nil
}

func TestGetBlockRange(t *testing.T) {
	testT = t
	common.RawRequest = getblockStub
	common.RawRequest = getblockStub
	lwd, _ := testsetup()

	blockrange := &walletrpc.BlockRange{
		Start: &walletrpc.BlockID{Height: 380640},
		End:   &walletrpc.BlockID{Height: 380640},
	}
	// getblockStub() case 1 (success)
	err := lwd.GetBlockRange(blockrange, &testgetbrange{})
	if err != nil {
		t.Fatal("GetBlockRange failed", err)
	}
	// getblockStub() case 2 (failure)
	err = lwd.GetBlockRange(blockrange, &testgetbrange{})
	if err == nil {
		t.Fatal("GetBlockRange should have failed")
	}
	step = 0
}

func getblockchaininfoStub(method string, params []json.RawMessage) (json.RawMessage, error) {
	getsaplinginfo, _ := ioutil.ReadFile("../testdata/getsaplinginfo")
	getblockchaininfoReply, _ := hex.DecodeString(string(getsaplinginfo))
	return getblockchaininfoReply, nil
}

func TestGetLightdInfo(t *testing.T) {
	testT = t
	common.RawRequest = getblockchaininfoStub
	lwd, _ := testsetup()

	ldinfo, err := lwd.GetLightdInfo(context.Background(), &walletrpc.Empty{})
	if err != nil {
		t.Fatal("GetLightdInfo failed", err)
	}
	if ldinfo.Vendor != "ECC LightWalletD" {
		t.Fatal("GetLightdInfo: unexpected vendor", ldinfo)
	}
	step = 0
}

func sendrawtransactionStub(method string, params []json.RawMessage) (json.RawMessage, error) {
	step++
	if method != "sendrawtransaction" {
		testT.Fatal("unexpected method")
	}
	if string(params[0]) != "\"07\"" {
		testT.Fatal("unexpected tx data")
	}
	switch step {
	case 1:
		return []byte("sendtxresult"), nil
	case 2:
		return nil, errors.New("-17: some error")
	}
	testT.Fatal("unexpected call to sendrawtransactionStub")
	return nil, nil
}

func TestSendTransaction(t *testing.T) {
	testT = t
	lwd, _ := testsetup()
	common.RawRequest = sendrawtransactionStub
	rawtx := walletrpc.RawTransaction{Data: []byte{7}}
	sendresult, err := lwd.SendTransaction(context.Background(), &rawtx)
	if err != nil {
		t.Fatal("SendTransaction failed", err)
	}
	if sendresult.ErrorCode != 0 {
		t.Fatal("SendTransaction unexpected ErrorCode return")
	}
	if sendresult.ErrorMessage != "sendtxresult" {
		t.Fatal("SendTransaction unexpected ErrorMessage return")
	}

	// sendrawtransactionStub case 2 (error)
	// but note that the error is send within the response
	sendresult, err = lwd.SendTransaction(context.Background(), &rawtx)
	if err != nil {
		t.Fatal("SendTransaction failed:", err)
	}
	if sendresult.ErrorCode != -17 {
		t.Fatal("SendTransaction unexpected ErrorCode return")
	}
	if sendresult.ErrorMessage != "some error" {
		t.Fatal("SendTransaction unexpected ErrorMessage return")
	}
	step = 0
}

var sampleconf = `
testnet = 1
rpcport = 18232
rpcbind = 127.0.0.1
rpcuser = testlightwduser
rpcpassword = testlightwdpassword
`

func TestNewZRPCFromConf(t *testing.T) {
	connCfg, err := connFromConf([]byte(sampleconf))
	if err != nil {
		t.Fatal("connFromConf failed")
	}
	if connCfg.Host != "127.0.0.1:18232" {
		t.Fatal("connFromConf returned unexpected Host")
	}
	if connCfg.User != "testlightwduser" {
		t.Fatal("connFromConf returned unexpected User")
	}
	if connCfg.Pass != "testlightwdpassword" {
		t.Fatal("connFromConf returned unexpected User")
	}
	if !connCfg.HTTPPostMode {
		t.Fatal("connFromConf returned unexpected HTTPPostMode")
	}
	if !connCfg.DisableTLS {
		t.Fatal("connFromConf returned unexpected DisableTLS")
	}

	// can't pass an integer
	connCfg, err = connFromConf(10)
	if err == nil {
		t.Fatal("connFromConf unexpected success")
	}

	// Can't verify returned values, but at least run it
	_, err = NewZRPCFromConf([]byte(sampleconf))
	if err != nil {
		t.Fatal("NewZRPCFromClient failed")
	}
	_, err = NewZRPCFromConf(10)
	if err == nil {
		t.Fatal("NewZRPCFromClient unexpected success")
	}
}
