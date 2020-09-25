// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .

package common

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/zcash/lightwalletd/parser"
	"github.com/zcash/lightwalletd/walletrpc"
)

// 'make build' will overwrite this string with the output of git-describe (tag)
var (
	Version   = "v0.0.0.0-dev"
	GitCommit = ""
	BuildDate = ""
	BuildUser = ""
)

type Options struct {
	GRPCBindAddr        string `json:"grpc_bind_address,omitempty"`
	HTTPBindAddr        string `json:"http_bind_address,omitempty"`
	TLSCertPath         string `json:"tls_cert_path,omitempty"`
	TLSKeyPath          string `json:"tls_cert_key,omitempty"`
	LogLevel            uint64 `json:"log_level,omitempty"`
	LogFile             string `json:"log_file,omitempty"`
	ZcashConfPath       string `json:"zcash_conf,omitempty"`
	RPCUser             string `json:"rpcuser"`
	RPCPassword         string `json:"rpcpassword"`
	RPCHost             string `json:"rpchost"`
	RPCPort             string `json:"rpcport"`
	NoTLSVeryInsecure   bool   `json:"no_tls_very_insecure,omitempty"`
	GenCertVeryInsecure bool   `json:"gen_cert_very_insecure,omitempty"`
	Redownload          bool   `json:"redownload"`
	DataDir             string `json:"data_dir"`
	Darkside            bool   `json:"darkside"`
	DarksideTimeout     uint64 `json:"darkside_timeout"`
}

// RawRequest points to the function to send a an RPC request to zcashd;
// in production, it points to btcsuite/btcd/rpcclient/rawrequest.go:RawRequest();
// in unit tests it points to a function to mock RPCs to zcashd.
var RawRequest func(method string, params []json.RawMessage) (json.RawMessage, error)

// Sleep allows a request to time.Sleep() to be mocked for testing;
// in production, it points to the standard library time.Sleep();
// in unit tests it points to a mock function.
var Sleep func(d time.Duration)

// Log as a global variable simplifies logging
var Log *logrus.Entry

type (
	Upgradeinfo struct {
		// there are other fields that aren't needed here, omit them
		ActivationHeight int
	}
	ConsensusInfo struct {
		Nextblock string
		Chaintip  string
	}
	Blockchaininfo struct {
		Chain     string
		Upgrades  map[string]Upgradeinfo
		Headers   int
		Consensus ConsensusInfo
	}
)

// GetSaplingInfo returns the result of the getblockchaininfo RPC to zcashd
func GetSaplingInfo() (int, int, string, string) {
	// This request must succeed or we can't go on; give zcashd time to start up
	var blockchaininfo Blockchaininfo
	retryCount := 0
	for {
		result, rpcErr := RawRequest("getblockchaininfo", []json.RawMessage{})
		if rpcErr == nil {
			if retryCount > 0 {
				Log.Warn("getblockchaininfo RPC successful")
			}
			err := json.Unmarshal(result, &blockchaininfo)
			if err != nil {
				Log.Fatalf("error parsing JSON getblockchaininfo response: %v", err)
			}
			break
		}
		retryCount++
		if retryCount > 10 {
			Log.WithFields(logrus.Fields{
				"timeouts": retryCount,
			}).Fatal("unable to issue getblockchaininfo RPC call to zcashd node")
		}
		Log.WithFields(logrus.Fields{
			"error": rpcErr.Error(),
			"retry": retryCount,
		}).Warn("error with getblockchaininfo rpc, retrying...")
		Sleep(time.Duration(10+retryCount*5) * time.Second) // backoff
	}

	// If the sapling consensus branch doesn't exist, it must be regtest
	var saplingHeight int
	if saplingJSON, ok := blockchaininfo.Upgrades["76b809bb"]; ok { // Sapling ID
		saplingHeight = saplingJSON.ActivationHeight
	}

	return saplingHeight, blockchaininfo.Headers, blockchaininfo.Chain,
		blockchaininfo.Consensus.Nextblock
}

func getBlockFromRPC(blockID walletrpc.BlockID) (*walletrpc.CompactBlock, error) {
	// The zcash getblock rpc accepts either a block height or block hash
	params := make([]json.RawMessage, 2)
	if blockID.Hash != nil {
		// id.Hash is big-endian, keep in big-endian for the rpc
		hashJSON, err := json.Marshal(hex.EncodeToString(blockID.Hash))
		if err != nil {
			return nil, errors.Wrap(err, "error marshaling block hash")
		}
		params[0] = hashJSON
	} else {
		heightJSON, err := json.Marshal(strconv.Itoa(int(blockID.Height)))
		if err != nil {
			return nil, errors.Wrap(err, "error marshaling height")
		}
		params[0] = heightJSON
	}
	params[1] = json.RawMessage("0") // non-verbose (raw hex)
	result, rpcErr := RawRequest("getblock", params)

	// For some reason, the error responses are not JSON
	if rpcErr != nil {
		// Check to see if we are requesting a height the zcashd doesn't have yet
		if (strings.Split(rpcErr.Error(), ":"))[0] == "-8" {
			return nil, nil
		}
		return nil, errors.Wrap(rpcErr, "error requesting block")
	}

	var blockDataHex string
	err := json.Unmarshal(result, &blockDataHex)
	if err != nil {
		return nil, errors.Wrap(err, "error reading JSON response")
	}

	blockData, err := hex.DecodeString(blockDataHex)
	if err != nil {
		return nil, errors.Wrap(err, "error decoding getblock output")
	}

	block := parser.NewBlock()
	rest, err := block.ParseFromSlice(blockData)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing block")
	}
	if len(rest) != 0 {
		return nil, errors.New("received overlong message")
	}

	if blockID.Hash != nil {
		if !block.EqualHash(blockID.Hash) {
			Log.Fatal(
				"incorrect hash height ", block.GetHeight(),
				" block hash ", block.GetDisplayHash(),
				" expected ", blockID.Hash)
		}
	} else {
		if block.GetHeight() != int(blockID.Height) {
			Log.Fatal(
				"incorrect height ", block.GetHeight(),
				" expected ", blockID.Height)
		}
	}
	return block.ToCompact(), nil
}

var (
	ingestorRunning  bool
	stopIngestorChan = make(chan struct{})
)

func startIngestor(c *BlockCache) {
	if !ingestorRunning {
		ingestorRunning = true
		go BlockIngestor(c, 0)
	}
}
func stopIngestor() {
	if ingestorRunning {
		ingestorRunning = false
		stopIngestorChan <- struct{}{}
	}
}

// BlockIngestor runs as a goroutine and polls zcashd for new blocks, adding them
// to the cache. The repetition count, rep, is nonzero only for unit-testing.
func BlockIngestor(c *BlockCache, rep int) {
	lastLog := time.Now()
	reorgCount := 0
	lastHeightLogged := 0
	retryCount := 0
	wait := true

	// Start listening for new blocks
	for i := 0; rep == 0 || i < rep; i++ {
		// stop if requested
		select {
		case <-stopIngestorChan:
			return
		default:
		}

		height := c.GetNextHeight()
		block, err := getBlockFromRPC(walletrpc.BlockID{Height: uint64(height)})
		if err != nil {
			Log.WithFields(logrus.Fields{
				"height": height,
				"error":  err,
			}).Warn("error zcashd getblock rpc")
			retryCount++
			if retryCount > 10 {
				Log.WithFields(logrus.Fields{
					"timeouts": retryCount,
				}).Fatal("unable to issue RPC call to zcashd node")
			}
			// Delay then retry the same height.
			c.Sync()
			Sleep(10 * time.Second)
			wait = true
			continue
		}
		retryCount = 0
		if block == nil {
			// No block at this height.
			if height == c.GetFirstHeight() {
				Log.Info("Waiting for zcashd height to reach Sapling activation height ",
					"(", c.GetFirstHeight(), ")...")
				reorgCount = 0
				Sleep(20 * time.Second)
				continue
			}
			if wait {
				// Wait a bit then retry the same height.
				c.Sync()
				if lastHeightLogged+1 != height {
					Log.Info("Ingestor waiting for block: ", height)
					lastHeightLogged = height - 1
				}
				Sleep(2 * time.Second)
				wait = false
				continue
			}
		}
		if block == nil || c.HashMismatch(block.PrevHash) {
			// This may not be a reorg; it may be we're at the tip
			// and there's no new block yet, but we want to back up
			// so we detect a reorg in which the new chain is the
			// same length or shorter.
			reorgCount++
			if reorgCount > 100 {
				Log.Fatal("Reorg exceeded max of 100 blocks! Help!")
			}
			// Print the hash of the block that is getting reorg-ed away
			// as 'phash', not the prevhash of the block we just received.
			if block != nil {
				Log.WithFields(logrus.Fields{
					"height": height,
					"hash":   displayHash(block.Hash),
					"phash":  displayHash(c.GetLatestHash()),
					"reorg":  reorgCount,
				}).Warn("REORG")
			} else if reorgCount > 1 {
				Log.WithFields(logrus.Fields{
					"height": height,
					"phash":  displayHash(c.GetLatestHash()),
					"reorg":  reorgCount,
				}).Warn("REORG")
			}
			// Try backing up
			c.Reorg(height - 1)
			continue
		}
		// We have a valid block to add.
		wait = true
		reorgCount = 0
		if err := c.Add(height, block); err != nil {
			Log.Fatal("Cache add failed:", err)
		}
		// Don't log these too often.
		if time.Now().Sub(lastLog).Seconds() >= 4 && c.GetNextHeight() == height+1 && height != lastHeightLogged {
			lastLog = time.Now()
			lastHeightLogged = height
			Log.Info("Ingestor adding block to cache: ", height)
		}
	}
}

// GetBlock returns the compact block at the requested height, first by querying
// the cache, then, if not found, will request the block from zcashd. It returns
// nil if no block exists at this height.
func GetBlock(cache *BlockCache, blockID walletrpc.BlockID) (*walletrpc.CompactBlock, error) {
	// First, check the cache to see if we have the block
	block := cache.Get(blockID)
	if block != nil {
		return block, nil
	}

	// Not in the cache, ask zcashd
	block, err := getBlockFromRPC(blockID)
	if err != nil {
		return nil, err
	}
	if block == nil {
		// Block height is too large
		return nil, errors.New("block requested is newer than latest block")
	}
	return block, nil
}

// GetBlockRange returns a sequence of consecutive blocks in the given range.
func GetBlockRange(cache *BlockCache, blockOut chan<- *walletrpc.CompactBlock, errOut chan<- error, start, end walletrpc.BlockID) {
	// Go over [start, end] inclusive

	startBlock, err := GetBlock(cache, start)
	if err != nil {
		errOut <- err
		return
	}
	startHeight := startBlock.GetHeight()
	endBlock, err := GetBlock(cache, end)
	if err != nil {
		errOut <- err
		return
	}
	endHeight := endBlock.GetHeight()
	for i := startHeight; i <= endHeight; i++ {
		block, err := GetBlock(cache, walletrpc.BlockID{Height: uint64(i)})
		if err != nil {
			errOut <- err
			return
		}
		blockOut <- block
	}
	errOut <- nil
}

func displayHash(hash []byte) string {
	return hex.EncodeToString(parser.Reverse(hash))
}
