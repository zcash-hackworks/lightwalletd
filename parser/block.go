// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .

// Package parser deserializes blocks from zcashd.
package parser

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/zcash/lightwalletd/parser/internal/bytestring"
	"github.com/zcash/lightwalletd/walletrpc"
)

// Block represents a full block (not a compact block).
type Block struct {
	hdr    *BlockHeader
	vtx    []*Transaction
	height int
}

// NewBlock constructs a block instance.
func NewBlock() *Block {
	return &Block{height: -1}
}

// GetVersion returns a block's version number (current 4)
func (b *Block) GetVersion() int {
	return int(b.hdr.Version)
}

// GetTxCount returns the number of transactions in the block,
// including the coinbase transaction (minimum 1).
func (b *Block) GetTxCount() int {
	return len(b.vtx)
}

// Transactions returns the list of the block's transactions.
func (b *Block) Transactions() []*Transaction {
	// TODO: these should NOT be mutable
	return b.vtx
}

// GetDisplayHash returns the block hash in big-endian display order.
func (b *Block) GetDisplayHash() []byte {
	return b.hdr.GetDisplayHash()
}

// TODO: encode hash endianness in a type?

// GetEncodableHash returns the block hash in little-endian wire order.
func (b *Block) GetEncodableHash() []byte {
	return b.hdr.GetEncodableHash()
}

// GetDisplayPrevHash returns the block's previous hash in big-endian format.
func (b *Block) GetDisplayPrevHash() []byte {
	return b.hdr.GetDisplayPrevHash()
}

// EqualHash indicates if the block's hash matches the given hash
func (b *Block) EqualHash(hash []byte) bool {
	return b.hdr.EqualHash(hash)
}

// HasSaplingTransactions indicates if the block contains any Sapling tx.
func (b *Block) HasSaplingTransactions() bool {
	for _, tx := range b.vtx {
		if tx.HasSaplingElements() {
			return true
		}
	}
	return false
}

// see https://github.com/zcash/lightwalletd/issues/17#issuecomment-467110828
const genesisTargetDifficulty = 520617983

// GetHeight extracts the block height from the coinbase transaction. See
// BIP34. Returns block height on success, or -1 on error.
func (b *Block) GetHeight() int {
	if b.height != -1 {
		return b.height
	}
	coinbaseScript := bytestring.String(b.vtx[0].transparentInputs[0].ScriptSig)
	var heightNum int64
	if !coinbaseScript.ReadScriptInt64(&heightNum) {
		return -1
	}
	if heightNum < 0 {
		return -1
	}
	// uint32 should last us a while (Nov 2018)
	if heightNum > int64(^uint32(0)) {
		return -1
	}
	blockHeight := uint32(heightNum)

	if blockHeight == genesisTargetDifficulty {
		blockHeight = 0
	}

	b.height = int(blockHeight)
	return int(blockHeight)
}

// GetPrevHash returns the hash of the block's previous block (little-endian).
func (b *Block) GetPrevHash() []byte {
	return b.hdr.HashPrevBlock
}

// ToCompact returns the compact representation of the full block.
func (b *Block) ToCompact() *walletrpc.CompactBlock {
	compactBlock := &walletrpc.CompactBlock{
		//TODO ProtoVersion: 1,
		Height:   uint64(b.GetHeight()),
		PrevHash: b.hdr.HashPrevBlock,
		Hash:     b.GetEncodableHash(),
		Time:     b.hdr.Time,
	}

	// Only Sapling transactions have a meaningful compact encoding
	saplingTxns := make([]*walletrpc.CompactTx, 0, len(b.vtx))
	for idx, tx := range b.vtx {
		if tx.HasSaplingElements() {
			saplingTxns = append(saplingTxns, tx.ToCompact(idx))
		}
	}
	compactBlock.Vtx = saplingTxns
	return compactBlock
}

// ParseFromSlice deserializes a block from the given data stream
// and returns a slice to the remaining data. The caller should verify
// there is no remaining data if none is expected.
func (b *Block) ParseFromSlice(data []byte) (rest []byte, err error) {
	hdr := NewBlockHeader()
	data, err = hdr.ParseFromSlice(data)
	if err != nil {
		return nil, errors.Wrap(err, "parsing block header")
	}

	s := bytestring.String(data)
	var txCount int
	if !s.ReadCompactSize(&txCount) {
		return nil, errors.New("could not read tx_count")
	}
	data = []byte(s)

	vtx := make([]*Transaction, 0, txCount)
	var i int
	for i = 0; i < txCount && len(data) > 0; i++ {
		tx := NewTransaction()
		data, err = tx.ParseFromSlice(data)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("parsing transaction %d", i))
		}
		vtx = append(vtx, tx)
	}
	if i < txCount {
		return nil, errors.New("parsing block transactions: not enough data")
	}
	b.hdr = hdr
	b.vtx = vtx
	return data, nil
}
