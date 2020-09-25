// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .

// Package common contains utilities that are shared by other packages.
package common

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zcash/lightwalletd/walletrpc"
)

// BlockCache contains a consecutive set of recent compact blocks in marshalled form.
type (
	hashSmall  [4]byte
	hashLarge  [32]byte
	BlockCache struct {
		lengthsName, blocksName string // pathnames
		lengthsFile, blocksFile *os.File
		starts                  []int64 // Starting offset of each block within blocksFile
		firstBlock              int     // height of the first block in the cache (usually Sapling activation)
		nextBlock               int     // height of the first block not in the cache
		latestHash              []byte  // hash of the most recent (highest height) block, for detecting reorgs.
		mutex                   sync.RWMutex

		// These give the block height for the given block hash,
		// the large map is for collisions (always check first).
		blockHashMapLarge map[hashLarge]int
		blockHashMapSmall map[hashSmall]int
	}
)

// getHeightFromHash returns the block height for a given hash, or -1 if not found.
// The hash argument must be little-endian.
func (c *BlockCache) getHeightFromHash(hashSlice []byte) int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	var hl hashLarge
	copy(hl[:], hashSlice)
	if h, ok := c.blockHashMapLarge[hl]; ok {
		if h >= c.nextBlock {
			return -1
		}
		return h
	}
	var hs hashSmall
	copy(hs[:], hashSlice)
	if h, ok := c.blockHashMapSmall[hs]; ok {
		if h >= c.nextBlock {
			return -1
		}
		return h
	}
	return -1
}

// Add an entry to the block hash map.
// The hash argument must be little-endian.
func (c *BlockCache) addBlockHashMap(hashSlice []byte, height int) {
	var hs hashSmall
	var hl hashLarge
	copy(hs[:], hashSlice)
	copy(hl[:], hashSlice)
	if h, ok := c.blockHashMapSmall[hs]; ok && h != height {
		c.blockHashMapLarge[hl] = height
	} else {
		c.blockHashMapSmall[hs] = h
		delete(c.blockHashMapLarge, hl)
	}
}

// GetNextHeight returns the height of the lowest unobtained block.
func (c *BlockCache) GetNextHeight() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.nextBlock
}

// GetFirstHeight returns the height of the lowest block (usually Sapling activation).
func (c *BlockCache) GetFirstHeight() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.firstBlock
}

// GetLatestHash returns the hash (block ID) of the most recent (highest) known block.
func (c *BlockCache) GetLatestHash() []byte {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.latestHash
}

// HashMismatch indicates if the given prev-hash doesn't match the most recent block's hash
// so reorgs can be detected.
func (c *BlockCache) HashMismatch(prevhash []byte) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.latestHash != nil && !bytes.Equal(c.latestHash, prevhash)
}

// Make the block at the given height the first height that we don't have.
// In other words, wipe out this height and beyond.
// Caller should hold c.mutex.Lock().
func (c *BlockCache) setDbFiles(height int) {
	if height <= c.nextBlock {
		// we could remove entries from the block hash maps,
		// but it's not worth it, and does no harm.
		if height < c.firstBlock {
			height = c.firstBlock
		}
		index := height - c.firstBlock
		if err := c.lengthsFile.Truncate(int64(index * 4)); err != nil {
			Log.Fatal("truncate lengths file failed: ", err)
		}
		if err := c.blocksFile.Truncate(c.starts[index]); err != nil {
			Log.Fatal("truncate blocks file failed: ", err)
		}
		c.Sync()
		c.starts = c.starts[:index+1]
		c.nextBlock = height
		c.setLatestHash()
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

// Caller should hold c.mutex.Lock().
func (c *BlockCache) recoverFromCorruption(height int) {
	Log.Warning("CORRUPTION detected in db blocks-cache files, height ", height, " redownloading")

	// Save the corrupted files for post-mortem analysis.
	save := c.lengthsName + "-corrupted"
	if err := copyFile(c.lengthsName, save); err != nil {
		Log.Warning("Could not copy db lengths file: ", err)
	}
	save = c.blocksName + "-corrupted"
	if err := copyFile(c.blocksName, save); err != nil {
		Log.Warning("Could not copy db lengths file: ", err)
	}

	c.setDbFiles(height)
}

// not including the checksum
func (c *BlockCache) blockLength(height int) int {
	index := height - c.firstBlock
	return int(c.starts[index+1] - c.starts[index] - 8)
}

// Calculate the 8-byte checksum that precedes each block in the blocks file.
func checksum(height int, b []byte) []byte {
	h := make([]byte, 8)
	binary.LittleEndian.PutUint64(h, uint64(height))
	cs := fnv.New64a()
	cs.Write(h)
	cs.Write(b)
	return cs.Sum(nil)
}

// Caller should hold (at least) c.mutex.RLock().
func (c *BlockCache) readBlock(height int) *walletrpc.CompactBlock {
	blockLen := c.blockLength(height)
	b := make([]byte, blockLen+8)
	offset := c.starts[height-c.firstBlock]
	n, err := c.blocksFile.ReadAt(b, offset)
	if err != nil || n != len(b) {
		Log.Warning("blocks read offset: ", offset, " failed: ", n, err)
		return nil
	}
	diskcs := b[:8]
	b = b[8 : blockLen+8]
	if !bytes.Equal(checksum(height, b), diskcs) {
		Log.Warning("bad block checksum at height: ", height, " offset: ", offset)
		return nil
	}
	block := &walletrpc.CompactBlock{}
	err = proto.Unmarshal(b, block)
	if err != nil {
		// Could be file corruption.
		Log.Warning("blocks unmarshal at offset: ", offset, " failed: ", err)
		return nil
	}
	if int(block.Height) != height {
		// Could be file corruption.
		Log.Warning("block unexpected height at height ", height, " offset: ", offset)
		return nil
	}
	return block
}

// Caller should hold c.mutex.Lock().
func (c *BlockCache) setLatestHash() {
	c.latestHash = nil
	// There is at least one block; get the last block's hash
	if c.nextBlock > c.firstBlock {
		// At least one block remains; get the last block's hash
		block := c.readBlock(c.nextBlock - 1)
		if block == nil {
			c.recoverFromCorruption(c.nextBlock - 10000)
			return
		}
		c.latestHash = make([]byte, len(block.Hash))
		copy(c.latestHash, block.Hash)
	}
}

// Reset is used only for darkside testing.
func (c *BlockCache) Reset(startHeight int) {
	c.setDbFiles(c.firstBlock) // empty the cache
	c.firstBlock = startHeight
	c.nextBlock = startHeight
}

// NewBlockCache returns an instance of a block cache object.
// (No locking here, we assume this is single-threaded.)
func NewBlockCache(dbPath string, chainName string, startHeight int, redownload bool) *BlockCache {
	c := &BlockCache{}
	c.firstBlock = startHeight
	c.nextBlock = startHeight
	c.lengthsName, c.blocksName = dbFileNames(dbPath, chainName)
	var err error
	if err := os.MkdirAll(filepath.Join(dbPath, chainName), 0755); err != nil {
		Log.Fatal("mkdir ", dbPath, " failed: ", err)
	}
	c.blocksFile, err = os.OpenFile(c.blocksName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		Log.Fatal("open ", c.blocksName, " failed: ", err)
	}
	c.lengthsFile, err = os.OpenFile(c.lengthsName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		Log.Fatal("open ", c.lengthsName, " failed: ", err)
	}
	if redownload {
		if err := c.lengthsFile.Truncate(0); err != nil {
			Log.Fatal("truncate lengths file failed: ", err)
		}
		if err := c.blocksFile.Truncate(0); err != nil {
			Log.Fatal("truncate blocks file failed: ", err)
		}
	}
	lengths, err := ioutil.ReadFile(c.lengthsName)
	if err != nil {
		Log.Fatal("read ", c.lengthsName, " failed: ", err)
	}
	c.blockHashMapLarge = make(map[hashLarge]int, 0)
	c.blockHashMapSmall = make(map[hashSmall]int, 0)

	// The last entry in starts[] is where to write the next block.
	var offset int64
	c.starts = nil
	c.starts = append(c.starts, 0)
	for i := 0; i < len(lengths)/4; i++ {
		if len(lengths[:4]) < 4 {
			Log.Warning("lengths file has a partial entry")
			c.recoverFromCorruption(c.nextBlock)
			break
		}
		length := binary.LittleEndian.Uint32(lengths[i*4 : (i+1)*4])
		if length < 74 || length > 4*1000*1000 {
			Log.Warning("lengths file has impossible value ", length)
			c.recoverFromCorruption(c.nextBlock)
			break
		}
		offset += int64(length) + 8
		c.starts = append(c.starts, offset)
		// Check for corruption.
		block := c.readBlock(c.nextBlock)
		if block == nil {
			Log.Warning("error reading block")
			c.recoverFromCorruption(c.nextBlock)
			break
		}
		c.addBlockHashMap(block.Hash, i)
		c.nextBlock++
	}
	c.setDbFiles(c.nextBlock)
	Log.Info("Found ", c.nextBlock-c.firstBlock, " blocks in cache")
	return c
}

func dbFileNames(dbPath string, chainName string) (string, string) {
	return filepath.Join(dbPath, chainName, "lengths"),
		filepath.Join(dbPath, chainName, "blocks")
}

// Add adds the given block to the cache at the given height, returning true
// if a reorg was detected.
func (c *BlockCache) Add(height int, block *walletrpc.CompactBlock) error {
	// Invariant: m[firstBlock..nextBlock) are valid.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if height > c.nextBlock {
		// Cache has been reset (for example, checksum error)
		return nil
	}
	if height < c.firstBlock {
		// Should never try to add a block before Sapling activation height
		Log.Fatal("cache.Add height below Sapling: ", height)
		return nil
	}
	if height < c.nextBlock {
		// Should never try to "backup" (call Reorg() instead).
		Log.Fatal("cache.Add height going backwards: ", height)
		return nil
	}
	if int(block.Height) != height {
		// This could only happen if zcashd returned the wrong
		// block (not the height we requested).
		Log.Fatal("cache.Add wrong height: ", block.Height, " expecting: ", height)
		return nil
	}

	// Add the new block and its length to the db files.
	data, err := proto.Marshal(block)
	if err != nil {
		return err
	}
	_, err = c.blocksFile.Write(append(checksum(height, data), data...))
	if err != nil {
		Log.Fatal("blocks write failed: ", err)
	}
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(len(data)))
	_, err = c.lengthsFile.Write(b)
	if err != nil {
		Log.Fatal("lengths write failed: ", err)
	}

	// update the in-memory variables
	offset := c.starts[len(c.starts)-1]
	c.starts = append(c.starts, offset+int64(len(data)+8))
	c.addBlockHashMap(block.Hash, height)

	if c.latestHash == nil {
		c.latestHash = make([]byte, len(block.Hash))
	}
	copy(c.latestHash, block.Hash)
	c.nextBlock++
	// Invariant: m[firstBlock..nextBlock) are valid.
	return nil
}

// Reorg resets nextBlock (the block that should be Add()ed next)
// downward to the given height.
func (c *BlockCache) Reorg(height int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Allow the caller not to have to worry about Sapling start height.
	if height < c.firstBlock {
		height = c.firstBlock
	}
	if height >= c.nextBlock {
		// Timing window, ignore this request
		return
	}
	// Remove the end of the cache.
	c.nextBlock = height
	newCacheLen := height - c.firstBlock
	c.starts = c.starts[:newCacheLen+1]

	if err := c.lengthsFile.Truncate(int64(4 * newCacheLen)); err != nil {
		Log.Fatal("truncate failed: ", err)
	}
	if err := c.blocksFile.Truncate(c.starts[newCacheLen]); err != nil {
		Log.Fatal("truncate failed: ", err)
	}
	c.setLatestHash()
}

// Get returns the compact block at the requested hash or height if it's
// in the cache, else nil.
func (c *BlockCache) Get(blockID walletrpc.BlockID) *walletrpc.CompactBlock {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var height = int(blockID.Height)
	if blockID.Hash != nil {
		// this takes precedence as a specification
		height = c.getHeightFromHash(blockID.Hash)
	}
	if height < c.firstBlock || height >= c.nextBlock {
		return nil
	}
	block := c.readBlock(height)
	if block == nil {
		go func() {
			// We hold only the read lock, need the exclusive lock.
			c.mutex.Lock()
			c.recoverFromCorruption(height - 10000)
			c.mutex.Unlock()
		}()
		return nil
	}
	return block
}

// GetLatestHeight returns the height of the most recent block, or -1
// if the cache is empty.
func (c *BlockCache) GetLatestHeight() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.firstBlock == c.nextBlock {
		return -1
	}
	return c.nextBlock - 1
}

// Sync ensures that the db files are flushed to disk, can be called unnecessarily.
func (c *BlockCache) Sync() {
	c.lengthsFile.Sync()
	c.blocksFile.Sync()
}

// Close is Currently used only for testing.
func (c *BlockCache) Close() {
	// Some operating system require you to close files before you can remove them.
	if c.lengthsFile != nil {
		c.lengthsFile.Close()
		c.lengthsFile = nil
	}
	if c.blocksFile != nil {
		c.blocksFile.Close()
		c.blocksFile = nil
	}
}
