# PostgreSQL Buffer Management (Shared Buffers) - Documentation Structure

## Overview

This document proposes the hierarchical documentation structure for PostgreSQL's buffer management subsystem based on architecture analysis of 68 symbols across 17 functional categories and 8 critical paths. The analysis covers the complete data path from user-space buffer access through kernel page cache to persistent storage.

### Source Files Analyzed

| File | Purpose |
|------|---------|
| `src/backend/storage/buffer/bufmgr.c` | Main buffer manager interface (~6000 lines) |
| `src/backend/storage/buffer/buf_init.c` | Buffer pool initialization (~190 lines) |
| `src/backend/storage/buffer/buf_table.c` | Buffer tag hash table (~160 lines) |
| `src/backend/storage/buffer/freelist.c` | Replacement strategy / clock sweep (~820 lines) |
| `src/backend/storage/buffer/localbuf.c` | Local buffers for temp tables (~600 lines) |
| `src/backend/storage/smgr/smgr.c` | Storage manager interface |
| `src/backend/storage/smgr/md.c` | Magnetic disk storage backend |
| `src/backend/storage/page/bufpage.c` | Page layout and manipulation |
| `src/backend/storage/page/checksum.c` | Page checksum computation |
| `src/include/storage/buf.h` | Basic buffer type definitions |
| `src/include/storage/buf_internals.h` | Internal structures: BufferDesc, BufferTag, flags |
| `src/include/storage/bufmgr.h` | Public buffer manager API |
| `src/include/storage/bufpage.h` | Page header and page access macros |
| `src/include/storage/smgr.h` | Storage manager types and inline wrappers |

---

## Proposed Documentation Structure

### Chapter 1: Buffer Pool Architecture and Shared Memory Layout
**Depth: Deep | Estimated Size: 3000 words | Priority: Critical**

- 1.1 Design Philosophy: Why a Buffer Pool?
  - Double buffering concept (PostgreSQL buffer pool + OS page cache)
  - Trade-offs vs direct I/O
  - Relation to MVCC and crash recovery

- 1.2 Shared Memory Layout
  - `NBuffers` GUC parameter and sizing considerations
  - `BufferDescriptors[]` array -- cache-line-aligned `BufferDescPadded` (64 bytes)
  - `BufferBlocks[]` -- contiguous 8KB page storage (I/O-aligned)
  - `BufferIOCVArray[]` -- per-buffer condition variables for I/O wait
  - `CkptBufferIds[]` -- checkpoint sort array
  - Buffer numbering: 1-based for shared, negative for local

- 1.3 Initialization Flow
  - **Key symbols**: `InitBufferPool`, `BufferShmemSize`, `ShmemInitStruct`
  - Buffer descriptor initialization loop (tag, state, freeNext linked list)
  - Content lock initialization (`LWLockInitialize`)
  - Delegation to `StrategyInitialize` for hash table and freelist

### Chapter 2: Buffer Descriptor and State Management
**Depth: Deep | Estimated Size: 2500 words | Priority: Critical**

- 2.1 BufferDesc Structure
  - **Key symbol**: `BufferDesc`
  - Fields: `tag`, `buf_id`, `state` (atomic uint32), `wait_backend_pgprocno`, `freeNext`, `content_lock`
  - Cache line sizing rationale (< 64 bytes)

- 2.2 Buffer Tag (BufferTag)
  - **Key symbol**: `BufferTag`
  - Fields: `spcOid`, `dbOid`, `relNumber`, `forkNum`, `blockNum`
  - `InitBufferTag`, `ClearBufferTag`, `BufferTagsEqual`
  - Relation to `RelFileLocator`

- 2.3 Atomic State Word Encoding
  - 32-bit packed state: 18 bits refcount + 4 bits usage count + 10 bits flags
  - `BUF_REFCOUNT_MASK`, `BUF_USAGECOUNT_MASK`, `BUF_FLAG_MASK`
  - `BUF_STATE_GET_REFCOUNT()`, `BUF_STATE_GET_USAGECOUNT()`

- 2.4 Buffer State Flags
  - `BM_LOCKED` -- header spinlock bit
  - `BM_DIRTY` -- data needs writing
  - `BM_VALID` -- data is valid
  - `BM_TAG_VALID` -- tag is assigned (has hash table entry)
  - `BM_IO_IN_PROGRESS` -- read or write in progress
  - `BM_IO_ERROR` -- previous I/O failed
  - `BM_JUST_DIRTIED` -- dirtied since write started
  - `BM_PIN_COUNT_WAITER` -- have waiter for sole pin
  - `BM_CHECKPOINT_NEEDED` -- must write for checkpoint
  - `BM_PERMANENT` -- permanent relation (not unlogged/init fork)

### Chapter 3: Buffer Lookup Hash Table
**Depth: Medium | Estimated Size: 1500 words | Priority: High**

- 3.1 Hash Table Design
  - **Key symbols**: `BufTableLookup`, `BufTableInsert`, `BufTableDelete`, `BufTableHashCode`, `InitBufTable`
  - `SharedBufHash` -- shared memory hash table
  - `BufferLookupEnt` -- entry type (BufferTag key -> buffer ID)
  - Sizing: `NBuffers + NUM_BUFFER_PARTITIONS` entries

- 3.2 Partition-Based Locking
  - `NUM_BUFFER_PARTITIONS` (128 by default)
  - `BufTableHashPartition()` -- hashcode % NUM_BUFFER_PARTITIONS
  - `BufMappingPartitionLock()` -- LWLock per partition
  - Lock modes: shared for lookup, exclusive for insert/delete

- 3.3 Interaction with BufferAlloc
  - Lookup-before-insert protocol
  - Race condition handling when two backends request the same page

### Chapter 4: Buffer Access Protocol (Read Path)
**Depth: Deep | Estimated Size: 3500 words | Priority: Critical**

- 4.1 ReadBuffer API Hierarchy
  - **Key symbols**: `ReadBuffer`, `ReadBufferExtended`, `ReadBuffer_common`, `ReadBufferWithoutRelcache`
  - `ReadBufferMode` enum: `RBM_NORMAL`, `RBM_ZERO_AND_LOCK`, `RBM_ZERO_AND_CLEANUP_LOCK`, `RBM_ZERO_ON_ERROR`
  - `BufferManagerRelation` and `BMR_REL`/`BMR_SMGR` macros

- 4.2 BufferAlloc: The Core Allocation Engine
  - **Key symbol**: `BufferAlloc`
  - Step 1: Compute tag and hash code
  - Step 2: Acquire partition lock (shared), lookup in hash table
  - Step 3: If found -- pin the existing buffer
  - Step 4: If not found -- get a victim buffer via `GetVictimBuffer`
  - Step 5: Insert new tag, handle race conditions
  - Step 6: Initialize buffer state flags

- 4.3 Vectorized Read API
  - **Key symbols**: `StartReadBuffers`, `WaitReadBuffers`, `ReadBuffersOperation`
  - Scatter-gather I/O for multi-block reads
  - Integration with read streams

- 4.4 Buffer Extension
  - **Key symbols**: `ExtendBufferedRel`, `ExtendBufferedRelBy`, `ExtendBufferedRelTo`
  - Extension lock coordination

### Chapter 5: Pin and Content Lock Management
**Depth: Deep | Estimated Size: 2500 words | Priority: Critical**

- 5.1 Pin Semantics
  - **Key symbols**: `PinBuffer`, `PinBuffer_Locked`, `UnpinBuffer`, `ReleaseBuffer`
  - Shared refcount in atomic state word
  - Private refcount: `PrivateRefCountEntry` array (8 slots) + overflow hash table
  - `ReservePrivateRefCountEntry`, `ResourceOwnerRememberBuffer`

- 5.2 Content Locks (LWLock per buffer)
  - **Key symbols**: `LockBuffer`, `ConditionalLockBuffer`, `LockBufferForCleanup`
  - Lock modes: `BUFFER_LOCK_SHARE`, `BUFFER_LOCK_EXCLUSIVE`, `BUFFER_LOCK_UNLOCK`
  - `BufferDescriptorGetContentLock()` -- embedded LWLock in BufferDesc

- 5.3 I/O Lock (BM_IO_IN_PROGRESS)
  - **Key symbols**: `StartBufferIO`, `TerminateBufferIO`, `WaitIO`
  - Condition variable per buffer (`BufferIOCVArray`)
  - Protocol: set flag -> do I/O -> clear flag + broadcast

- 5.4 Buffer Header Spinlock
  - **Key symbols**: `LockBufHdr`, `UnlockBufHdr`
  - BM_LOCKED flag in atomic state word
  - Spin-wait with progressive backoff

- 5.5 LockBufferForCleanup Protocol
  - Wait for all other pins to drain
  - `BM_PIN_COUNT_WAITER` flag
  - Recovery conflict handling (`ResolveRecoveryConflictWithBufferPin`)

- 5.6 Lock Ordering Conventions
  - Rule: partition lock before buffer header lock
  - Rule: content lock before I/O operations
  - Rule: never hold buffer header lock while doing I/O

### Chapter 6: Buffer Replacement Policy (Clock Sweep)
**Depth: Deep | Estimated Size: 2000 words | Priority: Critical**

- 6.1 Clock Sweep Algorithm
  - **Key symbols**: `StrategyGetBuffer`, `ClockSweepTick`, `BufferStrategyControl`
  - `nextVictimBuffer` -- atomic clock hand
  - Usage count decrement on each sweep pass
  - `BM_MAX_USAGE_COUNT` (5) -- trade-off between accuracy and sweep speed

- 6.2 Free List
  - `firstFreeBuffer` / `lastFreeBuffer` linked list
  - `StrategyFreeBuffer` -- return buffer to free list
  - Free list checked before clock sweep

- 6.3 Ring Buffer Strategies
  - **Key symbols**: `GetAccessStrategy`, `BufferAccessStrategyData`, `StrategyRejectBuffer`
  - `BAS_BULKREAD` (256KB ring) -- sequential scan
  - `BAS_BULKWRITE` (16MB ring) -- COPY, CREATE TABLE AS
  - `BAS_VACUUM` (2MB ring) -- VACUUM
  - `GetBufferFromRing` / `AddBufferToRing` internal ring management

- 6.4 Victim Buffer Selection
  - **Key symbol**: `GetVictimBuffer`
  - Dirty victim: flush before reuse (WAL-before-data)
  - `StrategyRejectBuffer` for bulk read -- avoid WAL flush penalty

### Chapter 7: Page Layout and Structure
**Depth: Medium | Estimated Size: 2000 words | Priority: High**

- 7.1 PageHeaderData Structure
  - **Key symbol**: `PageHeaderData`
  - `pd_lsn` -- LSN of last change (WAL-before-data enforcement)
  - `pd_checksum` -- page checksum
  - `pd_flags` -- `PD_HAS_FREE_LINES`, `PD_PAGE_FULL`, `PD_ALL_VISIBLE`
  - `pd_lower` / `pd_upper` -- free space boundaries
  - `pd_special` -- special space for access method opaque data
  - `pd_pagesize_version` -- packed page size + layout version
  - `pd_prune_xid` -- oldest prunable XID hint
  - `pd_linp[]` -- line pointer (ItemId) array

- 7.2 Page Operations
  - **Key symbols**: `PageInit`, `PageAddItemExtended`, `PageRepairFragmentation`
  - `PageGetFreeSpace`, `PageGetHeapFreeSpace`, `PageGetExactFreeSpace`
  - `PageIndexTupleDelete`, `PageIndexMultiDelete`

- 7.3 Page Verification and Checksums
  - **Key symbols**: `PageIsVerifiedExtended`, `PageSetChecksumCopy`, `PageSetChecksumInplace`
  - Copy-on-write for concurrent checksum safety
  - `PG_DATA_CHECKSUM_VERSION`

### Chapter 8: Dirty Buffer Management and Write-Back
**Depth: Deep | Estimated Size: 2500 words | Priority: Critical**

- 8.1 Marking Buffers Dirty
  - **Key symbols**: `MarkBufferDirty`, `MarkBufferDirtyHint`
  - `BM_DIRTY` + `BM_JUST_DIRTIED` flags
  - Hint bit updates: reduced WAL overhead with `XLogSaveBufferForHint`

- 8.2 FlushBuffer: The Write Engine
  - **Key symbol**: `FlushBuffer`
  - WAL-before-data: `XLogFlush(PageGetLSN(page))`
  - Checksum computation via `PageSetChecksumCopy`
  - Write via `smgrwrite` -> `mdwritev`
  - Error callback registration

- 8.3 Background Writer
  - **Key symbol**: `BgBufferSync`
  - Adaptive scan rate based on buffer allocation rate
  - `StrategySyncStart` -- synchronization with clock sweep
  - `StrategyNotifyBgWriter` -- hibernation/wake-up protocol

- 8.4 Checkpoint Buffer Flush
  - **Key symbols**: `CheckPointBuffers`, `BufferSync`, `SyncOneBuffer`
  - Two-phase: scan for dirty buffers, then write with tablespace interleaving
  - `CkptSortItem` / `CkptTsStatus` for I/O scheduling
  - `BM_CHECKPOINT_NEEDED` flag
  - `CheckpointWriteDelay` for throttling

- 8.5 Writeback Context
  - **Key symbols**: `WritebackContext`, `WritebackContextInit`, `ScheduleBufferTagForWriteback`, `IssuePendingWritebacks`
  - Coalescing adjacent writes
  - GUC parameters: `checkpoint_flush_after`, `bgwriter_flush_after`, `backend_flush_after`

### Chapter 9: WAL-Before-Data Guarantee and LSN Management
**Depth: Medium | Estimated Size: 1500 words | Priority: High**

- 9.1 The Fundamental Rule
  - "Thou shalt write xlog before data" -- enforced by `FlushBuffer`
  - `pd_lsn` in PageHeaderData is the enforcement point

- 9.2 Page LSN Operations
  - **Key symbols**: `PageGetLSN`, `PageSetLSN`, `BufferGetLSNAtomic`
  - `PageXLogRecPtr` -- split 64-bit LSN stored as two 32-bit values
  - Atomic LSN read without content lock

- 9.3 Full-Page Writes
  - First modification after checkpoint triggers full-page image in WAL
  - `XLogInsert` with `REGBUF_FORCE_IMAGE`
  - Recovery: `XLogInitBufferForRedo`

- 9.4 Hint Bit Updates and WAL
  - `MarkBufferDirtyHint` -- may skip WAL for non-permanent buffers
  - `XLogSaveBufferForHint` -- full-page write to protect checksums

### Chapter 10: Storage Manager Layer
**Depth: Medium | Estimated Size: 2000 words | Priority: High**

- 10.1 SMgr Interface
  - **Key symbols**: `smgropen`, `smgrread`, `smgrwrite`, `smgrextend`, `smgrreadv`, `smgrwritev`
  - `f_smgr` function pointer table
  - `SMgrRelation` -- per-relation storage state with cached nblocks
  - Transaction-scoped lifetime with pin/unpin

- 10.2 Magnetic Disk Manager (md.c)
  - **Key symbols**: `mdreadv`, `mdwritev`, `mdextend`, `mdwriteback`, `mdnblocks`
  - `MdfdVec` -- segment file descriptor (VFD + segment number)
  - Segment layout: RELSEG_SIZE blocks per segment (1GB default)
  - `mdopenfork` -- lazy segment opening

- 10.3 VFD Layer
  - Virtual file descriptor pool
  - LRU-based file descriptor management
  - `FileRead`, `FileWrite`, `FileWriteV`, `FileReadV`, `FileWriteback`, `FilePrefetch`

### Chapter 11: Data Movement: Memory to Disk
**Depth: Medium | Estimated Size: 1500 words | Priority: Medium**

- 11.1 Double Buffering Architecture
  - PostgreSQL shared buffers <-> OS kernel page cache <-> disk
  - `smgrwrite` writes to kernel page cache (not direct I/O by default)
  - `smgrwriteback` (`FileWriteback`) advises kernel to flush pages

- 11.2 Fsync Semantics
  - Checkpoint fsync via `SyncFileRange` / `msync`
  - `smgrimmedsync` -- immediate sync for non-WAL-logged writes
  - `mdregistersync` / `smgrDoPendingSyncs` -- deferred sync requests

- 11.3 Direct I/O Option
  - `io_direct` GUC parameter (`IO_DIRECT_DATA`, `IO_DIRECT_WAL`)
  - Impact on double buffering behavior
  - `PG_IO_ALIGN_SIZE` alignment requirement

### Chapter 12: Local Buffers for Temporary Tables
**Depth: Medium | Estimated Size: 1200 words | Priority: Medium**

- 12.1 Design Rationale
  - No WAL logging needed
  - No shared memory -- private to backend
  - No locking overhead

- 12.2 Implementation
  - **Key symbols**: `LocalBufferAlloc`, `PinLocalBuffer`, `UnpinLocalBuffer`, `MarkLocalBufferDirty`
  - `LocalBufferDescriptors` array -- same `BufferDesc` struct, different usage
  - `LocalBufferBlockPointers` / `LocalRefCount`
  - Local hash table (`LocalBufHash`)
  - Buffer numbering: negative Buffer values (-1 .. -NLocBuffer)

- 12.3 Initialization and Storage
  - **Key symbols**: `InitLocalBuffers`, `GetLocalBufferStorage`
  - Lazy initialization on first access
  - Block allocation for efficient memory management

### Chapter 13: Integration with Access Methods
**Depth: Medium | Estimated Size: 1500 words | Priority: Medium**

- 13.1 Heap Access Method
  - `heap_fetch` -> `ReadBuffer` -> `LockBuffer(SHARE)` -> read tuple -> `ReleaseBuffer`
  - `heap_insert` -> `ReadBuffer` -> `LockBuffer(EXCLUSIVE)` -> `PageAddItem` -> `MarkBufferDirty` -> `UnlockReleaseBuffer`
  - `heap_delete`, `heap_update`, `heap_lock_tuple` patterns
  - `RelationGetBufferForTuple` -- finds page with space

- 13.2 B-tree Index Access
  - `_bt_getbuf` / `_bt_allocbuf` -> `ReadBuffer`
  - Page-level locking for tree traversal
  - `_bt_search_insert` buffer management

- 13.3 VACUUM Integration
  - `BAS_VACUUM` ring buffer strategy
  - `lazy_scan_heap` -> `ReadBufferExtended` with strategy
  - `LockBufferForCleanup` for tuple deletion
  - `count_nondeletable_pages` truncation scan

- 13.4 Prefetching
  - **Key symbols**: `PrefetchBuffer`, `PrefetchSharedBuffer`
  - `effective_io_concurrency` / `maintenance_io_concurrency` GUCs
  - Bitmap heap scan prefetch (`BitmapPrefetch`)

---

## Estimated Total Documentation Size

| Chapter | Words | Priority |
|---------|-------|----------|
| Ch 1: Pool Architecture | 3000 | Critical |
| Ch 2: Descriptor & State | 2500 | Critical |
| Ch 3: Hash Table | 1500 | High |
| Ch 4: Access Protocol | 3500 | Critical |
| Ch 5: Pin & Lock | 2500 | Critical |
| Ch 6: Replacement Policy | 2000 | Critical |
| Ch 7: Page Layout | 2000 | High |
| Ch 8: Dirty Management | 2500 | Critical |
| Ch 9: WAL Integration | 1500 | High |
| Ch 10: Storage Manager | 2000 | High |
| Ch 11: Data Movement | 1500 | Medium |
| Ch 12: Local Buffers | 1200 | Medium |
| Ch 13: Access Methods | 1500 | Medium |
| **Total** | **~26,700** | |

## Key Architecture Insights

1. **Layered Design**: The buffer manager is cleanly layered: public API (ReadBuffer/LockBuffer/MarkBufferDirty) -> internal engine (BufferAlloc/FlushBuffer) -> strategy (StrategyGetBuffer) -> storage (smgr -> md -> VFD).

2. **Lock-Free Fast Path**: Pin/unpin operations use CAS on the atomic state word to avoid spinlock acquisition in the common (no-contention) case. The private refcount array (8 slots) keeps frequently pinned buffers entirely in backend-local memory.

3. **Unified State Word**: Packing refcount (18 bits), usage count (4 bits), and flags (10 bits) into a single 32-bit atomic word enables lock-free state transitions for common operations.

4. **WAL-Before-Data as Architectural Invariant**: The `pd_lsn` field in every page header creates a hard dependency between the buffer manager and the WAL subsystem. `FlushBuffer` enforces this by calling `XLogFlush` before every page write.

5. **Ring Buffer Isolation**: Access strategies (`BAS_BULKREAD`, `BAS_BULKWRITE`, `BAS_VACUUM`) prevent sequential scans and bulk operations from evicting frequently used pages from the main buffer pool.

6. **Clock Sweep Simplicity**: The replacement algorithm uses a single atomic counter (`nextVictimBuffer`) as the clock hand, requiring no per-buffer timestamp tracking and minimal synchronization.

7. **Background Writer Coordination**: `BgBufferSync` uses allocation rate statistics from `StrategySyncStart` to adaptively tune its scan rate, with a hibernation mechanism via `StrategyNotifyBgWriter` when the system is idle.

8. **Checkpoint I/O Scheduling**: `BufferSync` sorts dirty buffers by tablespace and interleaves writes across tablespaces for fair I/O distribution, with `CheckpointWriteDelay` throttling to limit checkpoint impact.
