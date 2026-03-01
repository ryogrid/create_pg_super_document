# Buffer Pool Architecture and Shared Memory Layout

## Overview

PostgreSQL's buffer pool is a fixed-size region of shared memory that caches disk pages (8 KB blocks) in RAM, mediating all data access between backends and the storage subsystem. Every read or write of a relation page passes through the buffer manager, which maintains a pool of `NBuffers` page-sized slots (controlled by the `shared_buffers` GUC parameter). The buffer pool serves as the central caching layer, implementing a clock-sweep replacement algorithm, pin-based reference counting for concurrency safety, and WAL-before-data enforcement for crash recovery.

## Design Philosophy

### Why a Buffer Pool?

PostgreSQL intentionally interposes its own buffer cache between the executor and the operating system's page cache. This design provides several advantages:

1. **Controlled replacement policy**: The clock-sweep algorithm is tuned for database workloads, unlike the OS's generic LRU policy.
2. **Pin-based concurrency control**: Backends can hold references (pins) that guarantee a page remains in memory, which no OS page cache API supports.
3. **WAL integration**: The buffer manager enforces the WAL-before-data rule by checking `pd_lsn` before writing any dirty page, a constraint impossible to enforce through the OS cache alone.
4. **Checkpoint coordination**: The buffer manager tracks dirty pages and coordinates their write-out during checkpoints.
5. **Visibility tracking**: Buffer pins prevent physical removal of tuples that other backends may still reference.

### Double Buffering

PostgreSQL's buffer pool exists on top of the OS kernel page cache, creating a "double buffering" architecture:

```
  Backend Process
       |
  [Shared Buffer Pool]   <-- PostgreSQL-managed, NBuffers * 8KB
       |
  [OS Page Cache]         <-- Kernel-managed
       |
  [Physical Disk]
```

When `smgrwrite()` writes a dirty page, it writes into the kernel page cache (via `write()`), not directly to disk. The kernel later flushes to disk on its own schedule, or when PostgreSQL explicitly requests it via `fsync` at checkpoint time. This means data passes through two caches. The `io_direct` GUC can bypass the OS cache for data files if desired, reducing memory waste at the cost of losing OS-level read-ahead.

## Shared Memory Layout

The buffer pool is allocated in shared memory during postmaster startup by `InitBufferPool()` (source: `src/backend/storage/buffer/buf_init.c:67-151`). The layout consists of four parallel arrays plus support structures:

```
Shared Memory Region
|
+-- BufferDescriptors[NBuffers]   (BufferDescPadded, 64 bytes each, cache-line aligned)
|
+-- BufferBlocks[NBuffers]        (char*, NBuffers * BLCKSZ bytes, I/O aligned)
|
+-- BufferIOCVArray[NBuffers]     (ConditionVariableMinimallyPadded, per-buffer I/O wait)
|
+-- CkptBufferIds[NBuffers]       (CkptSortItem, checkpoint sorting workspace)
|
+-- SharedBufHash                 (Partitioned hash table: BufferTag -> buf_id)
|
+-- BufferStrategyControl         (Clock sweep state: nextVictimBuffer, freelist, stats)
```

### Array Relationships

All arrays are indexed by `buf_id` (0-based). The relationship between a buffer descriptor and its data block is:

```c
/* From src/backend/storage/buffer/bufmgr.c:67 */
#define BufHdrGetBlock(bufHdr) ((Block) (BufferBlocks + ((Size) (bufHdr)->buf_id) * BLCKSZ))
```

The public `Buffer` type is 1-based for shared buffers (Buffer = buf_id + 1) and negative for local buffers. This encoding is implemented in:

```c
/* From src/include/storage/buf_internals.h:330-334 */
static inline Buffer
BufferDescriptorGetBuffer(const BufferDesc *bdesc)
{
    return (Buffer) (bdesc->buf_id + 1);
}
```

### Memory Sizing

`BufferShmemSize()` computes the total shared memory requirement (source: `src/backend/storage/buffer/buf_init.c:159-186`):

| Component | Size per Buffer | Purpose |
|-----------|----------------|---------|
| `BufferDescPadded` | 64 bytes | Descriptor with cache-line padding |
| `BLCKSZ` | 8,192 bytes | Page data |
| `ConditionVariableMinimallyPadded` | ~64 bytes | I/O wait CV |
| `CkptSortItem` | 20 bytes | Checkpoint sort workspace |
| Hash table entry | ~40 bytes | BufferTag -> buf_id mapping |

For the default `shared_buffers = 128MB` (16,384 buffers), total shared memory for the buffer subsystem is approximately 128 MB (data) + 1.6 MB (descriptors) + 1.0 MB (CVs) + 0.3 MB (ckpt) + 0.9 MB (hash) = ~132 MB.

## BufferDesc: The Buffer Descriptor

Each buffer slot has a corresponding `BufferDesc` structure (source: `src/include/storage/buf_internals.h:245-256`):

```c
typedef struct BufferDesc
{
    BufferTag   tag;            /* ID of page contained in buffer */
    int         buf_id;         /* buffer's index number (from 0) */

    /* state of the tag, containing flags, refcount and usagecount */
    pg_atomic_uint32 state;

    int         wait_backend_pgprocno;  /* backend of pin-count waiter */
    int         freeNext;       /* link in freelist chain */
    LWLock      content_lock;   /* to lock access to buffer contents */
} BufferDesc;
```

The structure is padded to exactly 64 bytes (one cache line) using `BufferDescPadded`:

```c
/* From src/include/storage/buf_internals.h:278-284 */
#define BUFFERDESC_PAD_TO_SIZE  (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union BufferDescPadded
{
    BufferDesc  bufferdesc;
    char        pad[BUFFERDESC_PAD_TO_SIZE];
} BufferDescPadded;
```

This cache-line alignment is critical for performance in highly concurrent workloads, preventing false sharing between adjacent descriptors.

## BufferTag: Page Identity

A `BufferTag` uniquely identifies a disk page (source: `src/include/storage/buf_internals.h:93-100`):

```c
typedef struct buftag
{
    Oid         spcOid;         /* tablespace oid */
    Oid         dbOid;          /* database oid */
    RelFileNumber relNumber;    /* relation file number */
    ForkNumber  forkNum;        /* fork number */
    BlockNumber blockNum;       /* blknum relative to begin of reln */
} BufferTag;
```

The tag must contain enough information to locate the block on disk without consulting any catalog tables, because the backend flushing a buffer may not have visibility into the relation's catalog entry. This is noted explicitly in the source comment: "It's possible that the backend flushing the buffer doesn't even believe the relation is visible yet."

## Atomic State Word Encoding

The `state` field is a single 32-bit atomic variable encoding three pieces of information (source: `src/include/storage/buf_internals.h:31-48`):

```
Bit Layout (32 bits):
[31..22] flags     (10 bits)  -- BM_LOCKED, BM_DIRTY, BM_VALID, etc.
[21..18] usagecount (4 bits)  -- 0..15 (capped at BM_MAX_USAGE_COUNT=5)
[17.. 0] refcount  (18 bits)  -- 0..262143
```

Key constants:

```c
#define BUF_REFCOUNT_ONE    1
#define BUF_REFCOUNT_MASK   ((1U << 18) - 1)        /* 0x0003FFFF */
#define BUF_USAGECOUNT_MASK 0x003C0000U
#define BUF_USAGECOUNT_ONE  (1U << 18)               /* 0x00040000 */
#define BUF_FLAG_MASK       0xFFC00000U
```

This packing enables lock-free CAS operations for common state transitions (pin/unpin, dirty marking) without acquiring the buffer header spinlock.

### Buffer State Flags

| Flag | Bit | Meaning |
|------|-----|---------|
| `BM_LOCKED` | 22 | Buffer header spinlock is held |
| `BM_DIRTY` | 23 | Data needs writing to disk |
| `BM_VALID` | 24 | Buffer contains valid data |
| `BM_TAG_VALID` | 25 | Tag is assigned (has hash table entry) |
| `BM_IO_IN_PROGRESS` | 26 | Read or write I/O in progress |
| `BM_IO_ERROR` | 27 | Previous I/O failed |
| `BM_JUST_DIRTIED` | 28 | Dirtied since write started |
| `BM_PIN_COUNT_WAITER` | 29 | A backend is waiting for sole pin |
| `BM_CHECKPOINT_NEEDED` | 30 | Must write for current checkpoint |
| `BM_PERMANENT` | 31 | Permanent relation (not unlogged/init fork) |

## Initialization Flow

### InitBufferPool()

Source: `src/backend/storage/buffer/buf_init.c:67-151`

This function is called once during shared memory initialization in the postmaster. It performs:

1. **Allocate descriptor array**: `BufferDescriptors` via `ShmemInitStruct()`, cache-line aligned.

2. **Allocate block array**: `BufferBlocks` via `ShmemInitStruct()`, aligned to `PG_IO_ALIGN_SIZE` for direct I/O compatibility.

3. **Allocate I/O condition variables**: `BufferIOCVArray`, one per buffer.

4. **Allocate checkpoint sort array**: `CkptBufferIds`, pre-allocated to avoid runtime allocation failures during checkpoints.

5. **Initialize all buffer descriptors** (first-time only):

```c
/* From src/backend/storage/buffer/buf_init.c:118-139 */
for (i = 0; i < NBuffers; i++)
{
    BufferDesc *buf = GetBufferDescriptor(i);
    ClearBufferTag(&buf->tag);
    pg_atomic_init_u32(&buf->state, 0);
    buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;
    buf->buf_id = i;
    /* Link all buffers into freelist */
    buf->freeNext = i + 1;
    LWLockInitialize(BufferDescriptorGetContentLock(buf),
                     LWTRANCHE_BUFFER_CONTENT);
    ConditionVariableInit(BufferDescriptorGetIOCV(buf));
}
/* Terminate the linked list */
GetBufferDescriptor(NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;
```

6. **Delegate to `StrategyInitialize()`**: Initializes the hash table and freelist control structure.

7. **Initialize backend writeback context**: `WritebackContextInit(&BackendWritebackContext, &backend_flush_after)`.

### StrategyInitialize()

Source: `src/backend/storage/buffer/freelist.c:473-526`

This function initializes the partitioned hash table and the `BufferStrategyControl` shared structure:

```c
/* Initialize hash table with extra room for concurrent inserts */
InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

/* Initialize strategy control block */
StrategyControl->firstFreeBuffer = 0;
StrategyControl->lastFreeBuffer = NBuffers - 1;
pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);
StrategyControl->completePasses = 0;
pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);
StrategyControl->bgwprocno = -1;
```

The hash table is sized to `NBuffers + NUM_BUFFER_PARTITIONS` entries because `BufferAlloc()` inserts a new entry before deleting the old one, and this could happen concurrently in each partition.

## Buffer Numbering Convention

- **Shared buffers**: `Buffer` values 1..NBuffers (1-based), corresponding to `buf_id` 0..NBuffers-1 (0-based).
- **Local buffers**: `Buffer` values -1..-NLocBuffer (negative), used for temporary table pages.
- **Invalid buffer**: `Buffer` value 0 (`InvalidBuffer`).

The `BufferIsLocal()` macro tests for negative values. `BufferGetBlock()` dispatches to either `BufferBlocks` (shared) or `LocalBufferBlockPointers` (local) based on this test:

```c
/* From src/include/storage/bufmgr.h:370-379 */
static inline Block
BufferGetBlock(Buffer buffer)
{
    Assert(BufferIsValid(buffer));
    if (BufferIsLocal(buffer))
        return LocalBufferBlockPointers[-buffer - 1];
    else
        return (Block) (BufferBlocks + ((Size) (buffer - 1)) * BLCKSZ);
}
```

## Key Architectural Invariants

1. **A buffer must be pinned before being accessed**. An unpinned buffer can be reclaimed and reassigned at any time.

2. **The buffer header spinlock (`BM_LOCKED`) must be held to modify the tag, state, or wait_backend_pgprocno fields**. Exception: CAS operations on the state word are permitted without the spinlock as long as `BM_LOCKED` is verified not set.

3. **The `buf_id` field never changes after initialization**. It represents a fixed slot in the pool.

4. **The `freeNext` field is protected by `buffer_strategy_lock`**, not the buffer header spinlock.

5. **The content lock (`LWLock`) controls access to buffer data**, not the header spinlock. The header spinlock only protects metadata.

6. **Buffer descriptors must fit within 64 bytes** to avoid false sharing. The I/O condition variables are deliberately kept in a separate array to maintain this constraint.
