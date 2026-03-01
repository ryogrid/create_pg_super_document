# Buffer Access Protocol (Read Path)

## Overview

The buffer access protocol defines how PostgreSQL backends read relation pages into the shared buffer pool. The protocol handles cache hits, cache misses, victim selection, I/O coordination, and race conditions between concurrent backends requesting the same page. The primary entry point is `ReadBuffer()`, which delegates through a chain of functions culminating in `BufferAlloc()` for buffer slot management and disk I/O for data loading.

All functions in this chapter are implemented in `src/backend/storage/buffer/bufmgr.c` unless otherwise noted.

## API Hierarchy

```
ReadBuffer(rel, blockNum)
  |
  v
ReadBufferExtended(rel, forkNum, blockNum, mode, strategy)
  |
  v
ReadBuffer_common(rel, smgr, persistence, forkNum, blockNum, mode, strategy)
  |
  +-- [P_NEW] --> ExtendBufferedRel()
  |
  +-- [ZERO_AND_LOCK / ZERO_AND_CLEANUP_LOCK]
  |     --> PinBufferForBlock() + ZeroAndLockBuffer()
  |
  +-- [NORMAL / ZERO_ON_ERROR]
        --> StartReadBuffer() / StartReadBuffers()
              |
              +-- PinBufferForBlock()
              |     |
              |     +-- BufferAlloc()  [shared buffers]
              |     |     |
              |     |     +-- BufTableLookup() --> [HIT: PinBuffer, return]
              |     |     +-- GetVictimBuffer() --> [MISS: evict, insert, return]
              |     |
              |     +-- LocalBufferAlloc()  [local buffers]
              |
              +-- smgrreadv()  [if I/O needed]
              v
        WaitReadBuffers()
              |
              +-- StartBufferIO() --> smgrreadv() --> TerminateBufferIO()
```

## ReadBuffer()

Source: `src/backend/storage/buffer/bufmgr.c` (thin wrapper)

```c
Buffer ReadBuffer(Relation reln, BlockNumber blockNum)
```

Simplified entry point that reads a block from the relation's main fork using default parameters:

- Fork: `MAIN_FORKNUM`
- Mode: `RBM_NORMAL`
- Strategy: `NULL` (default replacement strategy)

This is the most commonly used function for heap and index page access.

**Returns:** A pinned `Buffer` value. The caller must eventually call `ReleaseBuffer()`.

## ReadBufferExtended()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
Buffer ReadBufferExtended(Relation reln, ForkNumber forkNum,
                          BlockNumber blockNum, ReadBufferMode mode,
                          BufferAccessStrategy strategy)
```

Full-featured buffer read interface supporting all forks, read modes, and replacement strategies.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `reln` | `Relation` | The relation to read from |
| `forkNum` | `ForkNumber` | Which fork (MAIN, FSM, VM, INIT) |
| `blockNum` | `BlockNumber` | Block number to read (or P_NEW to extend) |
| `mode` | `ReadBufferMode` | Read behavior mode (see below) |
| `strategy` | `BufferAccessStrategy` | Replacement strategy or NULL |

### ReadBufferMode Values

| Mode | Behavior |
|------|----------|
| `RBM_NORMAL` | Normal read from disk |
| `RBM_ZERO_AND_LOCK` | Do not read from disk; zero-fill the page and return it exclusive-locked |
| `RBM_ZERO_AND_CLEANUP_LOCK` | Like `RBM_ZERO_AND_LOCK` but with cleanup lock (wait for pin count = 1) |
| `RBM_ZERO_ON_ERROR` | Read from disk, but return an all-zeros page on I/O error |
| `RBM_NORMAL_NO_LOG` | Like `RBM_NORMAL` but do not log invalid page during WAL replay |

## ReadBuffer_common()

Source: `src/backend/storage/buffer/bufmgr.c:1192-1254`

This is the central unified function implementing all `ReadBuffer` variants. It handles three distinct paths:

### Path 1: P_NEW (Relation Extension)

```c
if (unlikely(blockNum == P_NEW))
{
    uint32 flags = EB_SKIP_EXTENSION_LOCK;
    if (mode == RBM_ZERO_AND_LOCK || mode == RBM_ZERO_AND_CLEANUP_LOCK)
        flags |= EB_LOCK_FIRST;
    return ExtendBufferedRel(BMR_REL(rel), forkNum, strategy, flags);
}
```

This is a backward-compatibility path; modern code should use `ExtendBufferedRel()` directly.

### Path 2: Zero-and-Lock Modes

```c
if (unlikely(mode == RBM_ZERO_AND_CLEANUP_LOCK || mode == RBM_ZERO_AND_LOCK))
{
    bool found;
    buffer = PinBufferForBlock(rel, smgr, smgr_persistence,
                               forkNum, blockNum, strategy, &found);
    ZeroAndLockBuffer(buffer, mode, found);
    return buffer;
}
```

Allocates a buffer slot and zeros its contents without performing disk I/O. Used when the caller will completely overwrite the page (e.g., heap insert into a new page).

### Path 3: Normal Read

```c
if (StartReadBuffer(&operation, &buffer, blockNum, flags))
    WaitReadBuffers(&operation);
return buffer;
```

Uses the two-phase read API: `StartReadBuffer()` allocates the buffer and determines whether I/O is needed; `WaitReadBuffers()` performs the actual disk read if required.

## BufferAlloc(): The Core Allocation Engine

Source: `src/backend/storage/buffer/bufmgr.c:1574-1752`

```c
static pg_attribute_always_inline BufferDesc *
BufferAlloc(SMgrRelation smgr, char relpersistence, ForkNumber forkNum,
            BlockNumber blockNum, BufferAccessStrategy strategy,
            bool *foundPtr, IOContext io_context)
```

This is the heart of the buffer manager. It either finds an existing buffer for the requested page or allocates a new one (evicting a victim if necessary). The function does NOT perform I/O -- it only manages buffer slot assignment.

### Step-by-Step Walkthrough

**Step 1: Prepare resources**

```c
ResourceOwnerEnlarge(CurrentResourceOwner);
ReservePrivateRefCountEntry();
```

Pre-allocate a resource owner slot and private refcount entry. This is done before acquiring any locks to avoid memory allocation under spinlock.

**Step 2: Compute tag and hash**

```c
InitBufferTag(&newTag, &smgr->smgr_rlocator.locator, forkNum, blockNum);
newHash = BufTableHashCode(&newTag);
newPartitionLock = BufMappingPartitionLock(newHash);
```

**Step 3: Hash table lookup under shared lock**

```c
LWLockAcquire(newPartitionLock, LW_SHARED);
existing_buf_id = BufTableLookup(&newTag, newHash);
```

If found (cache hit):

```c
buf = GetBufferDescriptor(existing_buf_id);
valid = PinBuffer(buf, strategy);
LWLockRelease(newPartitionLock);
*foundPtr = true;
if (!valid)
    *foundPtr = false;  /* I/O still in progress or error */
return buf;
```

Pin the buffer atomically via CAS, then release the partition lock. The buffer is pinned before releasing the lock to prevent eviction.

**Step 4: Cache miss -- get a victim buffer**

```c
LWLockRelease(newPartitionLock);
victim_buffer = GetVictimBuffer(strategy, io_context);
victim_buf_hdr = GetBufferDescriptor(victim_buffer - 1);
```

Release the shared lock (no point holding it during eviction), then call `GetVictimBuffer()` which may involve clock sweep and dirty buffer flush.

**Step 5: Insert into hash table under exclusive lock**

```c
LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
existing_buf_id = BufTableInsert(&newTag, newHash, victim_buf_hdr->buf_id);
```

**Step 6: Handle race condition**

If `BufTableInsert()` returns a non-negative value, another backend inserted the same page while we were obtaining a victim:

```c
if (existing_buf_id >= 0)
{
    UnpinBuffer(victim_buf_hdr);
    StrategyFreeBuffer(victim_buf_hdr);     /* return victim to freelist */
    existing_buf_hdr = GetBufferDescriptor(existing_buf_id);
    valid = PinBuffer(existing_buf_hdr, strategy);
    LWLockRelease(newPartitionLock);
    *foundPtr = true;
    return existing_buf_hdr;
}
```

**Step 7: Initialize the victim with the new tag**

```c
victim_buf_state = LockBufHdr(victim_buf_hdr);
Assert(BUF_STATE_GET_REFCOUNT(victim_buf_state) == 1);
Assert(!(victim_buf_state & (BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_IO_IN_PROGRESS)));

victim_buf_hdr->tag = newTag;
victim_buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
if (relpersistence == RELPERSISTENCE_PERMANENT || forkNum == INIT_FORKNUM)
    victim_buf_state |= BM_PERMANENT;

UnlockBufHdr(victim_buf_hdr, victim_buf_state);
LWLockRelease(newPartitionLock);
*foundPtr = false;
return victim_buf_hdr;
```

The buffer is returned with `BM_TAG_VALID` set but without `BM_VALID`, indicating that the data has not yet been read from disk.

## GetVictimBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:1937-2089`

```c
static Buffer GetVictimBuffer(BufferAccessStrategy strategy, IOContext io_context)
```

Finds a buffer suitable for reuse. This involves:

1. **Select a victim via `StrategyGetBuffer()`**: Returns a buffer with the header spinlock held, guaranteed unpinned (refcount = 0).

2. **Pin the victim via `PinBuffer_Locked()`**: Increments refcount while holding the spinlock.

3. **If dirty, flush to disk**:
   - Acquire shared content lock (conditional, to avoid deadlock).
   - If using a non-default strategy, check whether WAL flush is needed and consult `StrategyRejectBuffer()`.
   - Call `FlushBuffer()` to write the page to disk.
   - Release content lock.
   - Schedule writeback via `ScheduleBufferTagForWriteback()`.

4. **Invalidate the old hash table entry**:
   ```c
   if ((buf_state & BM_TAG_VALID) && !InvalidateVictimBuffer(buf_hdr))
   {
       UnpinBuffer(buf_hdr);
       goto again;  /* someone pinned or dirtied it; try another */
   }
   ```

5. **Return**: The buffer is clean, unpinned by others, and has no hash table entry. It is ready for reassignment.

### Deadlock Avoidance

The function uses `LWLockConditionalAcquire()` for the content lock when flushing a dirty victim:

```c
if (!LWLockConditionalAcquire(content_lock, LW_SHARED))
{
    UnpinBuffer(buf_hdr);
    goto again;
}
```

This prevents a deadlock scenario where two backends are both trying to evict buffers and each holds a content lock the other needs (observed in btree page split workloads).

### Strategy Rejection

For `BAS_BULKREAD` strategy, if writing the victim would require a WAL flush, `StrategyRejectBuffer()` removes the buffer from the ring and returns `true`, causing `GetVictimBuffer()` to try another buffer:

```c
if (XLogNeedsFlush(lsn) && StrategyRejectBuffer(strategy, buf_hdr, from_ring))
{
    LWLockRelease(content_lock);
    UnpinBuffer(buf_hdr);
    goto again;
}
```

## Vectorized Read API

### StartReadBuffers()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
bool StartReadBuffers(ReadBuffersOperation *operation,
                      Buffer *buffers, BlockNumber blockNum,
                      int *nblocks, int flags)
```

Initiates a multi-block read operation. It allocates buffer slots for a contiguous range of blocks and determines which blocks actually need I/O (not already valid in the pool). Returns `true` if any I/O is needed (caller must then call `WaitReadBuffers()`).

This enables scatter-gather I/O for sequential access patterns, reducing the number of system calls.

### WaitReadBuffers()

```c
void WaitReadBuffers(ReadBuffersOperation *operation)
```

Completes the I/O for buffers that were not already valid. For each buffer needing I/O:

1. Calls `StartBufferIO()` to claim the I/O lock.
2. Calls `smgrreadv()` for the actual disk read (vectorized).
3. Validates the page with `PageIsVerifiedExtended()`.
4. Calls `TerminateBufferIO()` with `BM_VALID` flag to mark the buffer valid and wake waiters.

## PinBufferForBlock()

Source: `src/backend/storage/buffer/bufmgr.c` (line ~1082)

```c
static pg_attribute_always_inline Buffer
PinBufferForBlock(Relation rel, SMgrRelation smgr, char smgr_persistence,
                  ForkNumber forkNum, BlockNumber blockNum,
                  BufferAccessStrategy strategy, bool *foundPtr)
```

Internal routing function that dispatches to either `BufferAlloc()` (for shared buffers) or `LocalBufferAlloc()` (for temporary tables).

## Key Invariants

1. **A buffer returned by `ReadBuffer()` is always pinned**. The caller must release it with `ReleaseBuffer()` or `UnlockReleaseBuffer()`.

2. **A buffer returned with `*foundPtr = true` may or may not be `BM_VALID`**. If the buffer is in the pool but still being read by another backend, `PinBuffer()` returns `false` for validity. The caller must handle I/O completion.

3. **No I/O occurs while holding the partition lock**. The lock is released before any disk operations.

4. **The hash table insert uses `HASH_ENTER` semantics**, which atomically handles the case where another backend inserts the same tag concurrently.

5. **Resource owner tracking ensures pins are released at transaction end**. Every `PinBuffer()` call registers with the current resource owner. Leaked pins are detected and released with a warning during transaction cleanup.

## Performance Characteristics

- **Cache hit**: Fast path through `BufTableLookup()` (shared partition lock) + `PinBuffer()` (lock-free CAS). No spinlocks or exclusive locks needed.

- **Cache miss, clean victim**: One exclusive partition lock + hash insert + tag assignment. No disk I/O for eviction.

- **Cache miss, dirty victim**: Additional disk write (`FlushBuffer()`) plus potentially a WAL flush (`XLogFlush()`). This is the most expensive path and is what the background writer aims to prevent.

- **Race condition (concurrent miss for same page)**: The losing backend unpins its victim, returns it to the freelist, and pins the winner's buffer. No wasted I/O occurs -- the page is read only once.
