# Buffer Access Protocol (Read Path)

[<< Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) | [Index](index.md) | [Next: Page Concurrency Control >>](06_page_concurrency_control.md)

---

## Overview

The buffer access protocol defines how PostgreSQL backends read relation pages into the shared buffer pool. The protocol handles cache hits, cache misses, victim selection, I/O coordination, and race conditions between concurrent backends requesting the same page. The primary entry point is `ReadBuffer()`, which delegates through a chain of functions culminating in [BufferAlloc()](03_buffer_pool_architecture.md) for buffer slot management and disk I/O for data loading.

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
              |     +-- LocalBufferAlloc()  [local buffers]
              |
              +-- smgrreadv()  [if I/O needed]
              v
        WaitReadBuffers()
              |
              +-- StartBufferIO() --> smgrreadv() --> TerminateBufferIO()
```

See the full flow diagram: [readbuffer_flow.mermaid](../diagrams/readbuffer_flow.mermaid)

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

```c
Buffer ReadBufferExtended(Relation reln, ForkNumber forkNum,
                          BlockNumber blockNum, ReadBufferMode mode,
                          BufferAccessStrategy strategy)
```

Full-featured buffer read interface supporting all forks, read modes, and [replacement strategies](07_buffer_replacement_policy.md).

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
| `RBM_ZERO_AND_CLEANUP_LOCK` | Like `RBM_ZERO_AND_LOCK` but with [cleanup lock](06_page_concurrency_control.md) (wait for pin count = 1) |
| `RBM_ZERO_ON_ERROR` | Read from disk, but return an all-zeros page on I/O error |
| `RBM_NORMAL_NO_LOG` | Like `RBM_NORMAL` but do not log invalid page during WAL replay |

## ReadBuffer_common()

Source: `src/backend/storage/buffer/bufmgr.c:1192`

This is the central unified function implementing all `ReadBuffer` variants. It handles three distinct paths:

### Path 1: P_NEW (Relation Extension)

```c
if (unlikely(blockNum == P_NEW))
    return ExtendBufferedRel(BMR_REL(rel), forkNum, strategy, flags);
```

This is a backward-compatibility path; modern code should use `ExtendBufferedRel()` directly.

### Path 2: Zero-and-Lock Modes

```c
if (unlikely(mode == RBM_ZERO_AND_CLEANUP_LOCK || mode == RBM_ZERO_AND_LOCK))
{
    buffer = PinBufferForBlock(rel, smgr, ..., &found);
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

Source: `src/backend/storage/buffer/bufmgr.c:1594`

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

Pre-allocate a resource owner slot and private refcount entry before acquiring any locks.

**Step 2: Compute tag and hash**

```c
InitBufferTag(&newTag, &smgr->smgr_rlocator.locator, forkNum, blockNum);
newHash = BufTableHashCode(&newTag);
newPartitionLock = BufMappingPartitionLock(newHash);
```

See [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) for hash table details.

**Step 3: Hash table lookup under shared lock**

```c
LWLockAcquire(newPartitionLock, LW_SHARED);
existing_buf_id = BufTableLookup(&newTag, newHash);
```

If found (cache hit): pin via CAS, release partition lock, return. This is the fast path.

**Step 4: Cache miss -- get a victim buffer**

```c
LWLockRelease(newPartitionLock);
victim_buffer = GetVictimBuffer(strategy, io_context);
```

Release the shared lock (no point holding it during eviction), then call `GetVictimBuffer()`.

**Step 5: Insert into hash table under exclusive lock**

```c
LWLockAcquire(newPartitionLock, LW_EXCLUSIVE);
existing_buf_id = BufTableInsert(&newTag, newHash, victim_buf_hdr->buf_id);
```

**Step 6: Handle race condition**

If another backend inserted the same page while we were obtaining a victim:

```c
if (existing_buf_id >= 0)
{
    UnpinBuffer(victim_buf_hdr);
    StrategyFreeBuffer(victim_buf_hdr);     /* return victim to freelist */
    existing_buf_hdr = GetBufferDescriptor(existing_buf_id);
    valid = PinBuffer(existing_buf_hdr, strategy);
    LWLockRelease(newPartitionLock);
    return existing_buf_hdr;
}
```

**Step 7: Initialize the victim with the new tag**

The buffer is returned with `BM_TAG_VALID` set but without `BM_VALID`, indicating that the data has not yet been read from disk.

## GetVictimBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:1937`

```c
static Buffer GetVictimBuffer(BufferAccessStrategy strategy, IOContext io_context)
```

Finds a buffer suitable for reuse:

1. **Select a victim via [StrategyGetBuffer()](07_buffer_replacement_policy.md)**: Returns a buffer with the header spinlock held, guaranteed unpinned.

2. **Pin the victim**: Increments refcount while holding the spinlock.

3. **If dirty, flush to disk**:
   - Acquire shared content lock (conditional, to avoid deadlock).
   - If using a non-default strategy, check whether WAL flush is needed and consult [StrategyRejectBuffer()](07_buffer_replacement_policy.md).
   - Call [FlushBuffer()](09_dirty_buffer_and_writeback.md) to write the page to disk.
   - Schedule writeback via `ScheduleBufferTagForWriteback()`.

4. **Invalidate the old hash table entry**:
   ```c
   if ((buf_state & BM_TAG_VALID) && !InvalidateVictimBuffer(buf_hdr))
   {
       UnpinBuffer(buf_hdr);
       goto again;  /* someone pinned or dirtied it; try another */
   }
   ```

5. **Return**: The buffer is clean, unpinned by others, and has no hash table entry.

### Deadlock Avoidance

The function uses `LWLockConditionalAcquire()` for the content lock when flushing a dirty victim, preventing deadlock when two backends are both trying to evict buffers:

```c
if (!LWLockConditionalAcquire(content_lock, LW_SHARED))
{
    UnpinBuffer(buf_hdr);
    goto again;
}
```

See [Page Concurrency Control](06_page_concurrency_control.md) for lock ordering rules.

## Vectorized Read API

### StartReadBuffers()

```c
bool StartReadBuffers(ReadBuffersOperation *operation,
                      Buffer *buffers, BlockNumber blockNum,
                      int *nblocks, int flags)
```

Initiates a multi-block read operation. Allocates buffer slots for a contiguous range of blocks and determines which need I/O. Returns `true` if any I/O is needed.

This enables scatter-gather I/O for sequential access patterns, reducing the number of system calls.

### WaitReadBuffers()

```c
void WaitReadBuffers(ReadBuffersOperation *operation)
```

Completes the I/O for buffers not already valid:

1. Calls [StartBufferIO()](06_page_concurrency_control.md) to claim the I/O lock.
2. Calls `smgrreadv()` for the actual disk read (vectorized).
3. Validates the page with [PageIsVerifiedExtended()](08_page_layout_and_types.md).
4. Calls [TerminateBufferIO()](06_page_concurrency_control.md) with `BM_VALID` flag to mark valid and wake waiters.

## PinBufferForBlock()

Source: `src/backend/storage/buffer/bufmgr.c` (line ~1082)

```c
static pg_attribute_always_inline Buffer
PinBufferForBlock(Relation rel, SMgrRelation smgr, char smgr_persistence,
                  ForkNumber forkNum, BlockNumber blockNum,
                  BufferAccessStrategy strategy, bool *foundPtr)
```

Internal routing function that dispatches to either `BufferAlloc()` (for shared buffers) or [LocalBufferAlloc()](13_local_buffers.md) (for temporary tables).

## Key Invariants

1. **A buffer returned by `ReadBuffer()` is always pinned**. The caller must release it with `ReleaseBuffer()` or `UnlockReleaseBuffer()`.

2. **No I/O occurs while holding the partition lock**. The lock is released before any disk operations.

3. **The hash table insert uses `HASH_ENTER` semantics**, which atomically handles the case where another backend inserts the same tag concurrently.

4. **Resource owner tracking ensures pins are released at transaction end**. Every `PinBuffer()` call registers with the current resource owner. Leaked pins are detected and released with a warning during transaction cleanup.

## Performance Characteristics

- **Cache hit**: Fast path through `BufTableLookup()` (shared partition lock) + `PinBuffer()` (lock-free CAS). No spinlocks or exclusive locks needed.

- **Cache miss, clean victim**: One exclusive partition lock + hash insert + tag assignment. No disk I/O for eviction.

- **Cache miss, dirty victim**: Additional disk write ([FlushBuffer()](09_dirty_buffer_and_writeback.md)) plus potentially a [WAL flush](10_wal_integration.md) (`XLogFlush()`). This is the most expensive path and is what the [background writer](09_dirty_buffer_and_writeback.md) aims to prevent.

- **Race condition (concurrent miss for same page)**: The losing backend unpins its victim, returns it to the freelist, and pins the winner's buffer. No wasted I/O occurs -- the page is read only once.

---

[<< Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) | [Index](index.md) | [Next: Page Concurrency Control >>](06_page_concurrency_control.md)
