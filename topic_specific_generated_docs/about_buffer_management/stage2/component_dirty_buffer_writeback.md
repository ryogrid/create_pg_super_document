# Dirty Buffer Management and Write-Back

## Overview

Dirty buffer management encompasses the mechanisms by which modified pages are tracked, scheduled for write-back, and ultimately written to persistent storage. PostgreSQL marks buffers dirty when their contents change, and writes them back to disk through three pathways: checkpoint (periodic bulk flush), background writer (proactive cleaning), and backend eviction (reactive cleaning when a buffer must be reused). All three pathways enforce the WAL-before-data rule and use the writeback context to coalesce I/O requests.

## Marking Buffers Dirty

### MarkBufferDirty()

Source: `src/backend/storage/buffer/bufmgr.c:2510-2567`

```c
void MarkBufferDirty(Buffer buffer)
```

Marks a buffer as dirty, indicating its contents differ from the on-disk version. The actual write to disk is deferred to a later time (checkpoint, background writer, or eviction).

**Preconditions:** Buffer must be pinned and exclusive-locked.

**Implementation:**

```c
old_buf_state = pg_atomic_read_u32(&bufHdr->state);
for (;;)
{
    if (old_buf_state & BM_LOCKED)
        old_buf_state = WaitBufHdrUnlocked(bufHdr);

    buf_state = old_buf_state;
    buf_state |= BM_DIRTY | BM_JUST_DIRTIED;

    if (pg_atomic_compare_exchange_u32(&bufHdr->state, &old_buf_state, buf_state))
        break;
}
```

Key points:
- Uses lock-free CAS to set `BM_DIRTY` and `BM_JUST_DIRTIED` atomically.
- `BM_JUST_DIRTIED` is used to detect re-dirtying during write-out (see `TerminateBufferIO()`).
- If the buffer was not previously dirty, updates vacuum cost accounting (`VacuumPageDirty`, `VacuumCostBalance`).

### MarkBufferDirtyHint()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
void MarkBufferDirtyHint(Buffer buffer, bool buffer_std)
```

A lighter-weight version for hint bit updates (e.g., setting `HEAP_XMIN_COMMITTED`). Unlike `MarkBufferDirty()`:

- Does NOT require an exclusive content lock (shared lock is sufficient).
- May skip WAL logging entirely for non-permanent buffers.
- For permanent buffers with data checksums enabled, calls `XLogSaveBufferForHint()` to generate a full-page write in WAL (necessary because hint bit changes would invalidate the page checksum, and without WAL protection, a torn page could leave an incorrect checksum on disk).

```c
if (XLogHintBitIsNeeded())
{
    /* Need WAL protection for checksum consistency */
    XLogSaveBufferForHint(buffer, buffer_std);
}
```

## FlushBuffer(): The Write Engine

Source: `src/backend/storage/buffer/bufmgr.c:3753-3901`

```c
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln,
                        IOObject io_object, IOContext io_context)
```

Physically writes a buffer's contents to the kernel page cache (not directly to disk). This is the central write function used by all three write-back pathways.

**Preconditions:** Caller holds a pin and a shared content lock on the buffer.

### Step-by-Step Flow

**Step 1: Claim I/O lock**

```c
if (!StartBufferIO(buf, false, false))
    return;  /* someone else already flushed it */
```

Sets `BM_IO_IN_PROGRESS`. If another backend is already writing this buffer, `StartBufferIO()` waits and then returns `false` (the buffer is already clean).

**Step 2: Read page LSN under header lock**

```c
buf_state = LockBufHdr(buf);
recptr = BufferGetLSN(buf);
buf_state &= ~BM_JUST_DIRTIED;
UnlockBufHdr(buf, buf_state);
```

The LSN must be read while holding the header spinlock because the caller only has a shared content lock (hint bit updates by other backends could be modifying the page concurrently). Clearing `BM_JUST_DIRTIED` allows detection of re-dirtying during the write.

**Step 3: WAL-before-data enforcement**

```c
if (buf_state & BM_PERMANENT)
    XLogFlush(recptr);
```

For permanent relations, flush WAL up to the page's LSN before writing the page. This is THE enforcement point for the fundamental WAL rule: "thou shalt write xlog before data." Non-permanent (unlogged) buffers skip this step.

**Step 4: Compute checksum and write**

```c
bufToWrite = PageSetChecksumCopy((Page) bufBlock, buf->tag.blockNum);
smgrwrite(reln, BufTagGetForkNum(&buf->tag), buf->tag.blockNum,
          bufToWrite, false);
```

`PageSetChecksumCopy()` returns a copy of the page with the checksum set. A copy is mandatory because hint bit updates can occur concurrently under shared lock -- modifying the live page's checksum field could produce an inconsistent write.

**Step 5: Terminate I/O and possibly clear dirty flag**

```c
TerminateBufferIO(buf, true, 0, true);
```

`TerminateBufferIO()` clears `BM_IO_IN_PROGRESS` and, if `BM_JUST_DIRTIED` is not set, also clears `BM_DIRTY` and `BM_CHECKPOINT_NEEDED`. If the buffer was re-dirtied during the write (by another backend setting hint bits), `BM_JUST_DIRTIED` will be set and the dirty flag is preserved.

## Background Writer

### BgBufferSync()

Source: `src/backend/storage/buffer/bufmgr.c:3165-3458`

```c
bool BgBufferSync(WritebackContext *wb_context)
```

The background writer's main buffer-cleaning function, called periodically (every `bgwriter_delay` ms, default 200ms). It proactively writes dirty buffers that are likely to be evicted soon, reducing the frequency of synchronous writes by backend processes during buffer allocation.

**Adaptive Algorithm:**

1. **Query the clock sweep position** via `StrategySyncStart()`:
   ```c
   strategy_buf_id = StrategySyncStart(&strategy_passes, &recent_alloc);
   ```
   This returns the current clock hand position and the number of buffer allocations since the last call.

2. **Compute strategy delta**: How far the clock sweep has advanced since last call.

3. **Track moving averages**:
   - `smoothed_alloc`: Exponential moving average of allocation rate. Fast attack, slow decay -- immediately follows increases.
   - `smoothed_density`: Moving average of scans-per-allocation (inverse of reusable buffer density).

4. **Estimate upcoming demand**:
   ```c
   upcoming_alloc_est = (int)(smoothed_alloc * bgwriter_lru_multiplier);
   ```
   The `bgwriter_lru_multiplier` GUC (default 2.0) scales the estimate for safety margin.

5. **Scan ahead of the clock sweep**:
   ```c
   while (num_to_scan > 0 && reusable_buffers < upcoming_alloc_est)
   {
       int sync_state = SyncOneBuffer(next_to_clean, true, wb_context);
       if (sync_state & BUF_WRITTEN)
           reusable_buffers++;
       else if (sync_state & BUF_REUSABLE)
           reusable_buffers++;
   }
   ```
   Stops when enough reusable buffers exist to meet the estimated demand, or when `bgwriter_lru_maxpages` (default 100) writes have been performed.

6. **Hibernation**: Returns `true` if the clock sweep has not advanced and no allocations have occurred, signaling that the bgwriter process can enter low-power hibernation mode.

### SyncOneBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:3460-3538`

```c
static int SyncOneBuffer(int buf_id, bool skip_recently_used,
                         WritebackContext *wb_context)
```

Processes a single buffer for potential write-out:

1. Lock the buffer header and check state.
2. If `skip_recently_used` and buffer is pinned or has usage count > 0, skip it.
3. If buffer is not dirty or not valid, skip it.
4. Pin the buffer, acquire shared content lock, and call `FlushBuffer()`.
5. Schedule the tag for writeback.

**Returns:** Bitmask of `BUF_WRITTEN` (buffer was written) and `BUF_REUSABLE` (buffer has refcount=0 and usagecount=0).

## Checkpoint Buffer Flush

### CheckPointBuffers()

Source: `src/backend/storage/buffer/bufmgr.c` (wrapper)

```c
void CheckPointBuffers(int flags)
```

Entry point for checkpoint buffer flushing. Delegates to `BufferSync()`.

### BufferSync()

Source: `src/backend/storage/buffer/bufmgr.c:2890-3163`

```c
static void BufferSync(int flags)
```

Writes all dirty buffers as part of a checkpoint. This is the most I/O-intensive operation in the buffer manager.

**Phase 1: Scan and Mark**

Scan all NBuffers, marking dirty buffers with `BM_CHECKPOINT_NEEDED`:

```c
for (buf_id = 0; buf_id < NBuffers; buf_id++)
{
    buf_state = LockBufHdr(bufHdr);
    if ((buf_state & mask) == mask)
    {
        buf_state |= BM_CHECKPOINT_NEEDED;
        CkptBufferIds[num_to_scan++] = ...;  /* record for sorting */
    }
    UnlockBufHdr(bufHdr, buf_state);
}
```

Only buffers matching the `mask` are marked. For normal checkpoints, `mask = BM_DIRTY | BM_PERMANENT`. For shutdown checkpoints, all dirty buffers are included.

**Phase 2: Sort by Tablespace**

```c
sort_checkpoint_bufferids(CkptBufferIds, num_to_scan);
```

Sorting by tablespace enables sequential I/O patterns within each tablespace and allows balanced write distribution.

**Phase 3: Interleaved Write with Tablespace Balancing**

Uses a min-heap over tablespace progress to interleave writes:

```c
while (!binaryheap_empty(ts_heap))
{
    CkptTsStatus *ts_stat = binaryheap_first(ts_heap);
    buf_id = CkptBufferIds[ts_stat->index].buf_id;

    if (pg_atomic_read_u32(&bufHdr->state) & BM_CHECKPOINT_NEEDED)
    {
        if (SyncOneBuffer(buf_id, false, &wb_context) & BUF_WRITTEN)
            num_written++;
    }

    ts_stat->progress += ts_stat->progress_slice;
    ts_stat->index++;

    /* Rebalance heap or remove exhausted tablespace */
    if (ts_stat->num_scanned == ts_stat->num_to_scan)
        binaryheap_remove_first(ts_heap);
    else
        binaryheap_replace_first(ts_heap, ...);

    /* Throttle I/O */
    CheckpointWriteDelay(flags, (double) num_processed / num_to_scan);
}
```

Each tablespace gets a `progress_slice = total_buffers / buffers_in_this_tablespace`, ensuring that progress through each tablespace is proportional to its share of the total dirty buffers. This prevents one tablespace from monopolizing I/O bandwidth.

**Phase 4: Issue Pending Writebacks**

```c
IssuePendingWritebacks(&wb_context, IOCONTEXT_NORMAL);
```

## Writeback Context

The writeback context coalesces adjacent write-back requests to advise the kernel to flush dirty pages to persistent storage.

### WritebackContext

Source: `src/include/storage/buf_internals.h:297-307`

```c
typedef struct WritebackContext
{
    int        *max_pending;       /* pointer to GUC controlling max coalescing */
    int         nr_pending;        /* current number of pending requests */
    PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;
```

### ScheduleBufferTagForWriteback()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
void ScheduleBufferTagForWriteback(WritebackContext *wb_context,
                                   IOContext io_context, BufferTag *tag)
```

Adds a buffer tag to the pending writeback list. When the list reaches `*max_pending` entries, triggers `IssuePendingWritebacks()`.

### IssuePendingWritebacks()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
void IssuePendingWritebacks(WritebackContext *wb_context, IOContext io_context)
```

Sorts pending requests by `(spcOid, dbOid, relNumber, forkNum, blockNum)`, coalesces adjacent block ranges, and calls `smgrwriteback()` for each contiguous range. This translates to `posix_fadvise(POSIX_FADV_DONTNEED)` or `sync_file_range()` depending on the platform, advising the kernel to write the specified pages to disk.

### GUC Parameters

| GUC | Default | Controls |
|-----|---------|----------|
| `checkpoint_flush_after` | 256 KB (32 pages) | Writeback coalescing limit for checkpointer |
| `bgwriter_flush_after` | 512 KB (64 pages) | Writeback coalescing limit for background writer |
| `backend_flush_after` | 0 (disabled) | Writeback coalescing limit for backend processes |

## Write Ordering and the BM_JUST_DIRTIED Protocol

A subtle race condition exists between writing a buffer and concurrent dirtying:

1. `FlushBuffer()` reads the page LSN and clears `BM_JUST_DIRTIED`.
2. Another backend modifies the page (sets `BM_DIRTY | BM_JUST_DIRTIED`).
3. `FlushBuffer()` writes the (now stale) page copy to disk.
4. `TerminateBufferIO()` checks: if `BM_JUST_DIRTIED` is set, it preserves `BM_DIRTY`.

Without `BM_JUST_DIRTIED`, the buffer would be incorrectly marked clean after step 4, and the new modification would be lost. The protocol ensures that any modification occurring after the write begins is detected and the buffer remains marked dirty for a future write.

```c
/* From TerminateBufferIO(), bufmgr.c:5595-5597 */
if (clear_dirty && !(buf_state & BM_JUST_DIRTIED))
    buf_state &= ~(BM_DIRTY | BM_CHECKPOINT_NEEDED);
```
