# Dirty Buffer Management and Write-Back

[<< Page Layout and Types](08_page_layout_and_types.md) | [Index](index.md) | [Next: WAL Integration >>](10_wal_integration.md)

---

## Overview

Dirty buffer management encompasses the mechanisms by which modified pages are tracked, scheduled for write-back, and ultimately written to persistent storage. PostgreSQL marks buffers dirty when their contents change, and writes them back to disk through three pathways: checkpoint (periodic bulk flush), background writer (proactive cleaning), and backend eviction (reactive cleaning when a buffer must be reused). All three pathways enforce the [WAL-before-data rule](10_wal_integration.md) and use the writeback context to coalesce I/O requests.

See diagram: [writeback_pipeline.mermaid](../diagrams/writeback_pipeline.mermaid)

## Marking Buffers Dirty

### MarkBufferDirty()

Source: `src/backend/storage/buffer/bufmgr.c:2520`

```c
void MarkBufferDirty(Buffer buffer)
```

Marks a buffer as dirty, indicating its contents differ from the on-disk version. The actual write to disk is deferred.

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
- `BM_JUST_DIRTIED` is used to detect re-dirtying during write-out (see the `BM_JUST_DIRTIED` protocol below).
- If the buffer was not previously dirty, updates vacuum cost accounting.

### MarkBufferDirtyHint()

```c
void MarkBufferDirtyHint(Buffer buffer, bool buffer_std)
```

A lighter-weight version for hint bit updates (e.g., setting `HEAP_XMIN_COMMITTED`). Unlike `MarkBufferDirty()`:

- Does NOT require an exclusive content lock (shared lock is sufficient).
- May skip WAL logging entirely for non-permanent buffers.
- For permanent buffers with data checksums enabled, calls `XLogSaveBufferForHint()` to generate a [full-page write](10_wal_integration.md) in WAL.

See [WAL Integration](10_wal_integration.md) and [Deep Dives](15_deep_dives.md) for more on hint bit WAL handling.

## FlushBuffer(): The Write Engine

Source: `src/backend/storage/buffer/bufmgr.c:3773`

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

Sets `BM_IO_IN_PROGRESS`. See [Page Concurrency Control](06_page_concurrency_control.md).

**Step 2: Read page LSN under header lock**

```c
buf_state = LockBufHdr(buf);
recptr = BufferGetLSN(buf);
buf_state &= ~BM_JUST_DIRTIED;
UnlockBufHdr(buf, buf_state);
```

The LSN must be read while holding the header spinlock because hint bit updates by other backends could be modifying the page concurrently under shared content lock.

**Step 3: WAL-before-data enforcement**

```c
if (buf_state & BM_PERMANENT)
    XLogFlush(recptr);
```

For permanent relations, flush [WAL](10_wal_integration.md) up to the page's LSN. Non-permanent (unlogged) buffers skip this step.

**Step 4: Compute checksum and write**

```c
bufToWrite = PageSetChecksumCopy((Page) bufBlock, buf->tag.blockNum);
smgrwrite(reln, BufTagGetForkNum(&buf->tag), buf->tag.blockNum,
          bufToWrite, false);
```

[PageSetChecksumCopy()](08_page_layout_and_types.md) returns a copy of the page with the checksum set. A copy is mandatory because hint bit updates can occur concurrently.

**Step 5: Terminate I/O and possibly clear dirty flag**

```c
TerminateBufferIO(buf, true, 0, true);
```

If `BM_JUST_DIRTIED` is not set, clears `BM_DIRTY` and `BM_CHECKPOINT_NEEDED`. If the buffer was re-dirtied during the write, the dirty flag is preserved.

## Background Writer

### BgBufferSync()

Source: `src/backend/storage/buffer/bufmgr.c:3165`

```c
bool BgBufferSync(WritebackContext *wb_context)
```

The background writer's main buffer-cleaning function, called periodically (every `bgwriter_delay` ms, default 200ms). It proactively writes dirty buffers that are likely to be evicted soon, reducing synchronous writes by backend processes during buffer allocation.

**Adaptive Algorithm:**

1. **Query the clock sweep position** via [StrategySyncStart()](07_buffer_replacement_policy.md).
2. **Compute strategy delta**: How far the clock sweep has advanced since last call.
3. **Track moving averages**: `smoothed_alloc` (allocation rate), `smoothed_density` (scans-per-allocation).
4. **Estimate upcoming demand**: `upcoming_alloc_est = smoothed_alloc * bgwriter_lru_multiplier`.
5. **Scan ahead of the clock sweep**:

```c
while (num_to_scan > 0 && reusable_buffers < upcoming_alloc_est)
{
    int sync_state = SyncOneBuffer(next_to_clean, true, wb_context);
    if (sync_state & BUF_WRITTEN)
        reusable_buffers++;
}
```

Stops when enough reusable buffers exist or `bgwriter_lru_maxpages` (default 100) writes have been performed.

6. **Hibernation**: Returns `true` if the clock sweep has not advanced, signaling the bgwriter can enter low-power mode.

### SyncOneBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:3460`

```c
static int SyncOneBuffer(int buf_id, bool skip_recently_used,
                         WritebackContext *wb_context)
```

Processes a single buffer for potential write-out. Locks the header, checks state, pins the buffer, acquires shared content lock, and calls `FlushBuffer()`.

**Returns:** Bitmask of `BUF_WRITTEN` and `BUF_REUSABLE`.

## Checkpoint Buffer Flush

### BufferSync()

Source: `src/backend/storage/buffer/bufmgr.c:2890`

```c
static void BufferSync(int flags)
```

Writes all dirty buffers as part of a checkpoint. This is the most I/O-intensive operation in the buffer manager.

**Phase 1: Scan and Mark**

Scan all NBuffers, marking dirty buffers with `BM_CHECKPOINT_NEEDED`.

**Phase 2: Sort by Tablespace**

```c
sort_checkpoint_bufferids(CkptBufferIds, num_to_scan);
```

Sorting enables sequential I/O patterns within each tablespace.

**Phase 3: Interleaved Write with Tablespace Balancing**

Uses a min-heap over tablespace progress to interleave writes. Each tablespace gets a proportional `progress_slice`, ensuring balanced I/O distribution. `CheckpointWriteDelay()` throttles I/O to spread writes over `checkpoint_completion_target` (default 0.9) of the checkpoint interval.

**Phase 4: Issue Pending Writebacks**

```c
IssuePendingWritebacks(&wb_context, IOCONTEXT_NORMAL);
```

## Writeback Context

The writeback context coalesces adjacent write-back requests to advise the kernel to flush dirty pages.

### WritebackContext

Source: `src/include/storage/buf_internals.h`

```c
typedef struct WritebackContext
{
    int        *max_pending;
    int         nr_pending;
    PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;
```

### ScheduleBufferTagForWriteback()

Adds a buffer tag to the pending writeback list. When the list reaches `*max_pending` entries, triggers `IssuePendingWritebacks()`.

### IssuePendingWritebacks()

Sorts pending requests, coalesces adjacent block ranges, and calls [smgrwriteback()](11_storage_manager.md) for each contiguous range. This translates to `sync_file_range()` or `posix_fadvise(POSIX_FADV_DONTNEED)` on the platform. See [Data Movement and Durability](12_data_movement_and_durability.md).

### GUC Parameters

| GUC | Default | Controls |
|-----|---------|----------|
| `checkpoint_flush_after` | 256 KB (32 pages) | Writeback coalescing limit for checkpointer |
| `bgwriter_flush_after` | 512 KB (64 pages) | Writeback coalescing limit for background writer |
| `backend_flush_after` | 0 (disabled) | Writeback coalescing limit for backend processes |

See [GUC Parameters Appendix](appendix_guc_parameters.md) for all related parameters.

## The BM_JUST_DIRTIED Protocol

A subtle race condition exists between writing a buffer and concurrent dirtying:

1. `FlushBuffer()` reads the page LSN and clears `BM_JUST_DIRTIED`.
2. Another backend modifies the page (sets `BM_DIRTY | BM_JUST_DIRTIED`).
3. `FlushBuffer()` writes the (now stale) page copy to disk.
4. `TerminateBufferIO()` checks: if `BM_JUST_DIRTIED` is set, it preserves `BM_DIRTY`.

Without `BM_JUST_DIRTIED`, the buffer would be incorrectly marked clean after step 4, and the new modification would be lost.

```c
/* From TerminateBufferIO() */
if (clear_dirty && !(buf_state & BM_JUST_DIRTIED))
    buf_state &= ~(BM_DIRTY | BM_CHECKPOINT_NEEDED);
```

See [Deep Dives](15_deep_dives.md) for a detailed analysis of this protocol.

---

[<< Page Layout and Types](08_page_layout_and_types.md) | [Index](index.md) | [Next: WAL Integration >>](10_wal_integration.md)
