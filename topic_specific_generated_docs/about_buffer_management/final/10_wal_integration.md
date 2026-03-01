# WAL Integration and LSN Management

[<< Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) | [Index](index.md) | [Next: Storage Manager >>](11_storage_manager.md)

---

## Overview

The WAL (Write-Ahead Logging) subsystem and the buffer manager are tightly coupled through the "WAL-before-data" rule: no dirty data page may be written to disk until the WAL record describing the change has been flushed to persistent storage. This rule is the foundation of PostgreSQL's crash recovery guarantee. The buffer manager enforces this rule through the `pd_lsn` field in every [page header](08_page_layout_and_types.md) and the `XLogFlush()` call in [FlushBuffer()](09_dirty_buffer_and_writeback.md).

## The Fundamental Rule

From the comment in `src/include/storage/bufpage.h`:

> The LSN is used by the buffer manager to enforce the basic rule of WAL: "thou shalt write xlog before data". A dirty buffer cannot be dumped to disk until xlog has been flushed at least as far as the page's LSN.

The enforcement mechanism:

```
Page modified -> PageSetLSN(page, lsn)  [sets pd_lsn to WAL record's LSN]
                        |
                        v
Buffer evicted -> FlushBuffer()
                    |
                    +-> recptr = PageGetLSN(page)
                    +-> XLogFlush(recptr)     [flush WAL to disk up to this LSN]
                    +-> smgrwrite(page)        [only NOW write the data page]
```

## Page LSN Operations

### PageGetLSN() / PageSetLSN()

Source: `src/include/storage/bufpage.h`

```c
static inline XLogRecPtr PageGetLSN(Page page)
{
    return PageXLogRecPtrGet(((PageHeader) page)->pd_lsn);
}

static inline void PageSetLSN(Page page, XLogRecPtr lsn)
{
    PageXLogRecPtrSet(((PageHeader) page)->pd_lsn, lsn);
}
```

`PageSetLSN()` is called after `XLogInsert()` to record the LSN of the WAL record that describes the page modification. The caller must hold an exclusive [content lock](06_page_concurrency_control.md) on the buffer.

### BufferGetLSNAtomic()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
XLogRecPtr BufferGetLSNAtomic(Buffer buffer)
```

Reads the page LSN without requiring a content lock. Uses the buffer header spinlock to ensure atomic reading of the two 32-bit halves:

```c
buf_state = LockBufHdr(bufHdr);
lsn = PageGetLSN(page);
UnlockBufHdr(bufHdr, buf_state);
```

Used when the caller needs the LSN but does not hold a content lock (e.g., during eviction decisions in [GetVictimBuffer()](05_buffer_access_protocol.md)).

## WAL-Before-Data Enforcement in FlushBuffer()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
/* Read LSN under header lock (shared content lock is insufficient) */
buf_state = LockBufHdr(buf);
recptr = BufferGetLSN(buf);
buf_state &= ~BM_JUST_DIRTIED;
UnlockBufHdr(buf, buf_state);

/* Force XLOG flush up to buffer's LSN */
if (buf_state & BM_PERMANENT)
    XLogFlush(recptr);
```

### Why the Header Lock is Needed

The LSN is read under the header spinlock (not the content lock) because [FlushBuffer()](09_dirty_buffer_and_writeback.md) only holds a shared content lock. Under a shared lock, other backends can update hint bits, which could result in a concurrent modification to `pd_lsn`. The header spinlock provides the necessary atomicity for reading the two 32-bit halves of the 64-bit LSN.

### Unlogged Relations Exception

Unlogged relations skip the WAL flush because:
1. They are lost after a crash anyway, so WAL protection is unnecessary.
2. Some unlogged relations use "fake" LSNs from `GetFakeLSNForUnloggedRel()`. Attempting to flush WAL to such a location would fail.

## XLogFlush()

Source: `src/backend/access/transam/xlog.c`

```c
void XLogFlush(XLogRecPtr record)
```

Ensures WAL is flushed to persistent storage up to (at least) the specified LSN. This may involve:

1. Writing WAL buffers from shared memory to the WAL segment files.
2. Calling `fsync()` on the WAL segment files.
3. Group commit optimization: if another backend is already flushing past our target LSN, wait for that flush to complete rather than issuing a redundant fsync.

This function can be expensive (involves disk I/O), which is why the [background writer](09_dirty_buffer_and_writeback.md) and checkpoint process proactively clean dirty buffers to avoid forcing backends to call `XLogFlush()` during buffer eviction.

## Full-Page Writes

### The Torn Page Problem

If a system crash occurs while the OS is writing an 8 KB page to disk, the result can be a "torn page" -- a page that is partially old data and partially new data. WAL replay cannot fix this because it applies changes incrementally to existing pages, and the base page is corrupted.

### Solution: Full-Page Images

The first time a page is modified after a checkpoint, PostgreSQL includes a full copy of the entire page (a "full-page image" or FPI) in the WAL record. During recovery, the full-page image replaces the on-disk page entirely, eliminating the torn-page risk.

The buffer manager's role:
- After a checkpoint completes, `BM_CHECKPOINT_NEEDED` is cleared on written buffers.
- The first modification to a page after checkpoint sets a new LSN. The WAL subsystem determines whether an FPI is needed based on whether this is the first modification since the last checkpoint.

See [Deep Dives](15_deep_dives.md) for a detailed analysis of full-page writes and their performance implications.

### Recovery Integration

During WAL replay, the startup process uses `XLogInitBufferForRedo()` and related functions to:
1. Read the target page into a buffer (via `ReadBufferWithoutRelcache()`).
2. If the WAL record contains an FPI, restore the page from the image.
3. If no FPI, apply the incremental change to the existing page.
4. Mark the buffer dirty and set the LSN.

## Hint Bit Updates and WAL

Hint bit updates (e.g., marking a tuple's `XMIN` as committed) are a special case:

1. They can be performed under only a shared [content lock](06_page_concurrency_control.md).
2. For non-checksummed databases, hint bit changes are not WAL-logged at all. They can be safely lost because they are re-derivable from `pg_xact`.
3. For checksummed databases, `XLogSaveBufferForHint()` generates a full-page WAL record. This is necessary because the hint bit change modifies the page (invalidating the existing checksum), and a torn write could leave the page with correct data but an incorrect checksum.

```c
/* From MarkBufferDirtyHint() */
if (XLogHintBitIsNeeded())
{
    XLogSaveBufferForHint(buffer, buffer_std);
}
```

See [Deep Dives](15_deep_dives.md) for more on the checksum/hint-bit interaction.

## LSN Comparison for I/O Decisions

### In GetVictimBuffer()

When a non-default [strategy](07_buffer_replacement_policy.md) encounters a dirty victim buffer, it checks whether writing would require a WAL flush:

```c
if (XLogNeedsFlush(lsn)
    && StrategyRejectBuffer(strategy, buf_hdr, from_ring))
{
    /* Reject this buffer -- WAL flush too expensive for bulkread */
    LWLockRelease(content_lock);
    UnpinBuffer(buf_hdr);
    goto again;
}
```

`XLogNeedsFlush()` returns true if the given LSN has not yet been flushed to disk. For `BAS_BULKREAD`, dirty buffers requiring WAL flushes are rejected from the ring.

### In Checkpoint

The checkpoint process calls `XLogFlush()` for the overall checkpoint REDO point before starting buffer writes. Individual `FlushBuffer()` calls still enforce WAL-before-data per buffer, but the `XLogFlush()` calls are typically no-ops since the REDO point is ahead of most buffer LSNs.

---

[<< Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) | [Index](index.md) | [Next: Storage Manager >>](11_storage_manager.md)
