# WAL Integration and LSN Management

## Overview

The WAL (Write-Ahead Logging) subsystem and the buffer manager are tightly coupled through the "WAL-before-data" rule: no dirty data page may be written to disk until the WAL record describing the change has been flushed to persistent storage. This rule is the foundation of PostgreSQL's crash recovery guarantee. The buffer manager enforces this rule through the `pd_lsn` field in every page header and the `XLogFlush()` call in `FlushBuffer()`.

## The Fundamental Rule

From the comment in `src/include/storage/bufpage.h:123-125`:

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

### PageGetLSN()

Source: `src/include/storage/bufpage.h:383-387`

```c
static inline XLogRecPtr
PageGetLSN(Page page)
{
    return PageXLogRecPtrGet(((PageHeader) page)->pd_lsn);
}
```

Reads the 64-bit LSN from the page header. The LSN is stored as two 32-bit values (`PageXLogRecPtr`) for historical reasons:

```c
/* From bufpage.h:100-104 */
static inline XLogRecPtr
PageXLogRecPtrGet(PageXLogRecPtr val)
{
    return (uint64) val.xlogid << 32 | val.xrecoff;
}
```

### PageSetLSN()

Source: `src/include/storage/bufpage.h:388-392`

```c
static inline void
PageSetLSN(Page page, XLogRecPtr lsn)
{
    PageXLogRecPtrSet(((PageHeader) page)->pd_lsn, lsn);
}
```

Called after `XLogInsert()` to record the LSN of the WAL record that describes the page modification. The caller must hold an exclusive content lock on the buffer.

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

This is used when the caller needs the LSN but does not hold a content lock (e.g., during eviction decisions).

## WAL-Before-Data Enforcement in FlushBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:3815-3835`

The enforcement code in `FlushBuffer()`:

```c
/* Read LSN under header lock (shared content lock is insufficient) */
buf_state = LockBufHdr(buf);
recptr = BufferGetLSN(buf);
buf_state &= ~BM_JUST_DIRTIED;
UnlockBufHdr(buf, buf_state);

/*
 * Force XLOG flush up to buffer's LSN.
 * However, skip for non-permanent (unlogged) relations.
 */
if (buf_state & BM_PERMANENT)
    XLogFlush(recptr);
```

### Why the Header Lock is Needed

The LSN is read under the header spinlock (not the content lock) because `FlushBuffer()` only holds a shared content lock. Under a shared lock, other backends can update hint bits, which could result in a concurrent modification to `pd_lsn` (since WAL-logging a hint bit update generates a new LSN). The header spinlock provides the necessary atomicity for reading the two 32-bit halves of the 64-bit LSN.

### Unlogged Relations Exception

Unlogged relations skip the WAL flush because:
1. They are lost after a crash anyway, so WAL protection is unnecessary.
2. Some unlogged relations (e.g., GiST indexes) use "fake" LSNs from `GetFakeLSNForUnloggedRel()`. These fake LSNs could potentially exceed the actual WAL insertion point, and attempting to flush WAL to such a location would fail.

```c
/* From FlushBuffer() comments in bufmgr.c:3824-3835 */
/*
 * However, this rule does not apply to unlogged relations, which will be
 * lost after a crash anyway. ... attempting to flush WAL through that
 * location would fail, with disastrous system-wide consequences.
 */
```

## XLogFlush()

Source: `src/backend/access/transam/xlog.c`

```c
void XLogFlush(XLogRecPtr record)
```

Ensures WAL is flushed to persistent storage up to (at least) the specified LSN. This may involve:

1. Writing WAL buffers from shared memory to the WAL segment files.
2. Calling `fsync()` on the WAL segment files.
3. Group commit optimization: if another backend is already flushing past our target LSN, wait for that flush to complete rather than issuing a redundant fsync.

This function can be expensive (involves disk I/O), which is why the background writer and checkpoint process proactively clean dirty buffers to avoid forcing backends to call `XLogFlush()` during buffer eviction.

## Full-Page Writes

### The Torn Page Problem

If a system crash occurs while the OS is writing an 8 KB page to disk, the result can be a "torn page" -- a page that is partially old data and partially new data. WAL replay cannot fix this because it applies changes incrementally to existing pages, and the base page is corrupted.

### Solution: Full-Page Images

The first time a page is modified after a checkpoint, PostgreSQL includes a full copy of the entire page (a "full-page image" or FPI) in the WAL record. This is the `REGBUF_FORCE_IMAGE` flag in `XLogInsert()`. During recovery, the full-page image replaces the on-disk page entirely, eliminating the torn-page risk.

The buffer manager's role:
- After a checkpoint completes, the `BM_CHECKPOINT_NEEDED` flag is cleared on written buffers.
- The first modification to a page after checkpoint sets a new LSN. The WAL subsystem (not the buffer manager) determines whether an FPI is needed based on whether this is the first modification since the last checkpoint.

### Recovery Integration

During WAL replay, the startup process uses `XLogInitBufferForRedo()` and related functions to:
1. Read the target page into a buffer (via `ReadBufferWithoutRelcache()`).
2. If the WAL record contains an FPI, restore the page from the image.
3. If no FPI, apply the incremental change to the existing page.
4. Mark the buffer dirty and set the LSN.

## Hint Bit Updates and WAL

Hint bit updates (e.g., marking a tuple's `XMIN` as committed) are a special case:

1. They can be performed under only a shared content lock (rule #4 in `src/backend/storage/buffer/README`).
2. For non-checksummed databases, hint bit changes are not WAL-logged at all. They can be safely lost because they are re-derivable from `pg_xact`.
3. For checksummed databases, `XLogSaveBufferForHint()` generates a full-page WAL record. This is necessary because the hint bit change modifies the page (invalidating the existing checksum), and a torn write could leave the page with correct data but an incorrect checksum, which would be detected as corruption.

```c
/* From MarkBufferDirtyHint() */
if (XLogHintBitIsNeeded())
{
    /* Permanent buffer with checksums -- need WAL protection */
    XLogSaveBufferForHint(buffer, buffer_std);
}
```

## LSN Comparison for I/O Decisions

### In GetVictimBuffer()

When a non-default strategy encounters a dirty victim buffer, it checks whether writing the buffer would require a WAL flush:

```c
/* From GetVictimBuffer(), bufmgr.c:2023-2033 */
buf_state = LockBufHdr(buf_hdr);
lsn = BufferGetLSN(buf_hdr);
UnlockBufHdr(buf_hdr, buf_state);

if (XLogNeedsFlush(lsn)
    && StrategyRejectBuffer(strategy, buf_hdr, from_ring))
{
    /* Reject this buffer -- WAL flush would be too expensive for bulkread */
    LWLockRelease(content_lock);
    UnpinBuffer(buf_hdr);
    goto again;
}
```

`XLogNeedsFlush()` returns true if the given LSN has not yet been flushed to disk. For `BAS_BULKREAD`, dirty buffers requiring WAL flushes are rejected from the ring to avoid imposing WAL flush overhead on sequential scans.

### In Checkpoint

The checkpoint process does not need LSN comparisons for individual buffers because it calls `XLogFlush()` for the overall checkpoint REDO point before starting buffer writes. Individual `FlushBuffer()` calls still enforce WAL-before-data per buffer, but since the checkpoint's REDO point is ahead of most buffer LSNs, the `XLogFlush()` calls are typically no-ops.
