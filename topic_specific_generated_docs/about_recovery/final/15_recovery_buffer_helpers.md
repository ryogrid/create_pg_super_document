# 15 — Recovery Buffer Helpers

[← Rmgr Dispatch](14_rmgr_dispatch.md) | [index](index.md) | [next: Hooks and Extensibility →](16_hooks_and_extensibility.md)

---


This component documents the buffer-manager interface used by the
22 redo callbacks. The key entry points live in
`src/backend/access/transam/xlogutils.c`. They handle the
LSN-skip optimization, the "page doesn't exist yet" case, and the
"page is unrecoverable" case.


## Why a separate redo-side buffer manager

A redo callback needs to take a buffer reference to apply changes.
But unlike the primary, the standby:

* Cannot rely on the buffer being already in the buffer pool (the
  buffer manager has not been touched since recovery started).
* Must handle the case where `pd_lsn >= record_lsn` (page is
  already past this record — skip).
* Must handle the case where the buffer is missing because the
  underlying relation/segment was dropped/truncated by a *future*
  WAL record that we'll see later in the stream.
* Must update `minRecoveryPoint` when it dirties a page.

`XLogReadBufferForRedo` and friends solve these uniformly.

## API surface

```c
/* Returns BLK_NEEDS_REDO, BLK_DONE, BLK_RESTORED, or BLK_NOTFOUND. */
XLogRedoAction XLogReadBufferForRedo(XLogReaderState *record,
                                     uint8 block_id, Buffer *buf);

XLogRedoAction XLogReadBufferForRedoExtended(XLogReaderState *record,
                                             uint8 block_id,
                                             ReadBufferMode mode,
                                             bool get_cleanup_lock,
                                             Buffer *buf);

Buffer XLogInitBufferForRedo(XLogReaderState *record, uint8 block_id);
```

### `XLogRedoAction` enum

```c
typedef enum
{
    BLK_NEEDS_REDO,    /* page is in buffer; caller must replay */
    BLK_DONE,          /* page LSN >= record LSN; caller skips */
    BLK_RESTORED,      /* page was restored from FPI; caller should not modify */
    BLK_NOTFOUND       /* page/relation no longer exists; caller skips */
} XLogRedoAction;
```

The redo callback contract:

```c
static void
some_redo(XLogReaderState *record)
{
    Buffer buffer;
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        Page page = BufferGetPage(buffer);
        /* apply changes to page, dirty buffer */
        PageSetLSN(page, record->EndRecPtr);
        MarkBufferDirty(buffer);
    }
    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```

## `XLogReadBufferForRedo` semantics

1. Decode block reference: `XLogRecGetBlockTag(record, block_id,
   &rnode, &fork, &blknum)`.
2. If the record carries a Full-Page Image (FPI) for this block,
   use the FPI:
   * Restore the page from FPI bytes.
   * Set page LSN to `record->EndRecPtr`.
   * Mark dirty.
   * Return `BLK_RESTORED`.
3. Else read the page (`XLogReadBufferExtended` →
   `ReadBufferWithoutRelcache`).
   * If relation/page doesn't exist (the relation was dropped by a
     future record), return `BLK_NOTFOUND`.
   * If `page->pd_lsn >= record->EndRecPtr`, return `BLK_DONE`
     (this is the LSN-skip optimization: the page already has the
     change baked in).
   * Else return `BLK_NEEDS_REDO`.
4. The buffer is left locked exclusive on `BLK_NEEDS_REDO` /
   `BLK_RESTORED`. The caller must `UnlockReleaseBuffer`.

## LSN-skip optimization

The page LSN check is what makes redo idempotent. Suppose the
primary wrote record R that updates page P from LSN1 to LSN2:

* If we crash *after* P was flushed with LSN2, recovery sees
  `page->pd_lsn = LSN2 >= record_lsn`, returns `BLK_DONE`, skips.
* If we crash *before* P was flushed (page on disk has LSN0 or
  LSN1), recovery sees `pd_lsn < record_lsn`, returns
  `BLK_NEEDS_REDO`, applies the record.

This is why redo records must be deterministic given the page state
— there's no way to know whether a previous run already applied
them.

Records that **must** apply unconditionally (FPIs, CHECKPOINT,
CLOG zero-page) bypass this check by being `BLK_RESTORED` or by
not going through the page-LSN check at all (CLOG, SLRU records).

## `XLogInitBufferForRedo`

Creates a brand-new buffer for a block that's expected to not
exist yet (or be unused). Used for:

* Index-page split: a brand-new right sibling is being created.
* `XLOG_HEAP_INSERT` to a new page.
* `XLOG_BTREE_INSERT_LEAF` to a new leaf.

Always returns a locked, dirty buffer. The page is zeroed before
the caller writes.

## minRecoveryPoint advancement

When a redo callback dirties a buffer at LSN R, the buffer
manager's `XLogFlush` (called from page eviction) ensures
`minRecoveryPoint >= R`. The mechanism:

1. `MarkBufferDirty` sets `BM_DIRTY` and the buffer's `lsn` field.
2. When the buffer is later evicted (or checkpoint-flushed),
   `FlushBuffer` calls `XLogFlush(buf->lsn)`.
3. `XLogFlush` during recovery is special: it doesn't actually
   wait for WAL flush (we don't write WAL); instead it calls
   `UpdateMinRecoveryPoint(lsn, false)` which advances
   `pg_control->minRecoveryPoint`.

This guarantees that any page reaching disk with LSN R is matched
by `minRecoveryPoint >= R` in pg_control. So a future crash
recovery that resumes from `minRecoveryPoint` will not skip records
that have already touched the on-disk image.

## Source references

* `src/backend/access/transam/xlogutils.c` — `XLogReadBufferForRedo`,
  `XLogReadBufferForRedoExtended`, `XLogInitBufferForRedo`,
  `XLogReadBufferExtended`
* `src/backend/access/transam/xlog.c` — `UpdateMinRecoveryPoint`
* `src/backend/storage/buffer/bufmgr.c` — `FlushBuffer`,
  `MarkBufferDirty`

## Related

* All 22 redo callbacks call into these helpers — see
  `redo_callback_catalog/`.
* `verifyBackupPageConsistency` (called from `ApplyWalRecord`) uses
  `XLogReadBufferExtended` directly to compare an FPI against the
  just-replayed page.
