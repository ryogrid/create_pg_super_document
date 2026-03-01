# Integration with Access Methods

## Overview

Access methods (heap, btree, GiST, GIN, etc.) are the primary consumers of the buffer manager API. Each access method follows a common pattern of buffer access: read a page, acquire the appropriate lock, perform operations, mark dirty if modified, and release. This chapter documents the buffer access patterns for major access methods and how they interact with buffer management features like ring strategies, prefetching, and cleanup locks.

## Heap Access Method

### Heap Tuple Read (heap_fetch)

```
heap_fetch(rel, tid, snapshot)
  |
  +-> buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid))
  |     -- pins the buffer
  +-> LockBuffer(buffer, BUFFER_LOCK_SHARE)
  |     -- shared content lock for reading
  +-> ... examine tuple at ItemPointerGetOffsetNumber(tid) ...
  +-> ... check visibility with snapshot ...
  +-> LockBuffer(buffer, BUFFER_LOCK_UNLOCK)
  |     -- release content lock, but keep pin
  +-> ... caller accesses tuple data via pointer into buffer ...
  +-> ReleaseBuffer(buffer)
        -- release pin when done with tuple
```

Key point: the content lock is released before the caller finishes using the tuple data. This is safe because the pin prevents page eviction, and rule #2 from the buffer README states that tuple data can be accessed with only a pin (after initial visibility determination under lock).

### Heap Tuple Insert (heap_insert)

```
RelationGetBufferForTuple(rel, len, ...)
  |
  +-> ReadBuffer(rel, targetBlock)  or  ExtendBufferedRel(...)
  +-> LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)
  +-> ... check free space ...
  +-> [if no space, try another page]
  |
  +-> PageAddItemExtended(page, tuple, len, ...)
  +-> MarkBufferDirty(buffer)
  +-> XLogInsert(...)  -- WAL record
  +-> PageSetLSN(page, lsn)
  +-> UnlockReleaseBuffer(buffer)
```

The insert requires an exclusive content lock because it modifies the page structure. `MarkBufferDirty()` is called before `XLogInsert()` to ensure the buffer is marked dirty before the WAL record is written -- this ordering is important for checkpoint correctness (the checkpoint's REDO point must precede any WAL record for changes not yet written to disk).

### Heap Tuple Update (heap_update)

```
buffer = ReadBuffer(rel, block)
LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)

... find and lock the tuple ...
... check visibility and key constraints ...

[if new tuple fits on same page:]
    PageAddItemExtended(page, newtuple, ...)
    MarkBufferDirty(buffer)
    XLogInsert(...)
    PageSetLSN(page, lsn)
    UnlockReleaseBuffer(buffer)

[if new tuple needs different page:]
    newbuf = ReadBuffer(rel, newblock)
    if (newbuf != buffer)
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE)
    ... add new tuple to new page ...
    ... update old tuple's t_ctid to point to new location ...
    MarkBufferDirty(buffer)
    MarkBufferDirty(newbuf)
    XLogInsert(...)  -- single WAL record covering both pages
    PageSetLSN(old_page, lsn)
    PageSetLSN(new_page, lsn)
    UnlockReleaseBuffer(newbuf)
    UnlockReleaseBuffer(buffer)
```

When the update spans two pages, both are locked and modified within a single WAL record, ensuring atomic recovery.

### Heap Tuple Delete (heap_delete)

```
buffer = ReadBuffer(rel, block)
LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)
... mark tuple as deleted (set xmax, flags) ...
MarkBufferDirty(buffer)
XLogInsert(...)
PageSetLSN(page, lsn)
UnlockReleaseBuffer(buffer)
```

## B-tree Index Access

### Index Scan (_bt_search)

```
_bt_search(rel, key, ...)
  |
  +-> buf = _bt_getroot(rel)
  |     +-> ReadBuffer(rel, metapage)
  |     +-> LockBuffer(buf, BUFFER_LOCK_SHARE)
  |     +-> ... read root page number from meta ...
  |     +-> ... release meta, read root ...
  |
  +-> [descend through tree:]
  |   for each internal page:
  |     +-> LockBuffer(buf, BUFFER_LOCK_SHARE)
  |     +-> _bt_binsrch(page, key)
  |     +-> child = _bt_getbuf(rel, childblk, BT_READ)
  |     +-> ReleaseBuffer(buf)  -- release parent
  |     +-> buf = child
  |
  +-> [leaf page reached, still locked SHARE]
  +-> ... scan tuples ...
```

The btree traversal uses a "crabbing" lock protocol: the child page is locked before the parent is released. This prevents tree structure changes from invalidating the traversal path.

### Index Insert (_bt_doinsert)

```
_bt_doinsert(rel, itup, ...)
  |
  +-> _bt_search(rel, key)  -- find leaf page
  +-> LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)  -- upgrade to exclusive
  +-> [if page has space:]
  |     _bt_insertuple(page, itup)
  |     MarkBufferDirty(buf)
  |     XLogInsert(...)
  |     PageSetLSN(page, lsn)
  +-> [if page full: split]
  |     _bt_split(rel, buf, ...)
  |     -- allocates new page via _bt_getbuf(rel, P_NEW, BT_WRITE)
  |     -- redistributes tuples
  |     -- marks both pages dirty
  |     -- single WAL record for the split
  +-> UnlockReleaseBuffer(buf)
```

## VACUUM Integration

### Buffer Access Strategy

VACUUM uses a `BAS_VACUUM` ring buffer strategy (default 2 MB ring, controlled by `vacuum_buffer_usage_limit`):

```
strategy = GetAccessStrategy(BAS_VACUUM);

for each block in relation:
    buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
                             RBM_NORMAL, strategy);
```

This confines VACUUM's working set to the ring, preventing it from evicting hot pages from the main buffer pool.

### Cleanup Lock for Tuple Deletion

When VACUUM needs to physically remove dead tuples (not just mark them dead):

```
buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy)
LockBufferForCleanup(buf)
  -- waits for all other backends to unpin the page
  -- this ensures no backend has a pointer into the page data

lazy_vacuum_page(rel, buf, ...)
  +-> heap_page_prune(...)  -- redirect/remove dead tuples
  +-> PageRepairFragmentation(page)  -- compact free space
  +-> MarkBufferDirty(buf)
  +-> XLogInsert(...)
  +-> PageSetLSN(page, lsn)

UnlockReleaseBuffer(buf)
```

`LockBufferForCleanup()` is essential for VACUUM because dead tuple removal changes line pointer offsets. If another backend held a pointer to a tuple on the page (obtained during a previous scan under only a pin), that pointer would become invalid after compaction.

## Prefetching

### PrefetchBuffer()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
PrefetchBufferResult PrefetchBuffer(Relation reln, ForkNumber forkNum,
                                    BlockNumber blockNum)
```

Issues an asynchronous read request for a page that is likely to be needed soon. If the page is already in the buffer pool, returns immediately with `recent_buffer` set. Otherwise, calls `smgrprefetch()` which issues a `posix_fadvise(POSIX_FADV_WILLNEED)` hint to the kernel.

**Returns:**
```c
typedef struct PrefetchBufferResult
{
    Buffer  recent_buffer;  /* valid if already in pool */
    bool    initiated_io;   /* true if async I/O was requested */
} PrefetchBufferResult;
```

### Use in Bitmap Heap Scans

Bitmap heap scans prefetch pages ahead of the actual access point:

```
for each page in bitmap:
    if (prefetch_distance > 0 && pages_ahead < prefetch_distance)
        PrefetchBuffer(rel, MAIN_FORKNUM, next_block)

    buf = ReadBuffer(rel, current_block)
    ... process tuples ...
```

The `effective_io_concurrency` GUC (default 1) controls how many pages ahead the prefetch reaches. For SSDs, values of 10-200 are appropriate. `maintenance_io_concurrency` (default 10) is the equivalent for maintenance operations (VACUUM, CREATE INDEX).

### Read Streams (PostgreSQL 17)

PostgreSQL 17 introduced a read stream API that combines prefetching with vectorized reads:

```c
ReadBuffersOperation operation;
operation.smgr = smgr;
operation.forknum = MAIN_FORKNUM;
operation.strategy = strategy;

if (StartReadBuffers(&operation, buffers, blocknum, &nblocks, flags))
    WaitReadBuffers(&operation);
```

`StartReadBuffers()` allocates buffer slots for a contiguous range of blocks and initiates multi-block I/O through `smgrreadv()`, which translates to a single `preadv()` system call for the entire range.

## Summary of Buffer Access Patterns

| Operation | Lock Mode | Dirty? | WAL? | Strategy |
|-----------|-----------|--------|------|----------|
| Heap scan (read) | SHARE | No | No | None or BAS_BULKREAD |
| Heap fetch | SHARE then pin-only | No | No | None |
| Heap insert | EXCLUSIVE | Yes | Yes | None |
| Heap update | EXCLUSIVE (both pages) | Yes | Yes | None |
| Heap delete | EXCLUSIVE | Yes | Yes | None |
| Heap hint bits | SHARE | Yes (hint) | Maybe | None |
| VACUUM scan | SHARE or CLEANUP | Yes | Yes | BAS_VACUUM |
| Btree scan | SHARE (crabbing) | No | No | None |
| Btree insert | EXCLUSIVE (leaf) | Yes | Yes | None |
| Btree split | EXCLUSIVE (2 pages) | Yes | Yes | None |
| Seq scan | SHARE | No | No | BAS_BULKREAD |
| COPY IN | EXCLUSIVE | Yes | Yes | BAS_BULKWRITE |
| CREATE TABLE AS | EXCLUSIVE | Yes | Yes | BAS_BULKWRITE |
