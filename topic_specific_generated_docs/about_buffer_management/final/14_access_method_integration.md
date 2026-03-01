# Integration with Access Methods

[<< Local Buffers](13_local_buffers.md) | [Index](index.md) | [Next: Deep Dives >>](15_deep_dives.md)

---

## Overview

Access methods (heap, btree, GiST, GIN, etc.) are the primary consumers of the buffer manager API. Each access method follows a common pattern of buffer access: read a page, acquire the appropriate [lock](06_page_concurrency_control.md), perform operations, mark dirty if modified, and release. This chapter documents the buffer access patterns for major access methods and how they interact with buffer management features like [ring strategies](07_buffer_replacement_policy.md), [prefetching](#prefetching), and [cleanup locks](06_page_concurrency_control.md).

## Heap Access Method

### Heap Tuple Read (heap_fetch)

```
heap_fetch(rel, tid, snapshot)
  |
  +-> buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid))
  +-> LockBuffer(buffer, BUFFER_LOCK_SHARE)
  +-> ... examine tuple, check visibility ...
  +-> LockBuffer(buffer, BUFFER_LOCK_UNLOCK)
  +-> ... caller accesses tuple data via pointer into buffer ...
  +-> ReleaseBuffer(buffer)
```

Key point: the content lock is released before the caller finishes using the tuple data. This is safe because the pin prevents page eviction.

### Heap Tuple Insert (heap_insert)

```
RelationGetBufferForTuple(rel, len, ...)
  |
  +-> ReadBuffer(rel, targetBlock)  or  ExtendBufferedRel(...)
  +-> LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)
  +-> PageAddItemExtended(page, tuple, len, ...)
  +-> MarkBufferDirty(buffer)
  +-> XLogInsert(...)  -- WAL record
  +-> PageSetLSN(page, lsn)
  +-> UnlockReleaseBuffer(buffer)
```

The insert requires an exclusive content lock. [MarkBufferDirty()](09_dirty_buffer_and_writeback.md) is called before `XLogInsert()` to ensure the buffer is marked dirty before the WAL record is written -- this ordering is important for [checkpoint](09_dirty_buffer_and_writeback.md) correctness.

### Heap Tuple Update (heap_update)

```
buffer = ReadBuffer(rel, block)
LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)

[if new tuple fits on same page:]
    PageAddItemExtended(page, newtuple, ...)
    MarkBufferDirty(buffer)
    XLogInsert(...)
    PageSetLSN(page, lsn)
    UnlockReleaseBuffer(buffer)

[if new tuple needs different page:]
    newbuf = ReadBuffer(rel, newblock)
    LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE)
    ... modify both pages ...
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
  +-> buf = _bt_getroot(rel)  -- read metapage, then root
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
```

The btree traversal uses a "crabbing" lock protocol: the child page is locked before the parent is released. This prevents tree structure changes from invalidating the traversal path.

### Index Insert (_bt_doinsert)

```
_bt_doinsert(rel, itup, ...)
  |
  +-> _bt_search(rel, key)  -- find leaf page
  +-> LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)
  +-> [if page has space:]
  |     _bt_insertuple(page, itup)
  |     MarkBufferDirty(buf)
  |     XLogInsert(...)
  |     PageSetLSN(page, lsn)
  +-> [if page full: split]
  |     _bt_split(rel, buf, ...)
  |     -- allocates new page, redistributes tuples
  |     -- marks both pages dirty, single WAL record for the split
  +-> UnlockReleaseBuffer(buf)
```

## VACUUM Integration

### Buffer Access Strategy

VACUUM uses a `BAS_VACUUM` [ring buffer strategy](07_buffer_replacement_policy.md) (default 2 MB ring, controlled by `vacuum_buffer_usage_limit`):

```
strategy = GetAccessStrategy(BAS_VACUUM);

for each block in relation:
    buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
                             RBM_NORMAL, strategy);
```

This confines VACUUM's working set to the ring, preventing it from evicting hot pages.

### Cleanup Lock for Tuple Deletion

When VACUUM needs to physically remove dead tuples:

```
buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy)
LockBufferForCleanup(buf)
  -- waits for all other backends to unpin the page

lazy_vacuum_page(rel, buf, ...)
  +-> heap_page_prune(...)  -- redirect/remove dead tuples
  +-> PageRepairFragmentation(page)  -- compact free space
  +-> MarkBufferDirty(buf)
  +-> XLogInsert(...)
  +-> PageSetLSN(page, lsn)

UnlockReleaseBuffer(buf)
```

[LockBufferForCleanup()](06_page_concurrency_control.md) is essential because dead tuple removal changes line pointer offsets. If another backend held a pointer to a tuple on the page, that pointer would become invalid after compaction.

## Prefetching

### PrefetchBuffer()

Source: `src/backend/storage/buffer/bufmgr.c`

```c
PrefetchBufferResult PrefetchBuffer(Relation reln, ForkNumber forkNum,
                                    BlockNumber blockNum)
```

Issues an asynchronous read request for a page that is likely to be needed soon. If the page is already in the buffer pool, returns immediately. Otherwise, calls `smgrprefetch()` which issues a `posix_fadvise(POSIX_FADV_WILLNEED)` hint to the kernel.

### Use in Bitmap Heap Scans

Bitmap heap scans prefetch pages ahead of the actual access point:

```
for each page in bitmap:
    if (prefetch_distance > 0 && pages_ahead < prefetch_distance)
        PrefetchBuffer(rel, MAIN_FORKNUM, next_block)
    buf = ReadBuffer(rel, current_block)
    ... process tuples ...
```

The `effective_io_concurrency` GUC (default 1) controls prefetch depth. For SSDs, values of 10-200 are appropriate. `maintenance_io_concurrency` (default 10) is the equivalent for maintenance operations.

### Read Streams (PostgreSQL 17)

PostgreSQL 17 introduced a read stream API that combines prefetching with vectorized reads:

```c
if (StartReadBuffers(&operation, buffers, blocknum, &nblocks, flags))
    WaitReadBuffers(&operation);
```

[StartReadBuffers()](05_buffer_access_protocol.md) allocates buffer slots for a contiguous range and initiates multi-block I/O through `smgrreadv()`, translating to a single `preadv()` system call.

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

---

[<< Local Buffers](13_local_buffers.md) | [Index](index.md) | [Next: Deep Dives >>](15_deep_dives.md)
