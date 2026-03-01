# Buffer Management Quick Reference

[<< GUC Parameters](appendix_guc_parameters.md) | [Index](index.md) | [Next: API Reference >>](buffer_mgmt_api_reference.md)

---

## Architecture at a Glance

```
Access Methods --> Buffer Manager --> Storage Manager --> OS --> Disk
                       |
                   Shared Memory:
                   - BufferDescriptors[N]  (64B each)
                   - BufferBlocks[N]       (8KB each)
                   - SharedBufHash         (128-partition hash table)
                   - BufferStrategyControl  (clock sweep state)
```

## Buffer States

```
                 PinBuffer()           LockBuffer(SHARE)
  [Free] -----> [Pinned] ------------> [Pinned+SharedLock]
    ^               |                        |
    |               |  LockBuffer(EXCL)      | LockBuffer(UNLOCK)
    |               +----> [Pinned+ExclLock] +
    |               |            |
    |  UnpinBuffer()|            | LockBufferForCleanup()
    +---------------+            v
                         [CleanupLock]  (refcount=1, exclusive)
```

## Common API Patterns

### Read a Page
```c
Buffer buf = ReadBuffer(rel, blocknum);
LockBuffer(buf, BUFFER_LOCK_SHARE);
Page page = BufferGetPage(buf);
/* ... read page contents ... */
LockBuffer(buf, BUFFER_LOCK_UNLOCK);
ReleaseBuffer(buf);
```

### Modify a Page
```c
Buffer buf = ReadBuffer(rel, blocknum);
LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
Page page = BufferGetPage(buf);
/* ... modify page ... */
MarkBufferDirty(buf);
XLogInsert(...);
PageSetLSN(page, lsn);
UnlockReleaseBuffer(buf);
```

### VACUUM a Page
```c
Buffer buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy);
LockBufferForCleanup(buf);
/* ... physically remove dead tuples ... */
PageRepairFragmentation(page);
MarkBufferDirty(buf);
UnlockReleaseBuffer(buf);
```

## State Word Bit Layout

```
Bit: 31  30  29  28  27  26  25  24  23  22  21-18   17-0
     PM  CK  PW  JD  IE  IO  TV  VL  DT  LK  usage   refcount
```

| Abbr | Flag | Meaning |
|------|------|---------|
| PM | BM_PERMANENT | Permanent relation |
| CK | BM_CHECKPOINT_NEEDED | Must write for checkpoint |
| PW | BM_PIN_COUNT_WAITER | Backend waiting for pin=1 |
| JD | BM_JUST_DIRTIED | Dirtied since write started |
| IE | BM_IO_ERROR | Previous I/O failed |
| IO | BM_IO_IN_PROGRESS | I/O in progress |
| TV | BM_TAG_VALID | Tag assigned |
| VL | BM_VALID | Data is valid |
| DT | BM_DIRTY | Needs write-back |
| LK | BM_LOCKED | Header spinlock |

## Lock Ordering (Top to Bottom)

1. Relation-level lock
2. Buffer mapping partition lock (128 partitions, ascending order)
3. Buffer content lock (LWLock per buffer)
4. I/O lock (BM_IO_IN_PROGRESS)
5. Buffer header spinlock (BM_LOCKED)

**Rules:** Pin before lock. Never I/O under spinlock. Partition locks in ascending order.

## Clock Sweep Algorithm

```
for each buffer (circular scan):
    if pinned: skip
    if usage_count > 0: decrement, skip
    if usage_count == 0 and refcount == 0: VICTIM FOUND
```

## Ring Buffer Strategies

| Strategy | Ring Size | Use Case |
|----------|-----------|----------|
| BAS_NORMAL | None | Default |
| BAS_BULKREAD | 256 KB | Sequential scan |
| BAS_BULKWRITE | 16 MB | COPY IN, CREATE TABLE AS |
| BAS_VACUUM | 2 MB | VACUUM |

All capped at NBuffers / 8.

## Write-Back Pipeline

```
MarkBufferDirty() --> [deferred] --> FlushBuffer():
  1. XLogFlush(page_lsn)          -- WAL before data
  2. PageSetChecksumCopy()         -- copy + checksum
  3. smgrwrite() -> pwritev()      -- to kernel cache
  4. ScheduleBufferTagForWriteback -- batch advisory
  5. [checkpoint] fsync()          -- force to disk
```

## Key GUC Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| shared_buffers | 128 MB | Buffer pool size (set to 25-40% RAM) |
| bgwriter_delay | 200 ms | Bgwriter wakeup interval |
| bgwriter_lru_multiplier | 2.0 | Pre-cleaning safety margin |
| bgwriter_lru_maxpages | 100 | Max writes per bgwriter round |
| checkpoint_completion_target | 0.9 | Spread checkpoint I/O |
| effective_io_concurrency | 1 | Prefetch depth (10-200 for SSD) |
| vacuum_buffer_usage_limit | 2 MB | VACUUM ring size |
| io_direct | '' | Bypass OS cache (data, wal) |

## Key Source Files

| File | Purpose |
|------|---------|
| `bufmgr.c` | Core buffer manager |
| `freelist.c` | Clock sweep + ring buffers |
| `buf_table.c` | Hash table |
| `buf_init.c` | Initialization |
| `localbuf.c` | Local buffers |
| `bufpage.c` | Page operations |
| `smgr.c` | Storage manager |
| `md.c` | Magnetic disk layer |
| `buf_internals.h` | Internal structs |
| `bufpage.h` | Page layout |

---

[<< GUC Parameters](appendix_guc_parameters.md) | [Index](index.md) | [Next: API Reference >>](buffer_mgmt_api_reference.md)
