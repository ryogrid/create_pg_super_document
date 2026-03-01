# Data Movement: Memory to Disk

## Overview

This chapter describes the complete data path from PostgreSQL shared buffers through the operating system to persistent storage, covering the double-buffering architecture, fsync semantics, the direct I/O option, and writeback advisory mechanisms.

## Double Buffering Architecture

PostgreSQL uses a two-tier caching architecture where data pages pass through both the PostgreSQL shared buffer pool and the OS kernel page cache:

```
User Backend
     |
     | BufferGetPage() -- direct pointer into shared memory
     v
+----------------------------+
| PostgreSQL Shared Buffers  |  (shared_buffers, e.g., 128 MB - 8 GB)
| (NBuffers * 8KB pages)     |
+----------------------------+
     |
     | smgrwrite() --> write() system call
     v
+----------------------------+
| OS Kernel Page Cache       |  (managed by OS, potentially many GB)
+----------------------------+
     |
     | fsync() / sync_file_range() / pdflush daemon
     v
+----------------------------+
| Physical Disk / SSD        |
+----------------------------+
```

### Write Path

When `FlushBuffer()` writes a dirty page:

1. `smgrwrite()` calls `mdwritev()`, which calls `FileWriteV()`, which calls `pwritev()`.
2. The `pwritev()` system call copies the 8 KB page from PostgreSQL's shared memory into the kernel page cache. The function returns immediately (the page is NOT yet on disk).
3. The kernel's pdflush/writeback daemon eventually flushes the page to disk on its own schedule.
4. At checkpoint time, PostgreSQL calls `fsync()` on relation files to force all dirty kernel pages to persistent storage.

### Read Path

When `mdreadv()` reads a page:

1. `FileReadV()` calls `preadv()`.
2. If the page is in the kernel page cache (because it was recently written or read), the read completes from RAM -- no disk I/O.
3. If not cached, the kernel issues a disk read, copies the data into its page cache, and then into the PostgreSQL buffer.

### Implications of Double Buffering

**Memory waste**: The same data page can exist in both PostgreSQL's buffer pool and the OS page cache, consuming memory twice. For large `shared_buffers` settings, this can waste significant RAM.

**Consistency**: PostgreSQL relies on the OS page cache's write ordering guarantees (or lack thereof) for crash safety. The WAL-before-data rule and checkpoint fsync ensure that even if the OS reorders writes, recovery is possible.

**Read-ahead**: The OS may perform read-ahead on sequential file access, which benefits sequential scans even when the buffer manager does not explicitly prefetch.

## Fsync Semantics

PostgreSQL uses deferred fsync: writes during normal operation go to the kernel page cache without immediate syncing. Durability is guaranteed by fsync at checkpoint boundaries.

### Checkpoint Fsync Flow

```
BufferSync() -- writes all dirty buffers to kernel cache
     |
     v
IssuePendingWritebacks() -- advises kernel to start flushing
     |
     v
smgrDoPendingSyncs() -- calls fsync() on all modified files
     |
     v
Checkpoint complete -- all data on stable storage
```

### smgrimmedsync()

```c
void smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
```

Immediately fsyncs a specific relation fork. Used for operations that bypass WAL logging (e.g., `CREATE DATABASE` copying files) where deferred fsync at checkpoint would be insufficient.

### Deferred Sync Registration

During normal operation, when `mdwritev()` writes a page, it registers the segment for deferred fsync:

```c
/* In mdwritev() */
if (!skipFsync && !SmgrIsTemp(reln))
    register_dirty_segment(reln, forknum, v);
```

The registered segments are collected in a pending list and processed by `smgrDoPendingSyncs()` at the next checkpoint.

## Direct I/O Option

PostgreSQL 16+ introduced the `io_direct` GUC parameter to optionally bypass the OS page cache:

```
io_direct = ''              # default: use OS page cache
io_direct = 'data'          # bypass OS cache for data files
io_direct = 'wal'           # bypass OS cache for WAL files
io_direct = 'data,wal'      # bypass both
```

### When io_direct = 'data'

- Data file I/O uses `O_DIRECT` flag, bypassing the kernel page cache entirely.
- Eliminates double buffering -- memory is used more efficiently.
- PostgreSQL's shared buffer pool becomes the only data cache.
- Requires all I/O buffers to be aligned to `PG_IO_ALIGN_SIZE` (typically 4096 bytes).
- May reduce performance for workloads that benefit from OS read-ahead.
- `BufferBlocks` allocation in `InitBufferPool()` already includes alignment padding:
  ```c
  BufferBlocks = (char *)
      TYPEALIGN(PG_IO_ALIGN_SIZE,
                ShmemInitStruct("Buffer Blocks",
                                NBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
                                &foundBufs));
  ```

### Impact on Writeback

With direct I/O, `smgrwriteback()` becomes a no-op (there is no kernel page cache to advise). The writeback context mechanism is still invoked but the underlying `FileWriteback()` call skips the advisory.

## Writeback Advisory Mechanism

### Purpose

Between `smgrwrite()` (which copies data to the kernel page cache) and `fsync()` (which forces everything to disk), the writeback advisory provides a middle ground: it requests the kernel to start flushing specific pages to disk asynchronously. This smooths out I/O patterns and reduces the burst of disk writes at fsync time.

### Implementation

```c
/* From ScheduleBufferTagForWriteback() */
void ScheduleBufferTagForWriteback(WritebackContext *wb_context,
                                   IOContext io_context, BufferTag *tag)
```

Each write schedules the written block's tag for writeback. When `max_pending` requests accumulate, `IssuePendingWritebacks()` is called:

1. Sort pending tags by `(spcOid, dbOid, relNumber, forkNum, blockNum)`.
2. Coalesce adjacent blocks into contiguous ranges.
3. Call `smgrwriteback()` for each range.

The `smgrwriteback()` call translates to:
- **Linux**: `sync_file_range(fd, offset, nbytes, SYNC_FILE_RANGE_WRITE)` -- asks kernel to start writeback without waiting.
- **Other platforms**: `posix_fadvise(fd, offset, nbytes, POSIX_FADV_DONTNEED)` -- suggests the kernel can release the pages (which implies writing dirty ones first).

### Coalescing Benefits

Without coalescing, each buffer write would generate a separate `sync_file_range()` call. By batching and sorting, adjacent block writes become a single range request, which the kernel can optimize into sequential disk writes.

## Data Flow Summary

### Normal Operation Write

```
1. Backend modifies page in shared buffer
2. MarkBufferDirty() -- sets BM_DIRTY (no I/O)
3. [Eventually] Buffer eviction or checkpoint
4. FlushBuffer():
   a. XLogFlush(page_lsn) -- ensure WAL on disk
   b. PageSetChecksumCopy() -- copy page with checksum
   c. smgrwrite() -> mdwritev() -> FileWriteV() -> pwritev()
      --> data in kernel page cache
   d. ScheduleBufferTagForWriteback()
      --> [batch] sync_file_range() -- advise kernel to start writeback
5. [At checkpoint] smgrDoPendingSyncs()
   --> fsync() -- guarantee on disk
```

### Sequential Scan Read

```
1. ReadBuffer(rel, blocknum)
2. BufferAlloc() -- check hash table
3. [Cache miss] GetVictimBuffer() -- find free slot
4. StartReadBuffer() / WaitReadBuffers():
   a. StartBufferIO() -- claim I/O lock
   b. smgrreadv() -> mdreadv() -> FileReadV() -> preadv()
      --> data from disk (or OS cache) to shared buffer
   c. PageIsVerifiedExtended() -- validate checksums
   d. TerminateBufferIO(BM_VALID) -- mark valid, wake waiters
5. Return pinned buffer to caller
```
