# Local Buffers for Temporary Tables

[<< Data Movement and Durability](12_data_movement_and_durability.md) | [Index](index.md) | [Next: Access Method Integration >>](14_access_method_integration.md)

---

## Overview

Local buffers are a backend-private buffer pool used exclusively for temporary table pages. Because temporary tables are only visible to the creating backend and are not preserved across crashes, local buffers eliminate the overhead of shared memory synchronization: no locking, no WAL logging, and no shared reference counting.

Implemented in `src/backend/storage/buffer/localbuf.c`.

## Design Rationale

| Property | Shared Buffers | Local Buffers |
|----------|---------------|---------------|
| Visibility | All backends | Single backend |
| Crash recovery | Yes (WAL-logged) | No |
| Locking | [Content locks](06_page_concurrency_control.md) + header spinlocks | None |
| Reference counting | Shared atomic + private | Private only (`LocalRefCount`) |
| Memory location | Shared memory | Backend-local memory |
| Buffer numbering | Positive (1..NBuffers) | Negative (-1..-NLocBuffer) |

Local buffers use the same [BufferDesc](03_buffer_pool_architecture.md) structure as shared buffers, but the atomic state operations are simplified to plain reads and writes, since no concurrent access is possible.

## Data Structures

### Global Variables

```c
extern int NLocBuffer;                       /* default: 64 */
extern Block *LocalBufferBlockPointers;
extern int32 *LocalRefCount;
extern BufferDesc *LocalBufferDescriptors;
```

| Variable | Type | Description |
|----------|------|-------------|
| `NLocBuffer` | `int` | Number of local buffer slots (controlled by `num_temp_buffers`, default 8 MB = 1024 buffers) |
| `LocalBufferDescriptors` | `BufferDesc *` | Array of local buffer descriptors |
| `LocalBufferBlockPointers` | `Block *` | Array of pointers to page data |
| `LocalRefCount` | `int32 *` | Per-buffer reference counts |

### Buffer Numbering

Local buffers use negative `Buffer` values:
- Buffer value `-1` corresponds to local descriptor index 0.
- Buffer value `-n` corresponds to local descriptor index `n - 1`.
- General formula: `local_buf_id = -(buffer + 1)` or equivalently `-buffer - 1`.

This is reflected in `BufferGetBlock()`:

```c
if (BufferIsLocal(buffer))
    return LocalBufferBlockPointers[-buffer - 1];
```

## Core API

### LocalBufferAlloc()

Source: `src/backend/storage/buffer/localbuf.c`

```c
BufferDesc *LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum,
                             BlockNumber blockNum, bool *foundPtr)
```

The local equivalent of [BufferAlloc()](05_buffer_access_protocol.md). Looks up the requested page in the local hash table and either returns an existing buffer (cache hit) or allocates a new one (cache miss).

**Cache miss handling:**

Unlike shared buffers, local buffer replacement uses a simple clock-sweep through the local descriptor array. When a victim is found (refcount = 0, usage count = 0):

1. If the victim is dirty, write it to disk immediately via [smgrwrite()](11_storage_manager.md). No WAL flush is needed.
2. Remove the old hash table entry.
3. Assign the new tag and insert into the hash table.

### PinLocalBuffer()

```c
bool PinLocalBuffer(BufferDesc *buf_hdr, bool adjust_usagecount)
```

Increments the local reference count. Uses plain (non-atomic) state manipulation:

```c
if (LocalRefCount[bufid] == 0)
{
    if (adjust_usagecount &&
        BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT)
    {
        buf_state += BUF_USAGECOUNT_ONE;
        pg_atomic_unlocked_write_u32(&buf_hdr->state, buf_state);
    }
}
LocalRefCount[bufid]++;
```

**Returns:** `true` if the buffer is valid (`BM_VALID` is set).

### MarkLocalBufferDirty()

```c
void MarkLocalBufferDirty(Buffer buffer)
```

Sets the `BM_DIRTY` flag using a plain write (no CAS needed):

```c
buf_state = pg_atomic_read_u32(&bufHdr->state);
buf_state |= BM_DIRTY;
pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
```

## Initialization

### InitLocalBuffers()

Lazily initialized on the first access to a local buffer. Allocates:

1. `LocalBufferDescriptors`: Array of `NLocBuffer` `BufferDesc` structures (NOT padded to cache lines).
2. `LocalBufferBlockPointers`: Array of `NLocBuffer` block pointers (initially NULL).
3. `LocalRefCount`: Array of `NLocBuffer` `int32` reference counts.
4. `LocalBufHash`: Private hash table for tag-to-buffer mapping.

### GetLocalBufferStorage()

Allocates page data storage in blocks of 32 pages (256 KB) at once for efficiency, in `LocalBufferContext` (a long-lived memory context).

## Interaction with the Buffer Manager

The main buffer manager functions dispatch to local variants based on the buffer number sign:

```c
/* From MarkBufferDirty() */
if (BufferIsLocal(buffer))
{
    MarkLocalBufferDirty(buffer);
    return;
}

/* From LockBuffer() */
if (BufferIsLocal(buffer))
    return;  /* local buffers need no lock */

/* From ReleaseBuffer() */
if (BufferIsLocal(buffer))
{
    UnpinLocalBuffer(buffer);
    return;
}
```

## Transaction Cleanup

- **AtEOXact_LocalBuffers()**: In debug builds, checks that no local buffers remain pinned.
- **AtProcExit_LocalBuffers()**: Drops all local buffer resources during backend shutdown.

## Performance Characteristics

Local buffers are significantly faster than shared buffers for temporary table operations:

- **No spinlocks**: State modifications use plain reads/writes.
- **No LWLocks**: Content locking is skipped entirely.
- **No CAS loops**: Reference counting is a simple integer increment.
- **No WAL**: Dirty pages are written directly without [WAL logging](10_wal_integration.md).
- **No hash partition locks**: The local hash table has no contention.

The trade-off is that local buffer pool size is typically small (`num_temp_buffers`, default 8 MB = 1024 buffers), and there is no cross-backend sharing. See [GUC Parameters](appendix_guc_parameters.md) for `num_temp_buffers`.

---

[<< Data Movement and Durability](12_data_movement_and_durability.md) | [Index](index.md) | [Next: Access Method Integration >>](14_access_method_integration.md)
