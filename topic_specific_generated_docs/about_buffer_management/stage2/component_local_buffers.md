# Local Buffers for Temporary Tables

## Overview

Local buffers are a backend-private buffer pool used exclusively for temporary table pages. Because temporary tables are only visible to the creating backend and are not preserved across crashes, local buffers eliminate the overhead of shared memory synchronization: no locking, no WAL logging, and no shared reference counting. Local buffers are implemented in `src/backend/storage/buffer/localbuf.c`.

## Design Rationale

| Property | Shared Buffers | Local Buffers |
|----------|---------------|---------------|
| Visibility | All backends | Single backend |
| Crash recovery | Yes (WAL-logged) | No |
| Locking | Content locks + header spinlocks | None |
| Reference counting | Shared atomic + private | Private only (`LocalRefCount`) |
| Memory location | Shared memory | Backend-local memory |
| Buffer numbering | Positive (1..NBuffers) | Negative (-1..-NLocBuffer) |

Local buffers use the same `BufferDesc` structure as shared buffers, but the atomic state operations are simplified to plain reads and writes (`pg_atomic_read_u32()` and `pg_atomic_unlocked_write_u32()`), since no concurrent access is possible.

## Data Structures

### Global Variables

Source: `src/include/storage/bufmgr.h:179-182`

```c
extern PGDLLIMPORT int NLocBuffer;
extern PGDLLIMPORT Block *LocalBufferBlockPointers;
extern PGDLLIMPORT int32 *LocalRefCount;
```

Source: `src/include/storage/buf_internals.h:315`

```c
extern PGDLLIMPORT BufferDesc *LocalBufferDescriptors;
```

| Variable | Type | Description |
|----------|------|-------------|
| `NLocBuffer` | `int` | Number of local buffer slots (default: 64) |
| `LocalBufferDescriptors` | `BufferDesc *` | Array of local buffer descriptors |
| `LocalBufferBlockPointers` | `Block *` | Array of pointers to page data |
| `LocalRefCount` | `int32 *` | Per-buffer reference counts |

### Local Hash Table

A backend-private hash table (`LocalBufHash`) maps `BufferTag` to local buffer IDs, analogous to the shared `SharedBufHash` but without partition locks.

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

The local equivalent of `BufferAlloc()`. Looks up the requested page in the local hash table and either returns an existing buffer (cache hit) or allocates a new one (cache miss).

**Cache miss handling:**

Unlike shared buffers, local buffer replacement uses a simple clock-sweep through the local descriptor array. When a victim is found (refcount = 0, usage count = 0), it is evicted:

1. If the victim is dirty, write it to disk immediately via `smgrwrite()`. No WAL flush is needed.
2. Remove the old hash table entry.
3. Assign the new tag and insert into the hash table.

### PinLocalBuffer()

Source: `src/backend/storage/buffer/localbuf.c`

```c
bool PinLocalBuffer(BufferDesc *buf_hdr, bool adjust_usagecount)
```

Increments the local reference count. If `adjust_usagecount` is true, also increments the buffer's usage count (up to `BM_MAX_USAGE_COUNT`). Uses plain (non-atomic) state manipulation:

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

### UnpinLocalBuffer()

```c
void UnpinLocalBuffer(Buffer buffer)
```

Decrements the local reference count. If it reaches zero, the buffer becomes eligible for replacement.

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

Source: `src/backend/storage/buffer/localbuf.c`

```c
static void InitLocalBuffers(void)
```

Lazily initialized on the first access to a local buffer. Allocates:

1. `LocalBufferDescriptors`: Array of `NLocBuffer` `BufferDesc` structures (NOT padded to cache lines, since there is no false-sharing concern).
2. `LocalBufferBlockPointers`: Array of `NLocBuffer` block pointers (initially NULL).
3. `LocalRefCount`: Array of `NLocBuffer` `int32` reference counts (initially 0).
4. `LocalBufHash`: Private hash table for tag-to-buffer mapping.

Buffer descriptors are initialized with cleared tags, zero state, and a freelist linked list.

### GetLocalBufferStorage()

Source: `src/backend/storage/buffer/localbuf.c`

```c
static Block GetLocalBufferStorage(void)
```

Allocates page data storage for local buffers. Rather than allocating one page at a time, this function allocates blocks of 32 pages (256 KB) at once for efficiency. The bulk allocation reduces the number of `malloc()` calls and improves memory locality.

Storage is allocated in `LocalBufferContext`, a long-lived memory context that persists for the backend's lifetime.

## Interaction with the Buffer Manager

The main buffer manager functions (`ReadBuffer`, `MarkBufferDirty`, `ReleaseBuffer`, `LockBuffer`) dispatch to local variants based on the buffer number sign:

```c
/* From MarkBufferDirty() */
if (BufferIsLocal(buffer))
{
    MarkLocalBufferDirty(buffer);
    return;
}
```

```c
/* From LockBuffer() */
if (BufferIsLocal(buffer))
    return;  /* local buffers need no lock */
```

```c
/* From ReleaseBuffer() */
if (BufferIsLocal(buffer))
{
    UnpinLocalBuffer(buffer);
    return;
}
```

## Transaction Cleanup

### AtEOXact_LocalBuffers()

Called at end of transaction. In debug builds, checks that no local buffers remain pinned (which would indicate a leak). In release builds, this is essentially a no-op since local buffer pins should already be released.

### AtProcExit_LocalBuffers()

Called during backend shutdown. Drops all local buffer resources.

## Performance Characteristics

Local buffers are significantly faster than shared buffers for temporary table operations:

- **No spinlocks**: State modifications use plain reads/writes.
- **No LWLocks**: Content locking is skipped entirely.
- **No CAS loops**: Reference counting is a simple integer increment.
- **No WAL**: Dirty pages are written directly without WAL logging.
- **No hash partition locks**: The local hash table has no contention.

The trade-off is that local buffer pool size is typically small (`num_temp_buffers`, default 8 MB = 1024 buffers), and there is no cross-backend sharing of cached temporary table pages.
