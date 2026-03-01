# Page-Level Concurrency Control

[<< Buffer Access Protocol](05_buffer_access_protocol.md) | [Index](index.md) | [Next: Buffer Replacement Policy >>](07_buffer_replacement_policy.md)

---

## Overview

PostgreSQL uses a multi-layered locking scheme for buffer access, designed to minimize contention while maintaining correctness. There are four distinct lock types, each serving a different purpose and operating at a different granularity.

All functions are in `src/backend/storage/buffer/bufmgr.c` unless otherwise noted.

See diagram: [lock_hierarchy.mermaid](../diagrams/lock_hierarchy.mermaid)
See diagram: [pin_lock_protocol.mermaid](../diagrams/pin_lock_protocol.mermaid)

## Lock Taxonomy

### 1. Buffer Pin (Reference Count)

**Purpose:** Prevent buffer eviction during use.

**Mechanism:** Atomic increment/decrement of the refcount field in the [buffer state word](03_buffer_pool_architecture.md) (bits 0-17). Each backend also maintains a private refcount array to avoid modifying shared state for repeated pins on the same buffer.

**Duration:** Held for the duration of a buffer access, typically from `ReadBuffer()` to `ReleaseBuffer()`.

**Key functions:** `PinBuffer()`, `UnpinBuffer()`, `ReleaseBuffer()`

### 2. Buffer Content Lock (LWLock)

**Purpose:** Control concurrent read/write access to buffer page data.

**Mechanism:** LWLock embedded in the [BufferDesc](03_buffer_pool_architecture.md) structure (`content_lock` field). Supports shared (read) and exclusive (write) modes.

**Duration:** Short-term. Should not be held across I/O operations or for extended computation.

**Key functions:** `LockBuffer()`, `ConditionalLockBuffer()`, `LockBufferForCleanup()`

### 3. Buffer Header Spinlock (BM_LOCKED)

**Purpose:** Protect buffer descriptor metadata (tag, state flags, wait fields).

**Mechanism:** The `BM_LOCKED` bit (bit 22) in the atomic state word. Acquired via `pg_atomic_fetch_or_u32()` with spin-wait.

**Duration:** Extremely short -- only a few instructions. No other locks, I/O, or memory allocation should occur while held.

**Key functions:** `LockBufHdr()`, `UnlockBufHdr()`

### 4. I/O Lock (BM_IO_IN_PROGRESS)

**Purpose:** Serialize disk I/O operations on a buffer. Ensures only one backend reads or writes a given buffer at a time.

**Mechanism:** The `BM_IO_IN_PROGRESS` flag (bit 26) in the state word, coordinated via a per-buffer condition variable (`BufferIOCVArray`).

**Duration:** Held for the duration of one I/O operation (read or write).

**Key functions:** `StartBufferIO()`, `TerminateBufferIO()`, `WaitIO()`

## Pin Management

### PinBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:2617`

```c
static bool PinBuffer(BufferDesc *buf, BufferAccessStrategy strategy)
```

Pins a shared buffer using a lock-free CAS loop.

**Fast path (already pinned by this backend):**

```c
ref = GetPrivateRefCountEntry(b, true);
if (ref != NULL)
{
    result = (pg_atomic_read_u32(&buf->state) & BM_VALID) != 0;
    ref->refcount++;
    ResourceOwnerRememberBuffer(CurrentResourceOwner, b);
    return result;
}
```

No shared state modification is needed. This is extremely fast.

**Slow path (first pin by this backend):**

Uses a CAS loop to atomically increment the shared refcount and update the usage count. Key design points:

- **Lock-free**: Uses CAS rather than the buffer header spinlock.
- **Strategy-aware**: Default strategy increments usage count (up to `BM_MAX_USAGE_COUNT = 5`), but [ring strategies](07_buffer_replacement_policy.md) cap at 1 to avoid inflating counts for sequential scans.
- **Spinlock-safe**: Waits for `BM_LOCKED` to clear before attempting CAS.

### Private Refcount Optimization

Source: `src/backend/storage/buffer/bufmgr.c:177`

```c
typedef struct PrivateRefCountEntry
{
    Buffer      buffer;
    int32       refcount;
} PrivateRefCountEntry;

#define REFCOUNT_ARRAY_ENTRIES 8
static struct PrivateRefCountEntry PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES];
static HTAB *PrivateRefCountHash = NULL;
```

Each backend maintains a small array of 8 entries (64 bytes, one cache line) for tracking pinned buffers. In the vast majority of cases, a backend pins fewer than 8 distinct buffers simultaneously. When more than 8 buffers are pinned, overflow entries are displaced into a hash table.

### UnpinBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:2803`

Decrements the private refcount. When it reaches zero, decrements the shared refcount via CAS loop. Checks for `BM_PIN_COUNT_WAITER` flag -- if set and refcount reaches 1, signals the waiting backend (used by `LockBufferForCleanup()`).

## Content Lock Management

### LockBuffer()

Source: `src/backend/storage/buffer/bufmgr.c:5132`

```c
void LockBuffer(Buffer buffer, int mode)
```

Acquires or releases the content lock on a buffer. The buffer must be pinned.

| Constant | Value | LWLock Mode | Use Case |
|----------|-------|-------------|----------|
| `BUFFER_LOCK_UNLOCK` | 0 | Release | Drop lock |
| `BUFFER_LOCK_SHARE` | 1 | LW_SHARED | Read page contents |
| `BUFFER_LOCK_EXCLUSIVE` | 2 | LW_EXCLUSIVE | Modify page contents |

**Important:** [Local buffers](13_local_buffers.md) skip locking entirely (no concurrent access possible).

### ConditionalLockBuffer()

```c
bool ConditionalLockBuffer(Buffer buffer)
```

Attempts to acquire a shared content lock without blocking. Returns `true` on success. Used by [GetVictimBuffer()](05_buffer_access_protocol.md) to avoid deadlock when flushing dirty victims.

### LockBufferForCleanup()

Source: `src/backend/storage/buffer/bufmgr.c:5195`

```c
void LockBufferForCleanup(Buffer buffer)
```

Acquires an exclusive content lock AND waits for all other backends to unpin the buffer (pin count = 1). This is needed for operations that physically remove tuples from a page ([VACUUM](14_access_method_integration.md) delete phase, btree page deletion).

**Protocol:**

```
loop:
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)
    buf_state = LockBufHdr(bufHdr)
    if refcount == 1:
        UnlockBufHdr -> return  /* success */

    /* Set wait flag */
    bufHdr->wait_backend_pgprocno = MyProcNumber
    buf_state |= BM_PIN_COUNT_WAITER
    UnlockBufHdr(bufHdr, buf_state)
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK)

    /* Wait for signal from UnpinBuffer() */
    ProcWaitForSignal(WAIT_EVENT_BUFFER_PIN)
    goto loop
```

**Key constraints:**
- Only one backend can wait for pin-count-1 on a given buffer at a time.
- The caller must hold exactly one pin on the buffer.
- In hot standby, the startup process may need to cancel conflicting queries to proceed.

## Buffer Header Spinlock

### LockBufHdr()

Source: `src/backend/storage/buffer/bufmgr.c:5735`

```c
uint32 LockBufHdr(BufferDesc *desc)
```

Acquires the buffer header spinlock by setting the `BM_LOCKED` bit via atomic fetch-or, with progressive spin-wait backoff:

```c
init_local_spin_delay(&delayStatus);
while (true)
{
    old_buf_state = pg_atomic_fetch_or_u32(&desc->state, BM_LOCKED);
    if (!(old_buf_state & BM_LOCKED))
        break;
    perform_spin_delay(&delayStatus);
}
finish_spin_delay(&delayStatus);
return old_buf_state | BM_LOCKED;
```

**Returns:** The state word with `BM_LOCKED` set.

### UnlockBufHdr()

Source: `src/include/storage/buf_internals.h`

```c
static inline void
UnlockBufHdr(BufferDesc *desc, uint32 buf_state)
{
    pg_write_barrier();
    pg_atomic_write_u32(&desc->state, buf_state & (~BM_LOCKED));
}
```

The write barrier ensures all prior memory stores are visible before the lock is released.

## I/O Coordination

### StartBufferIO()

Source: `src/backend/storage/buffer/bufmgr.c:5507`

```c
static bool StartBufferIO(BufferDesc *buf, bool forInput, bool nowait)
```

Claims the I/O lock on a buffer by setting `BM_IO_IN_PROGRESS`. Multiple backends may attempt the same I/O; this function serializes them. If another backend already completed the I/O (buffer now valid for reads, or clean for writes), returns `false`.

### TerminateBufferIO()

Source: `src/backend/storage/buffer/bufmgr.c:5568`

```c
static void TerminateBufferIO(BufferDesc *buf, bool clear_dirty,
                              uint32 set_flag_bits, bool forget_owner)
```

Clears `BM_IO_IN_PROGRESS` and wakes all waiters via `ConditionVariableBroadcast()`. The `BM_JUST_DIRTIED` check prevents clearing the dirty flag if the buffer was re-dirtied during the write operation. See [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) for the `BM_JUST_DIRTIED` protocol.

## Lock Ordering Rules

From `src/backend/storage/buffer/README`:

1. **Partition lock before buffer header spinlock**: Never acquire a [partition lock](04_buffer_lookup_and_hashtable.md) while holding a buffer header spinlock.

2. **Content lock before I/O operations**: Always lock buffer contents before starting I/O. The content lock is held shared during writes to ensure page consistency.

3. **Never hold buffer header spinlock during I/O**: The spinlock is for metadata only and must be extremely short-lived.

4. **No nested buffer content locks**: A single backend must not try to acquire multiple content locks on the same buffer.

5. **Partition locks in partition-number order**: When multiple [partition locks](04_buffer_lookup_and_hashtable.md) are needed, acquire them in ascending partition number to prevent deadlock.

6. **Pin before lock**: Always pin a buffer before attempting to lock it.

## Lock Traces for Common Operations

### Heap Tuple Read

```
ReadBuffer()
  -> BufferAlloc()
       LWLockAcquire(partition_lock, LW_SHARED)      -- hash lookup
       PinBuffer() via CAS                             -- pin (atomic)
       LWLockRelease(partition_lock)
  <- buffer pinned

LockBuffer(buf, BUFFER_LOCK_SHARE)                    -- content lock (shared)
  ... read tuple data ...
LockBuffer(buf, BUFFER_LOCK_UNLOCK)                    -- release content lock
ReleaseBuffer(buf)                                     -- unpin
```

### Heap Tuple Insert

```
ReadBuffer() or ExtendBufferedRel()
  -> BufferAlloc() [same as above]

LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE)                 -- content lock (exclusive)
  PageAddItemExtended()                                -- add tuple to page
  MarkBufferDirty()                                    -- set BM_DIRTY via CAS
  XLogInsert()                                         -- WAL record
  PageSetLSN()                                         -- update page LSN
LockBuffer(buf, BUFFER_LOCK_UNLOCK)
ReleaseBuffer(buf)
```

### VACUUM Tuple Deletion

```
ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, strategy)
  -> BufferAlloc() with BAS_VACUUM strategy

LockBufferForCleanup(buf)                              -- exclusive + wait pin=1
  heap_page_prune()                                    -- remove dead tuples
  PageRepairFragmentation()                            -- compact page
  MarkBufferDirty()
  XLogInsert()
  PageSetLSN()
LockBuffer(buf, BUFFER_LOCK_UNLOCK)
ReleaseBuffer(buf)
```

### Buffer Eviction (in GetVictimBuffer)

```
StrategyGetBuffer()
  LockBufHdr()                                         -- spinlock to check state
  ... clock sweep: decrement usage counts ...
  <- return victim with spinlock held

PinBuffer_Locked()                                     -- pin while holding spinlock
  UnlockBufHdr()                                       -- release spinlock

[if dirty:]
  LWLockConditionalAcquire(content_lock, LW_SHARED)   -- shared lock for flush
  FlushBuffer()
    StartBufferIO()
      LockBufHdr() / UnlockBufHdr()                    -- set BM_IO_IN_PROGRESS
    LockBufHdr() / UnlockBufHdr()                      -- read LSN
    XLogFlush()                                        -- WAL-before-data
    smgrwrite()                                        -- write page
    TerminateBufferIO()
      LockBufHdr() / UnlockBufHdr()                    -- clear flags
      ConditionVariableBroadcast()                     -- wake waiters
  LWLockRelease(content_lock)

InvalidateVictimBuffer()
  LWLockAcquire(partition_lock, LW_EXCLUSIVE)          -- exclusive for delete
  LockBufHdr()                                         -- check state
  BufTableDelete()                                     -- remove from hash table
  UnlockBufHdr()                                       -- clear tag
  LWLockRelease(partition_lock)
```

See also: [Access Method Integration](14_access_method_integration.md) for more per-operation lock traces.

---

[<< Buffer Access Protocol](05_buffer_access_protocol.md) | [Index](index.md) | [Next: Buffer Replacement Policy >>](07_buffer_replacement_policy.md)
