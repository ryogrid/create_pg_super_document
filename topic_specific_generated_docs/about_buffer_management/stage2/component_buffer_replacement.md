# Buffer Replacement Policy (Clock Sweep)

## Overview

PostgreSQL uses a clock-sweep algorithm as its buffer replacement policy, augmented by a free list for freshly-initialized buffers and a ring buffer mechanism for bulk operations. The replacement policy is implemented in `src/backend/storage/buffer/freelist.c` (817 lines) and is invoked by `BufferAlloc()` when a cache miss requires allocating a buffer slot.

The key design goals are:
- **Low overhead**: The clock hand is a single atomic counter requiring no per-buffer timestamps.
- **Approximation of LRU**: Usage counts provide a coarse approximation of recency, where frequently-accessed pages survive more sweep passes.
- **Minimal contention**: Most operations use atomic instructions or brief spinlock holds.
- **Bulk operation isolation**: Ring buffers prevent sequential scans and VACUUM from evicting hot pages.

## BufferStrategyControl

Source: `src/backend/storage/buffer/freelist.c:30-62`

The shared control structure for the replacement strategy:

```c
typedef struct
{
    slock_t     buffer_strategy_lock;
    pg_atomic_uint32 nextVictimBuffer;  /* clock sweep hand */
    int         firstFreeBuffer;        /* head of free list */
    int         lastFreeBuffer;         /* tail of free list */
    uint32      completePasses;         /* full clock sweep cycles */
    pg_atomic_uint32 numBufferAllocs;   /* allocation count since reset */
    int         bgwprocno;              /* bgwriter proc for wakeup, or -1 */
} BufferStrategyControl;
```

### Key Fields

| Field | Description |
|-------|-------------|
| `nextVictimBuffer` | Atomic counter incremented by `ClockSweepTick()`. Modulo NBuffers gives the current buffer index. |
| `firstFreeBuffer` / `lastFreeBuffer` | Singly-linked free list of unused buffers, linked via `BufferDesc.freeNext`. |
| `completePasses` | Number of complete sweeps through the buffer pool. Used by `BgBufferSync()` to track sweep progress. |
| `numBufferAllocs` | Counter of buffer allocations since last reset. Read and reset by `StrategySyncStart()` to inform the background writer. |
| `bgwprocno` | Process number of the background writer, used for wakeup notification. |

## Clock Sweep Algorithm

### ClockSweepTick()

Source: `src/backend/storage/buffer/freelist.c:107-164`

Atomically advances the clock hand by one position:

```c
static inline uint32
ClockSweepTick(void)
{
    uint32 victim;
    victim = pg_atomic_fetch_add_u32(&StrategyControl->nextVictimBuffer, 1);
    if (victim >= NBuffers)
    {
        victim = victim % NBuffers;
        /* On wraparound, update completePasses under spinlock */
        if (victim == 0)
        {
            /* CAS loop to reset nextVictimBuffer and increment completePasses */
            SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
            /* ... CAS to wrap nextVictimBuffer ... */
            StrategyControl->completePasses++;
            SpinLockRelease(&StrategyControl->buffer_strategy_lock);
        }
    }
    return victim;
}
```

The atomic `fetch_add` means multiple backends can advance the hand concurrently. Buffers may be returned slightly out of order, but this is harmless for the algorithm's correctness.

### StrategyGetBuffer()

Source: `src/backend/storage/buffer/freelist.c:195-357`

```c
BufferDesc *StrategyGetBuffer(BufferAccessStrategy strategy,
                              uint32 *buf_state, bool *from_ring)
```

The main victim selection function. Returns a buffer with the header spinlock held (the caller, `GetVictimBuffer`, must pin it before releasing the spinlock).

**Algorithm:**

**Phase 1: Check ring buffer** (if strategy is non-NULL):

```c
if (strategy != NULL)
{
    buf = GetBufferFromRing(strategy, buf_state);
    if (buf != NULL)
    {
        *from_ring = true;
        return buf;
    }
}
```

**Phase 2: Wake background writer** (if registered):

```c
bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
if (bgwprocno != -1)
{
    StrategyControl->bgwprocno = -1;
    SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
}
```

**Phase 3: Count allocation** (for bgwriter rate estimation):

```c
pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);
```

**Phase 4: Try free list**:

```c
if (StrategyControl->firstFreeBuffer >= 0)
{
    while (true)
    {
        SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
        if (StrategyControl->firstFreeBuffer < 0)
        {
            SpinLockRelease(&StrategyControl->buffer_strategy_lock);
            break;
        }
        buf = GetBufferDescriptor(StrategyControl->firstFreeBuffer);
        StrategyControl->firstFreeBuffer = buf->freeNext;
        buf->freeNext = FREENEXT_NOT_IN_LIST;
        SpinLockRelease(&StrategyControl->buffer_strategy_lock);

        local_buf_state = LockBufHdr(buf);
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
            && BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0)
        {
            if (strategy != NULL)
                AddBufferToRing(strategy, buf);
            *buf_state = local_buf_state;
            return buf;
        }
        UnlockBufHdr(buf, local_buf_state);
    }
}
```

Buffers from the free list that are pinned or have non-zero usage count are discarded (this can happen if another backend raced to use the buffer).

**Phase 5: Clock sweep**:

```c
trycounter = NBuffers;
for (;;)
{
    buf = GetBufferDescriptor(ClockSweepTick());
    local_buf_state = LockBufHdr(buf);

    if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
    {
        if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
        {
            /* Decrement usage count and move on */
            local_buf_state -= BUF_USAGECOUNT_ONE;
            trycounter = NBuffers;
        }
        else
        {
            /* Found a victim: refcount=0, usagecount=0 */
            if (strategy != NULL)
                AddBufferToRing(strategy, buf);
            *buf_state = local_buf_state;
            return buf;
        }
    }
    else if (--trycounter == 0)
    {
        UnlockBufHdr(buf, local_buf_state);
        elog(ERROR, "no unpinned buffers available");
    }
    UnlockBufHdr(buf, local_buf_state);
}
```

The algorithm scans buffers circularly:
- **Unpinned, usage > 0**: Decrement usage count and continue scanning.
- **Unpinned, usage = 0**: Select as victim.
- **Pinned**: Skip and decrement trycounter. If all NBuffers are pinned, raise ERROR.

### Usage Count Semantics

The usage count (4 bits, range 0-15, capped at `BM_MAX_USAGE_COUNT = 5`) approximates how "hot" a page is:

- Incremented each time a buffer is pinned (up to the max of 5).
- Decremented by 1 each time the clock sweep passes over it.
- A buffer with usage count 0 and refcount 0 is eligible for eviction.

The maximum of 5 means a frequently-used page can survive up to 6 complete clock sweeps before being evicted. This is a deliberate trade-off: a larger value would better approximate LRU but would increase the time to find a free buffer under memory pressure.

## Free List

The free list is a singly-linked list of buffers that have never been used (at startup) or have been explicitly returned (by `StrategyFreeBuffer()`). It is checked before the clock sweep to provide fast allocation for fresh buffers.

### StrategyFreeBuffer()

Source: `src/backend/storage/buffer/freelist.c:362-380`

```c
void StrategyFreeBuffer(BufferDesc *buf)
{
    SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
    if (buf->freeNext == FREENEXT_NOT_IN_LIST)
    {
        buf->freeNext = StrategyControl->firstFreeBuffer;
        if (buf->freeNext < 0)
            StrategyControl->lastFreeBuffer = buf->buf_id;
        StrategyControl->firstFreeBuffer = buf->buf_id;
    }
    SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}
```

Called when `BufferAlloc()` loses a race condition (another backend inserted the same tag) and needs to return the victim buffer to the pool.

## Ring Buffer Strategies

Ring buffers are backend-private buffer pools used by bulk operations to avoid polluting the main buffer cache. They confine the working set of a large scan to a small ring of buffers.

### BufferAccessStrategyData

Source: `src/backend/storage/buffer/freelist.c:72-92`

```c
typedef struct BufferAccessStrategyData
{
    BufferAccessStrategyType btype;
    int         nbuffers;       /* ring size */
    int         current;        /* current slot index */
    Buffer      buffers[FLEXIBLE_ARRAY_MEMBER];  /* ring of buffer numbers */
} BufferAccessStrategyData;
```

### Strategy Types and Ring Sizes

| Strategy | Ring Size | Use Case |
|----------|-----------|----------|
| `BAS_NORMAL` | N/A (returns NULL) | Default -- no ring |
| `BAS_BULKREAD` | 256 KB (32 buffers) | Sequential scans |
| `BAS_BULKWRITE` | 16 MB (2048 buffers) | COPY IN, CREATE TABLE AS |
| `BAS_VACUUM` | 2 MB (256 buffers) | VACUUM |

All ring sizes are capped at 1/8 of `shared_buffers`:

```c
/* From GetAccessStrategyWithSize(), freelist.c:599 */
ring_buffers = Min(NBuffers / 8, ring_buffers);
```

### GetBufferFromRing()

Source: `src/backend/storage/buffer/freelist.c:694-739`

```c
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
                                     uint32 *buf_state)
```

Attempts to reuse a buffer from the ring:

1. Advance to the next ring slot.
2. If the slot is empty (`InvalidBuffer`), return NULL to trigger normal allocation (which will fill this slot via `AddBufferToRing()`).
3. If the buffer in the slot is unpinned and has usage count <= 1, return it.
4. If pinned or usage count > 1 (another backend touched it), return NULL.

```c
buf = GetBufferDescriptor(bufnum - 1);
local_buf_state = LockBufHdr(buf);
if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
    && BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1)
{
    *buf_state = local_buf_state;
    return buf;
}
```

### StrategyRejectBuffer()

Source: `src/backend/storage/buffer/freelist.c:797-816`

```c
bool StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf,
                          bool from_ring)
```

Only applies to `BAS_BULKREAD`. If a ring buffer is dirty and flushing it would require a WAL flush, the buffer is removed from the ring (set to `InvalidBuffer`) and the function returns `true`, causing `GetVictimBuffer()` to select a different victim. This prevents sequential scans from being slowed by WAL flush overhead.

For `BAS_BULKWRITE` and `BAS_VACUUM`, dirty ring buffers are flushed normally (their ring sizes are large enough to amortize the cost).

### Ring Buffer Behavior Summary

| Scenario | BAS_BULKREAD | BAS_BULKWRITE | BAS_VACUUM |
|----------|-------------|---------------|------------|
| Clean ring buffer | Reuse | Reuse | Reuse |
| Dirty, no WAL flush needed | Flush + reuse | Flush + reuse | Flush + reuse |
| Dirty, WAL flush needed | Reject (evict from ring) | Flush + reuse | Flush + reuse |
| Buffer pinned by other | Allocate new via clock sweep | Allocate new via clock sweep | Allocate new via clock sweep |

## Background Writer Coordination

### StrategySyncStart()

Source: `src/backend/storage/buffer/freelist.c:393-420`

```c
int StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
```

Called by `BgBufferSync()` to determine the current clock sweep position and the number of recent buffer allocations. The allocation count is atomically exchanged to zero, providing rate information for the background writer's adaptive scan.

### StrategyNotifyBgWriter()

Source: `src/backend/storage/buffer/freelist.c:431-441`

```c
void StrategyNotifyBgWriter(int bgwprocno)
```

Registers the background writer's process number so that `StrategyGetBuffer()` will wake it up (via `SetLatch()`) when the next buffer allocation occurs. This implements the hibernation/wakeup protocol: when the system is idle, the background writer sleeps until a backend needs a buffer.

## Initialization

### StrategyInitialize()

Source: `src/backend/storage/buffer/freelist.c:473-526`

Called by `InitBufferPool()` during postmaster startup. Initializes:

1. The partitioned hash table via `InitBufTable()`.
2. The free list (initially contains all buffers: 0 through NBuffers-1, linked by `InitBufferPool()`).
3. The clock sweep hand at position 0.
4. Statistics counters at zero.
5. The background writer notification to "none" (-1).
