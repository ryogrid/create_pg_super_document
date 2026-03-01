# Buffer Replacement Policy (Clock Sweep)

[<< Page Concurrency Control](06_page_concurrency_control.md) | [Index](index.md) | [Next: Page Layout and Types >>](08_page_layout_and_types.md)

---

## Overview

PostgreSQL uses a clock-sweep algorithm as its buffer replacement policy, augmented by a free list for freshly-initialized buffers and a ring buffer mechanism for bulk operations. The replacement policy is implemented in `src/backend/storage/buffer/freelist.c` (817 lines) and is invoked by [BufferAlloc()](05_buffer_access_protocol.md) when a cache miss requires allocating a buffer slot.

The key design goals are:
- **Low overhead**: The clock hand is a single atomic counter requiring no per-buffer timestamps.
- **Approximation of LRU**: Usage counts provide a coarse approximation of recency.
- **Minimal contention**: Most operations use atomic instructions or brief spinlock holds.
- **Bulk operation isolation**: Ring buffers prevent sequential scans and VACUUM from evicting hot pages.

See diagram: [clock_sweep.mermaid](../diagrams/clock_sweep.mermaid)

## BufferStrategyControl

Source: `src/backend/storage/buffer/freelist.c:30`

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

| Field | Description |
|-------|-------------|
| `nextVictimBuffer` | Atomic counter incremented by `ClockSweepTick()`. Modulo NBuffers gives the current buffer index. |
| `firstFreeBuffer` / `lastFreeBuffer` | Singly-linked free list of unused buffers, linked via `BufferDesc.freeNext`. |
| `completePasses` | Number of complete sweeps through the buffer pool. Used by [BgBufferSync()](09_dirty_buffer_and_writeback.md) to track sweep progress. |
| `numBufferAllocs` | Counter of buffer allocations since last reset. Read and reset by `StrategySyncStart()` to inform the background writer. |
| `bgwprocno` | Process number of the background writer, used for wakeup notification. |

## Clock Sweep Algorithm

### ClockSweepTick()

Source: `src/backend/storage/buffer/freelist.c:107`

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
        if (victim == 0)
        {
            SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
            StrategyControl->completePasses++;
            SpinLockRelease(&StrategyControl->buffer_strategy_lock);
        }
    }
    return victim;
}
```

The atomic `fetch_add` means multiple backends can advance the hand concurrently. Buffers may be returned slightly out of order, but this is harmless for the algorithm's correctness.

### StrategyGetBuffer()

Source: `src/backend/storage/buffer/freelist.c:195`

```c
BufferDesc *StrategyGetBuffer(BufferAccessStrategy strategy,
                              uint32 *buf_state, bool *from_ring)
```

The main victim selection function. Returns a buffer with the header spinlock held (the caller, [GetVictimBuffer()](05_buffer_access_protocol.md), must pin it before releasing the spinlock).

**Algorithm:**

**Phase 1: Check ring buffer** (if strategy is non-NULL):

```c
if (strategy != NULL)
{
    buf = GetBufferFromRing(strategy, buf_state);
    if (buf != NULL) { *from_ring = true; return buf; }
}
```

**Phase 2: Wake background writer** (if registered):

Wakes the [background writer](09_dirty_buffer_and_writeback.md) via `SetLatch()` when the next buffer allocation occurs, implementing the hibernation/wakeup protocol.

**Phase 3: Count allocation** (for bgwriter rate estimation):

```c
pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);
```

**Phase 4: Try free list**:

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

The maximum of 5 means a frequently-used page can survive up to 6 complete clock sweeps before being evicted.

## Free List

The free list is a singly-linked list of buffers that have never been used (at startup) or have been explicitly returned (by `StrategyFreeBuffer()`). It is checked before the clock sweep to provide fast allocation for fresh buffers.

### StrategyFreeBuffer()

Source: `src/backend/storage/buffer/freelist.c:362`

```c
void StrategyFreeBuffer(BufferDesc *buf)
```

Returns a buffer to the free list. Called when [BufferAlloc()](05_buffer_access_protocol.md) loses a race condition (another backend inserted the same tag) and needs to return the victim buffer to the pool.

## Ring Buffer Strategies

Ring buffers are backend-private buffer pools used by bulk operations to avoid polluting the main buffer cache. They confine the working set of a large scan to a small ring of buffers.

See diagram: [ring_buffer_strategies.mermaid](../diagrams/ring_buffer_strategies.mermaid)

### BufferAccessStrategyData

Source: `src/backend/storage/buffer/freelist.c:72`

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
ring_buffers = Min(NBuffers / 8, ring_buffers);
```

### GetBufferFromRing()

Source: `src/backend/storage/buffer/freelist.c:694`

Attempts to reuse a buffer from the ring. If the buffer in the slot is unpinned and has usage count <= 1, returns it. If pinned or usage count > 1 (another backend touched it), returns NULL, triggering normal clock sweep allocation.

### StrategyRejectBuffer()

Source: `src/backend/storage/buffer/freelist.c:797`

Only applies to `BAS_BULKREAD`. If a ring buffer is dirty and flushing it would require a [WAL flush](10_wal_integration.md), the buffer is removed from the ring and the function returns `true`, causing [GetVictimBuffer()](05_buffer_access_protocol.md) to select a different victim.

For `BAS_BULKWRITE` and `BAS_VACUUM`, dirty ring buffers are flushed normally.

### Ring Buffer Behavior Summary

| Scenario | BAS_BULKREAD | BAS_BULKWRITE | BAS_VACUUM |
|----------|-------------|---------------|------------|
| Clean ring buffer | Reuse | Reuse | Reuse |
| Dirty, no WAL flush needed | Flush + reuse | Flush + reuse | Flush + reuse |
| Dirty, WAL flush needed | Reject (evict from ring) | Flush + reuse | Flush + reuse |
| Buffer pinned by other | Allocate new via clock sweep | Allocate new via clock sweep | Allocate new via clock sweep |

## Background Writer Coordination

### StrategySyncStart()

Source: `src/backend/storage/buffer/freelist.c:393`

```c
int StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
```

Called by [BgBufferSync()](09_dirty_buffer_and_writeback.md) to determine the current clock sweep position and the number of recent buffer allocations. The allocation count is atomically exchanged to zero, providing rate information for the background writer's adaptive scan.

### StrategyNotifyBgWriter()

Source: `src/backend/storage/buffer/freelist.c:431`

```c
void StrategyNotifyBgWriter(int bgwprocno)
```

Registers the background writer's process number so that `StrategyGetBuffer()` will wake it up when the next buffer allocation occurs.

## Initialization

### StrategyInitialize()

Source: `src/backend/storage/buffer/freelist.c:473`

Called by [InitBufferPool()](03_buffer_pool_architecture.md) during postmaster startup. Initializes:

1. The partitioned [hash table](04_buffer_lookup_and_hashtable.md) via `InitBufTable()`.
2. The free list (initially contains all buffers: 0 through NBuffers-1).
3. The clock sweep hand at position 0.
4. Statistics counters at zero.
5. The background writer notification to "none" (-1).

---

[<< Page Concurrency Control](06_page_concurrency_control.md) | [Index](index.md) | [Next: Page Layout and Types >>](08_page_layout_and_types.md)
