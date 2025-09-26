# BufferStrategyControl

## Location
[src/backend/storage/buffer/freelist.c:62-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L62-L71)

## Overview
BufferStrategyControl is a shared memory control structure that manages PostgreSQL's buffer replacement strategy, implementing the clock sweep algorithm for buffer allocation and maintaining statistics about buffer pool usage.

## Definition
```c
typedef struct
{
    /* Spinlock: protects the values below */
    slock_t         buffer_strategy_lock;

    /*
     * Clock sweep hand: index of next buffer to consider grabbing. Note that
     * this isn't a concrete buffer - we only ever increase the value. So, to
     * get an actual buffer, it needs to be used modulo NBuffers.
     */
    pg_atomic_uint32 nextVictimBuffer;

    int            firstFreeBuffer;    /* Head of list of unused buffers */
    int            lastFreeBuffer;     /* Tail of list of unused buffers */

    /*
     * Statistics.  These counters should be wide enough that they can't
     * overflow during a single bgwriter cycle.
     */
    uint32        completePasses;     /* Complete cycles of the clock sweep */
    pg_atomic_uint32 numBufferAllocs;    /* Buffers allocated since last reset */

    /*
     * Bgworker process to be notified upon activity or -1 if none. See
     * StrategyNotifyBgWriter.
     */
    int            bgwprocno;
} BufferStrategyControl;
```

## Detailed Description
BufferStrategyControl is the central data structure for PostgreSQL's buffer management system, specifically for implementing the clock sweep replacement algorithm. This structure is allocated in shared memory and coordinates buffer allocation across all backend processes. The structure maintains both operational state (such as the clock sweep position and free buffer lists) and statistical information about buffer usage patterns.

The clock sweep algorithm is PostgreSQL's implementation of an approximation to LRU (Least Recently Used) replacement. The nextVictimBuffer field acts as a 'clock hand' that continuously advances through the buffer pool, looking for buffers that can be evicted. The structure also maintains a linked list of completely free buffers for fast allocation when possible.

All access to this structure must be protected by the buffer_strategy_lock spinlock to ensure consistency in multi-process environments. The atomic fields (nextVictimBuffer and numBufferAllocs) can be accessed with atomic operations for better performance in high-concurrency scenarios.

## Parameters / Member Variables
- `buffer_strategy_lock`: Spinlock that protects all non-atomic fields in this structure from concurrent access
- `nextVictimBuffer`: Atomic counter representing the clock sweep hand position, continuously incremented and used modulo NBuffers to find the actual buffer index
- `firstFreeBuffer`: Index of the first buffer in the linked list of completely unused buffers, or -1 if no free buffers exist
- `lastFreeBuffer`: Index of the last buffer in the free list, undefined when firstFreeBuffer is -1
- `completePasses`: Statistical counter tracking the number of complete clock sweep cycles through the entire buffer pool
- `numBufferAllocs`: Atomic counter tracking the total number of buffer allocations since the last reset, used for performance monitoring
- `bgwprocno`: Process number of the background writer process to notify of buffer activity, or -1 if none

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md) (spinlock type)
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (atomic integer type)
  - uint32 (standard integer type)

- Called from (representative examples):
  - [StrategyShmemSize](../S/StrategyShmemSize.md) (calculates shared memory requirements)
  - [StrategyInitialize](../S/StrategyInitialize.md) (initializes the structure in shared memory)

## Notes and Other Information
- This structure is allocated in shared memory using ShmemInitStruct with the name "Buffer Strategy Status"
- The structure is accessed through a static global pointer StrategyControl in freelist.c
- The nextVictimBuffer field continuously increases and wraps around the buffer pool using modulo arithmetic
- Free buffer management uses a simple linked list structure within the buffer pool itself
- Statistics fields are designed to be wide enough to prevent overflow during normal background writer cycles
- The bgwprocno field enables efficient notification of the background writer when buffer activity occurs
- All buffer replacement strategy logic in PostgreSQL centers around this control structure