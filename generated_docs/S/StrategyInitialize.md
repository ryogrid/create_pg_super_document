# StrategyInitialize

## Location
[src/backend/storage/buffer/freelist.c:474-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L474-L540)

## Overview
Initializes the buffer cache replacement strategy, including the buffer lookup hashtable and shared strategy control structures.

## Definition
```c
void StrategyInitialize(bool init)
```

## Detailed Description
StrategyInitialize sets up the core infrastructure for PostgreSQL's buffer replacement strategy. It initializes both the shared buffer lookup hashtable and the BufferStrategyControl structure that manages the replacement algorithm state. The function handles both initial setup (when called by the postmaster during startup) and attachment to existing structures (when called by other processes).

The function assumes that all buffers have already been built into a linked list by InitBufferPool(). It creates a lookup hashtable sized to accommodate concurrent buffer operations across all partitions, ensuring no lookup failures due to table overflow. The strategy control block manages the clock-sweep algorithm state, free buffer list, and various statistics.

## Parameters / Member Variables
- `init`: Boolean flag indicating whether this is initial setup (true for postmaster) or attachment to existing structures (false for other processes)

## Dependencies
- Functions called/Symbols referenced:
  - [InitBufTable](../I/InitBufTable.md) (initializes buffer lookup hashtable)
  - [ShmemInitStruct](ShmemInitStruct.md) (creates or attaches to shared memory structure)
  - SpinLockInit (initializes spinlock for thread safety)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md) (initializes atomic counters)
  - NBuffers (global variable for buffer count)
  - NUM_BUFFER_PARTITIONS (partitioning constant)
  - [BufferStrategyControl](../B/BufferStrategyControl.md) (control structure type)
- Called from (representative examples):
  - [InitBufferPool](../I/InitBufferPool.md) (src/backend/storage/buffer/buf_init.c:146)

## Notes and Other Information
- Only called by the postmaster during initialization, and by other processes during shared memory attachment
- The hashtable is sized to handle concurrent operations: NBuffers + NUM_BUFFER_PARTITIONS entries
- Initializes the clock-sweep algorithm with nextVictimBuffer starting at 0
- Sets up the complete free buffer chain from 0 to NBuffers-1
- Clears all statistics and sets bgwprocno to -1 (no pending background writer notification)
- Uses Assert() to ensure proper initialization ordering between postmaster and other processes