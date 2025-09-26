# ClockSweepTick

## Location
[src/backend/storage/buffer/freelist.c:108-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L108-L174)

## Overview
ClockSweepTick is a helper function for the buffer management clock sweep algorithm that atomically advances the clock hand to the next buffer position and returns the buffer ID under the new position.

## Definition

```c
static inline uint32
ClockSweepTick(void)
```
## Detailed Description
ClockSweepTick implements the core mechanism of PostgreSQL's clock sweep buffer replacement algorithm. It atomically increments the nextVictimBuffer counter in StrategyControl to move the clock hand forward by one buffer position. The function handles wraparound when the counter exceeds NBuffers, ensuring the returned buffer ID is always valid within the buffer pool range.

When a wraparound occurs (victim buffer ID reaches 0 after modulo operation), the function performs additional synchronization work. It acquires the buffer_strategy_lock spinlock and increments the completePasses counter, which tracks how many complete passes through the buffer pool have been made. This synchronization ensures that StrategySyncStart() can read consistent values of both nextVictimBuffer and completePasses.

The atomic operations allow multiple processes to safely advance the clock hand concurrently, though this may result in buffers being selected slightly out of apparent order.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_fetch_add_u32](../p/pg_atomic_fetch_add_u32.md)
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md)
  - SpinLockAcquire
  - SpinLockRelease
- Called from (representative examples):
  - [StrategyGetBuffer](../S/StrategyGetBuffer.md)

## Notes and Other Information
- The function is marked as static inline for performance optimization since it's called frequently during buffer replacement
- Uses atomic operations to ensure thread safety in concurrent environments
- The wraparound handling with spinlock synchronization is critical for maintaining consistency with StrategySyncStart()
- May return buffer IDs slightly out of order when multiple processes are competing, but this doesn't affect correctness of the clock algorithm