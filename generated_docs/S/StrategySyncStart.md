# StrategySyncStart

## Location
[src/backend/storage/buffer/freelist.c:394-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L394-L430)

## Overview
StrategySyncStart provides the background buffer synchronization process (bgwriter) with the optimal starting point for buffer synchronization, along with statistics about buffer allocation activity.

## Definition

```c
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
```
## Detailed Description
StrategySyncStart coordinates with the buffer replacement strategy to determine where the background writer should begin synchronizing dirty buffers to disk. The function returns the buffer index that represents the best starting point for BgBufferSync() to begin its circular scan of the buffer pool.

The function operates under spinlock protection to ensure it reads consistent values of nextVictimBuffer and completePasses. It calculates the starting buffer index as nextVictimBuffer modulo NBuffers, which corresponds to the current position in the clock sweep algorithm.

The function also provides two important statistics if the caller requests them:
1. **Complete passes count**: The total number of complete passes through the buffer pool, calculated by combining the stored completePasses counter with any additional wraparounds that occurred before completePasses could be incremented.
2. **Buffer allocation count**: The number of buffer allocations since the last call, which is atomically reset to zero after reading.

## Parameters / Member Variables
- : Optional output parameter for the total number of complete buffer pool passes
- : Optional output parameter for recent buffer allocation count (reset after reading)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - pg_atomic_read_u32
  - pg_atomic_exchange_u32
  - StrategyControl (global buffer strategy control structure)
- Called from (representative examples):
  - BgBufferSync
  - ResourceOwnerForgetBufferIO

## Notes and Other Information
- Uses spinlock protection to ensure atomic reading of nextVictimBuffer and completePasses
- The complete passes calculation accounts for wraparounds that may not yet be reflected in completePasses
- Buffer allocation statistics are automatically reset to zero after being read, providing a "delta" measurement
- The returned buffer index provides an optimal starting point for background synchronization to work ahead of the clock sweep
- Coordinates closely with ClockSweepTick() to maintain consistency between buffer replacement and background writing