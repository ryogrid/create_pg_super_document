# XLogPrefetchResetStats

## Location
src/backend/access/transam/xlogprefetcher.c: 303 - 314

## Overview
Resets all XLog prefetch statistics counters to zero and records the current timestamp as the reset time.

## Definition


## Detailed Description
This function performs a complete reset of all XLog prefetch statistics maintained in shared memory. It atomically writes zero to all statistical counters and updates the reset timestamp to the current time. The function ensures thread-safe operation by using atomic write operations for all counter updates.

The statistics reset include:
- Reset timestamp (set to current time)
- Prefetch counter
- Hit counter  
- Skip counters for various conditions (init, new, fpw, rep)

All operations use atomic writes to ensure consistency in a multi-process environment where multiple backends might be accessing the statistics concurrently.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp
  - pg_atomic_write_u64
- Called from (representative examples):
  - pg_stat_reset_shared

## Notes and Other Information
- This function is typically called when administrators want to reset prefetch statistics
- Uses atomic operations to ensure thread safety in shared memory
- The reset timestamp allows tracking when statistics were last cleared
- All counters are reset simultaneously to maintain consistency
- Called from PostgreSQL's statistics reset infrastructure via pg_stat_reset_shared
- Located in src/backend/access/transam/xlogprefetcher.c:303-314