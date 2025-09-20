# pgstat_reset_slru_counter_internal

## Location
[src/backend/utils/activity/pgstat_slru.c:238-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L238-L248)

## Overview
An internal static function that resets the statistics counters for a specific SLRU type in shared memory, clearing all accumulated statistics and recording the reset timestamp.

## Definition

```c
static void
pgstat_reset_slru_counter_internal(int index, TimestampTz ts)
```
## Detailed Description
This internal function performs the actual work of resetting SLRU statistics for a specific SLRU type identified by its index. It acquires an exclusive lock on the shared SLRU statistics structure to ensure atomic updates, clears all statistics fields using memset, sets the reset timestamp to track when the reset occurred, and then releases the lock. The exclusive lock prevents concurrent access during the reset operation, ensuring data consistency. This function serves as the core implementation for both individual and bulk SLRU statistics reset operations.

## Parameters / Member Variables
- : Index of the specific SLRU type to reset (must be valid index within SLRU_NUM_ELEMENTS)
- : Timestamp indicating when the reset operation occurred

## Dependencies
- Functions called/Symbols referenced:
  -  (shared memory structure for SLRU statistics)
  -  (exclusive lightweight lock mode)
  -  (acquire lightweight lock function)
  -  (release lightweight lock function)
  -  (memory clear function)
  -  (structure type for SLRU statistics)
  -  (shared memory reference)
- Called from (representative examples):
  -  at src/backend/utils/activity/pgstat_slru.c:51
  -  at src/backend/utils/activity/pgstat_slru.c:199

## Notes and Other Information
- Static function, only accessible within the pgstat_slru.c module
- Uses exclusive locking to prevent concurrent modifications during reset
- Records the reset timestamp for auditing and tracking purposes
- Serves as the common implementation for both individual and bulk reset operations
- Essential for maintaining accurate SLRU statistics when requested by database administrators
- The exclusive lock ensures atomicity of the reset operation across all fields in the statistics structure