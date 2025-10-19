# pgstat_slru_reset_all_cb

## Location
[src/backend/utils/activity/pgstat_slru.c:196-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L196-L202)

## Overview
A callback function that resets all SLRU statistics counters to zero across all known SLRU types in the system.

## Definition

```c
void
pgstat_slru_reset_all_cb(TimestampTz ts)
```
## Detailed Description
This function serves as a callback handler for resetting all SLRU (Simple LRU) statistics. It iterates through all elements in the SLRU statistics array and calls the internal reset function for each one. This provides a convenient way to clear all SLRU statistics at once, typically used during system maintenance or when requested by database administrators. The function ensures that all SLRU types including commit_timestamp, multixact_member, multixact_offset, notify, serializable, subtransaction, transaction, and other are reset uniformly.

## Parameters / Member Variables
- `ts`: Timestamp indicating when the reset operation occurred
## Dependencies
- Functions called/Symbols referenced:
  -  (constant defining number of SLRU types)
  -  (internal function to reset individual SLRU counters)
- Called from (representative examples):
  -  at src/backend/utils/activity/pgstat.c:381

## Notes and Other Information
- This is a callback function, typically registered with the statistics system for bulk reset operations
- The timestamp parameter allows tracking when the reset occurred for auditing purposes
- Provides atomic reset of all SLRU statistics to maintain consistency
- Used in conjunction with PostgreSQL's statistics reset infrastructure

## Simplified Source

```c
void pgstat_slru_reset_all_cb(TimestampTz ts)
{
    // Reset all SLRU statistics counters
    for (int i = 0; i < SLRU_NUM_ELEMENTS; i++)
        pgstat_reset_slru_counter_internal(i, ts);
}
```