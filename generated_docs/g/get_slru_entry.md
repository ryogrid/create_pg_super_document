# get_slru_entry

## Location
[src/backend/utils/activity/pgstat_slru.c:220-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L220-L237)

## Overview
Returns a pointer to the pending statistics entry for a given SLRU index, providing access to the counters that track various SLRU operations before they are flushed to shared memory.

## Definition

```c
static inline PgStat_SLRUStats *
get_slru_entry(int slru_idx)
```
## Detailed Description
This static inline function provides access to the pending SLRU statistics for a specific SLRU type identified by its index. It performs several safety checks including verifying that the statistics system is active, ensuring it's not running in the postmaster process (which should not accumulate statistics), and validating the index bounds. The function sets the global flag  to indicate that there are pending SLRU statistics and returns a pointer to the appropriate entry in the  array. This allows various SLRU operations to efficiently update their statistics counters.

## Parameters / Member Variables
- : Index of the SLRU type in the statistics array (must be within [0, SLRU_NUM_ELEMENTS))

## Dependencies
- Functions called/Symbols referenced:
  -  (assert statistics system is active)
  -  (constant defining number of SLRU types)
  -  (macro to check if running under postmaster)
  -  (macro to check postmaster environment)
  -  (global array of pending statistics)
  -  (global flag for pending statistics)
- Called from (representative examples):
  -  at src/backend/utils/activity/pgstat_slru.c:61
  -  at src/backend/utils/activity/pgstat_slru.c:67
  -  at src/backend/utils/activity/pgstat_slru.c:73
  -  at src/backend/utils/activity/pgstat_slru.c:79
  -  at src/backend/utils/activity/pgstat_slru.c:85
  -  at src/backend/utils/activity/pgstat_slru.c:91
  -  at src/backend/utils/activity/pgstat_slru.c:97

## Notes and Other Information
- Static inline function for performance, as it's called frequently by SLRU counting operations
- Includes important safety checks to prevent statistics corruption in fork scenarios
- The postmaster exclusion prevents double-counting of statistics in child processes
- Sets the  flag to indicate that statistics need to be flushed
- Central access point for all SLRU statistics counting functions

## Simplified Source

```c
static inline PgStat_SLRUStats *
get_slru_entry(int slru_idx)
{
    pgstat_assert_is_up();

    // Postmaster should never register SLRU stats (prevents fork duplication)
    Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

    Assert((slru_idx >= 0) && (slru_idx < SLRU_NUM_ELEMENTS));

    have_slrustats = true;

    return &pending_SLRUStats[slru_idx];
}
```