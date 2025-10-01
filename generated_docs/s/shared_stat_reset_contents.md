# shared_stat_reset_contents

## Location
[src/backend/utils/activity/pgstat_shmem.c:993-1008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L993-L1008)

## Overview
This static helper function resets the contents of a statistics entry to zero and optionally updates its reset timestamp.

## Definition
```c
static void shared_stat_reset_contents(PgStat_Kind kind, PgStatShared_Common *header, TimestampTz ts)
```

## Detailed Description
The `shared_stat_reset_contents` function performs a standardized reset operation on a statistics entry. It first zeroes out the data portion of the entry using the appropriate size for the given statistics kind. Then, if the statistics kind has a registered reset timestamp callback, it calls that callback to update the entry's reset timestamp. This provides a consistent way to reset statistics while preserving structural integrity and timestamp information.

## Parameters / Member Variables
- `kind`: The type of statistics entry being reset (PgStat_Kind enum value)
- `header`: Pointer to the shared statistics entry header
- `ts`: The timestamp to record as the reset time

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_kind_info](../p/pgstat_get_kind_info.md)
  - [pgstat_get_entry_data](../p/pgstat_get_entry_data.md)
  - [pgstat_get_entry_len](../p/pgstat_get_entry_len.md)
- Types used:
  - [PgStat_Kind](../P/PgStat_Kind.md)
  - [PgStatShared_Common](../P/PgStatShared_Common.md)
  - [PgStat_KindInfo](../P/PgStat_KindInfo.md)
  - TimestampTz
- Called from:
  - [pgstat_reset_entry](../p/pgstat_reset_entry.md)
  - [pgstat_reset_matching_entries](../p/pgstat_reset_matching_entries.md)

## Notes and Other Information
- This is a static helper function internal to pgstat_shmem.c
- Implements the common pattern for resetting statistics entries
- Handles both data zeroing and timestamp management
- Uses the statistics kind information system to determine appropriate data size and callbacks
- Part of PostgreSQL's statistics reset infrastructure
- Location: src/backend/utils/activity/pgstat_shmem.c:993-1008

## Simplified Source

```c
static void
shared_stat_reset_contents(PgStat_Kind kind, PgStatShared_Common *header,
                          TimestampTz ts)
{
    const PgStat_KindInfo *kind_info = pgstat_get_kind_info(kind);

    // Zero out the statistics data
    memset(pgstat_get_entry_data(kind, header), 0,
           pgstat_get_entry_len(kind));

    // Update reset timestamp if callback is available
    if (kind_info->reset_timestamp_cb)
        kind_info->reset_timestamp_cb(header, ts);
}
```