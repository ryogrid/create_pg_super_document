# pgstat_replslot_reset_timestamp_cb

## Location
src/backend/utils/activity/pgstat_replslot.c: 218 - 223

## Overview
A callback function that updates the statistics reset timestamp for replication slot statistics when a reset operation is performed.

## Definition
```c
void pgstat_replslot_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts)
```

## Detailed Description
This function is a specialized callback within PostgreSQL's statistics subsystem that handles updating the reset timestamp for replication slot statistics. When statistics for a replication slot are reset, this function is called to record the timestamp of when the reset occurred. It casts the generic PgStatShared_Common header to the specific PgStatShared_ReplSlot structure and updates the stat_reset_timestamp field with the provided timestamp.

This callback ensures that consumers of replication slot statistics can determine when the statistics were last reset, which is important for interpreting cumulative statistics correctly.

## Parameters / Member Variables
- `header`: Pointer to PgStatShared_Common structure that will be cast to PgStatShared_ReplSlot to access replication slot specific statistics
- `ts`: TimestampTz value representing the timestamp when the statistics reset occurred

## Dependencies
- Types referenced:
  - PgStatShared_Common
  - PgStatShared_ReplSlot
  - TimestampTz
- Called from (representative examples):
  - Statistics reset operations (via SH_DECLARE macro in pgstat.c:316)

## Notes and Other Information
- Simple callback that performs a direct timestamp assignment
- Part of the statistics reset framework for replication slots
- The function assumes the header parameter points to a valid PgStatShared_ReplSlot structure
- No error checking is performed as this is an internal callback with controlled usage
- Located in src/backend/utils/activity/pgstat_replslot.c:218-223