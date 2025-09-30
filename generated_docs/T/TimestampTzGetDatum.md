# TimestampTzGetDatum

## Location
[src/include/utils/timestamp.h:52-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timestamp.h#L52-L57)

## Overview
Converts a PostgreSQL TimestampTz (timestamp with timezone) value to a Datum by leveraging int64 conversion routines, since TimestampTz is internally represented as an int64 value.

## Definition
```c
static inline Datum TimestampTzGetDatum(TimestampTz X)
```

## Detailed Description
TimestampTzGetDatum is an inline function that converts a TimestampTz value into a Datum. Like Timestamp, TimestampTz is built on top of int64 representation (microseconds since PostgreSQL epoch), so this function simply delegates to Int64GetDatum to perform the conversion. This is the inverse operation of DatumGetTimestampTz and is used when returning timestamp with timezone values from PostgreSQL functions or storing them in tuple slots.

## Parameters / Member Variables
- `X`: The input TimestampTz value to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [Int64GetDatum](../I/Int64GetDatum.md)
  - TimestampTz (parameter type)
- Called from (representative examples):
  - [pg_last_committed_xact](../p/pg_last_committed_xact.md)
  - [pg_xact_commit_timestamp_origin](../p/pg_xact_commit_timestamp_origin.md)
  - [pg_prepared_xact](../p/pg_prepared_xact.md)
  - PG_STAT_GET_RECOVERY_PREFETCH_COLS
  - [pg_prepared_statement](../p/pg_prepared_statement.md)
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md)
  - PG_STAT_GET_SUBSCRIPTION_COLS
  - PG_GET_REPLICATION_SLOTS_COLS
  - [pg_stat_get_wal_receiver](../p/pg_stat_get_wal_receiver.md)
  - PG_STAT_GET_WAL_SENDERS_COLS
  - [parse_datetime](../p/parse_datetime.md)
  - [pg_stat_file](../p/pg_stat_file.md)
  - [pg_ls_dir_files](../p/pg_ls_dir_files.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)
  - PG_STAT_GET_ACTIVITY_COLS
  - [pg_stat_get_archiver](../p/pg_stat_get_archiver.md)
  - [generate_series_timestamptz_internal](../g/generate_series_timestamptz_internal.md)
  - [pg_control_system](../p/pg_control_system.md)
  - [pg_control_checkpoint](../p/pg_control_checkpoint.md)
  - PG_RETURN_TIMESTAMPTZ (macro)

## Notes and Other Information
- This function is defined as static inline for performance efficiency
- The function relies on the fact that TimestampTz and int64 have identical memory layouts
- Used extensively throughout the PostgreSQL system for returning timestamptz results from system functions
- Heavily utilized in system monitoring functions (pg_stat_*), replication functions, transaction management, and file system operations
- Essential for the PostgreSQL function manager (fmgr) interface when returning timestamptz values
- TimestampTz represents timestamps that are timezone-aware but stored in UTC internally
- Location: src/include/utils/timestamp.h:52-57

## Simplified Source

```c
static inline Datum TimestampTzGetDatum(TimestampTz X) {
    // Convert TimestampTz to Datum using int64 conversion since TimestampTz is int64
    return Int64GetDatum(X);
}
```