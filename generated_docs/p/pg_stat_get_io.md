# pg_stat_get_io

## Location
src/backend/utils/adt/pgstatfuncs.c: 1357 - 1468

## Overview
A PostgreSQL function that returns detailed I/O statistics for all backend types, I/O objects, and I/O contexts in a tabular format for the pg_stat_io system view.

## Definition


## Detailed Description
This function implements the backend logic for PostgreSQL's pg_stat_io system view, which provides comprehensive I/O statistics across different backend types (like autovacuum, background writer, checkpointer), I/O objects (relations, temp relations), and I/O contexts (normal, vacuum, bulkread, etc.). The function fetches current I/O statistics from the statistics collector and formats them into a table with columns for backend type, context, object, operation counts, timing information, and reset timestamps.

The function uses a set-returning function (SRF) pattern to generate multiple rows of data, iterating through all valid combinations of backend types, I/O objects, and I/O contexts. For each valid combination, it reports statistics for different I/O operations (reads, writes, extends, etc.) including both operation counts and timing data converted from microseconds to milliseconds.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure for set-returning functions

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initialize set-returning function)
  - [pgstat_fetch_stat_io](pgstat_fetch_stat_io.md) (fetch I/O statistics from collector)
  - [GetBackendTypeDesc](../G/GetBackendTypeDesc.md) (get backend type description)
  - [pgstat_bktype_io_stats_valid](pgstat_bktype_io_stats_valid.md) (validate backend I/O stats)
  - [pgstat_tracks_io_bktype](pgstat_tracks_io_bktype.md) (check if backend type has I/O tracking)
  - [pgstat_get_io_object_name](pgstat_get_io_object_name.md) (get I/O object name)
  - [pgstat_get_io_context_name](pgstat_get_io_context_name.md) (get I/O context name)
  - [pgstat_tracks_io_object](pgstat_tracks_io_object.md) (check if object/context combination is tracked)
  - [pgstat_tracks_io_op](pgstat_tracks_io_op.md) (check if specific I/O operation is tracked)
  - [pgstat_get_io_op_index](pgstat_get_io_op_index.md) (get column index for operation)
  - [pgstat_get_io_time_index](pgstat_get_io_time_index.md) (get column index for timing)
  - [pg_stat_us_to_ms](pg_stat_us_to_ms.md) (convert microseconds to milliseconds)
  - tuplestore_putvalues (add row to result set)
- Called from (representative examples):
  - SQL queries on pg_stat_io system view

## Notes and Other Information
- This function is the backend implementation for the pg_stat_io system view
- Uses nested loops to iterate through all valid combinations of backend types, I/O objects, and I/O contexts
- Skips invalid combinations to avoid cluttering the view with NULL-only rows
- Timing data is converted from microseconds to milliseconds for better readability
- The conversion factor is hard-coded to BLCKSZ (typically 8192 bytes) for block-oriented operations
- Returns Datum 0 as is standard for set-returning functions that populate their results via tuplestore
- Includes assertion checks in debug builds to validate statistics consistency