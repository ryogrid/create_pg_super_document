# pg_stat_reset_shared

## Location
src/backend/utils/adt/pgstatfuncs.c: 1702 - 1749

## Overview
A PostgreSQL system function that resets cluster-wide statistics counters, allowing selective reset of specific statistic categories or all statistics when no target is specified.

## Definition


## Detailed Description
The  function provides a mechanism to reset various shared cluster-wide statistical counters in PostgreSQL. It accepts an optional text parameter that specifies which category of statistics to reset. When called without arguments (NULL), it resets all supported statistics categories. The function is designed to help database administrators manage and monitor PostgreSQL's internal statistics by providing selective reset capabilities.

The function supports resetting statistics for several key PostgreSQL subsystems including the archiver, background writer, checkpointer, I/O operations, WAL prefetch recovery, SLRU (Simple LRU) caches, and write-ahead logging.

## Parameters / Member Variables
-  (optional text): Specifies which statistics category to reset. Valid values are:
  - : Resets archiver process statistics
  - : Resets background writer statistics  
  - : Resets checkpointer process statistics
  - : Resets I/O operation statistics
  - : Resets WAL recovery prefetch statistics
  - : Resets SLRU (Simple LRU) cache statistics
  - : Resets write-ahead logging statistics
  -  (no argument): Resets all supported statistics categories

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_reset_of_kind (multiple calls for different PGSTAT_KIND_* constants)
  - XLogPrefetchResetStats
  - text_to_cstring
  - PG_RETURN_VOID
  - ereport (for error handling)
- Constants used:
  - PGSTAT_KIND_ARCHIVER
  - PGSTAT_KIND_BGWRITER
  - PGSTAT_KIND_CHECKPOINTER
  - PGSTAT_KIND_IO
  - PGSTAT_KIND_SLRU
  - PGSTAT_KIND_WAL
- Called from:
  - SQL function interface (no direct C callers found)

## Notes and Other Information
- This function is exposed as a PostgreSQL SQL function for administrative use
- When an invalid target string is provided, the function raises an ERROR with code ERRCODE_INVALID_PARAMETER_VALUE
- The function provides a helpful hint listing all valid target options when an invalid target is specified
- Statistics names are designed to match those in  where relevant, maintaining consistency across the statistics subsystem
- The function requires appropriate privileges to execute, as it affects cluster-wide statistics
- Each statistics category is reset independently, allowing fine-grained control over which metrics to clear