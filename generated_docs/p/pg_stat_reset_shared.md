# pg_stat_reset_shared

## Location
[src/backend/utils/adt/pgstatfuncs.c:1702-1749](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1702-L1749)

## Overview
A PostgreSQL system function that resets cluster-wide statistics counters, allowing selective reset of specific statistic categories or all statistics when no target is specified.

## Definition

```c
Datum
pg_stat_reset_shared(PG_FUNCTION_ARGS)
```
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
  - [pgstat_reset_of_kind](pgstat_reset_of_kind.md) (multiple calls for different PGSTAT_KIND_* constants)
  - [XLogPrefetchResetStats](../X/XLogPrefetchResetStats.md)
  - [text_to_cstring](../t/text_to_cstring.md)
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

## Simplified Source

```c
Datum
pg_stat_reset_shared(PG_FUNCTION_ARGS)
{
    char *target = NULL;

    if (PG_ARGISNULL(0))
    {
        // Reset all statistics when no target specified
        pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER);
        pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER);
        pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER);
        pgstat_reset_of_kind(PGSTAT_KIND_IO);
        XLogPrefetchResetStats();
        pgstat_reset_of_kind(PGSTAT_KIND_SLRU);
        pgstat_reset_of_kind(PGSTAT_KIND_WAL);

        PG_RETURN_VOID();
    }

    target = text_to_cstring(PG_GETARG_TEXT_PP(0));

    // Reset specific statistics category based on target string
    if (strcmp(target, "archiver") == 0)
        pgstat_reset_of_kind(PGSTAT_KIND_ARCHIVER);
    else if (strcmp(target, "bgwriter") == 0)
        pgstat_reset_of_kind(PGSTAT_KIND_BGWRITER);
    else if (strcmp(target, "checkpointer") == 0)
        pgstat_reset_of_kind(PGSTAT_KIND_CHECKPOINTER);
    else if (strcmp(target, "io") == 0)
        pgstat_reset_of_kind(PGSTAT_KIND_IO);
    else if (strcmp(target, "recovery_prefetch") == 0)
        XLogPrefetchResetStats();
    else if (strcmp(target, "slru") == 0)
        pgstat_reset_of_kind(PGSTAT_KIND_SLRU);
    else if (strcmp(target, "wal") == 0)
        pgstat_reset_of_kind(PGSTAT_KIND_WAL);
    else
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unrecognized reset target: \"%s\"", target),
                 errhint("Target must be \"archiver\", \"bgwriter\", \"checkpointer\", \"io\", \"recovery_prefetch\", \"slru\", or \"wal\".")));

    PG_RETURN_VOID();
}
```