# pg_stat_reset_replication_slot

## Location
[src/backend/utils/adt/pgstatfuncs.c:1789-1805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1789-L1805)

## Overview
A PostgreSQL system function that resets replication slot statistics, allowing selective reset of a specific replication slot or all replication slots when no target is specified.

## Definition

```c
Datum
pg_stat_reset_replication_slot(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides a mechanism to reset statistics for PostgreSQL replication slots. Replication slots are a critical component of PostgreSQL's logical and physical replication system, ensuring that the WAL (Write-Ahead Log) segments needed by replication consumers are retained until they have been processed.

The function accepts an optional text parameter that specifies which replication slot's statistics to reset. When called without arguments (NULL), it resets statistics for all replication slots by calling  with . When a specific slot name is provided, it calls the more specific  function to reset statistics for the named replication slot.

## Parameters / Member Variables
-  (optional text): Specifies which replication slot's statistics to reset. When NULL, all replication slot statistics are reset. When provided, it should be the name of a specific replication slot.

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_reset_of_kind](pgstat_reset_of_kind.md) (to reset all replication slot statistics)
  - [text_to_cstring](../t/text_to_cstring.md) (to convert text parameter to C string)
  - [pgstat_reset_replslot](pgstat_reset_replslot.md) (to reset specific replication slot statistics)
  - PG_RETURN_VOID (to return from the function)
- Constants used:
  - PGSTAT_KIND_REPLSLOT (specifies replication slot statistics category)
- Called from:
  - SQL function interface (no direct C callers found)

## Notes and Other Information
- Replication slots are essential for both logical and physical replication in PostgreSQL
- Replication slot statistics typically include metrics like the number of WAL segments spilled to disk, the total bytes spilled, transactions streamed, and other replication-related performance data
- The function provides flexibility by allowing both complete reset (all slots) and selective reset (specific named slot)
- Replication slots can become inactive or accumulate large amounts of retained WAL, making statistics monitoring crucial for database administration
- The function requires appropriate administrative privileges to execute, as replication slot management affects cluster-wide replication behavior
- Invalid replication slot names will be handled by the underlying  function
- This function is particularly useful for monitoring and troubleshooting replication performance issues
- Resetting statistics does not affect the actual replication slot functionality, only the accumulated statistical counters

## Simplified Source

```c
Datum
pg_stat_reset_replication_slot(PG_FUNCTION_ARGS)
{
    char *target = NULL;

    if (PG_ARGISNULL(0))
        // Reset all replication slot statistics when no target specified
        pgstat_reset_of_kind(PGSTAT_KIND_REPLSLOT);
    else
    {
        // Reset specific replication slot statistics
        target = text_to_cstring(PG_GETARG_TEXT_PP(0));
        pgstat_reset_replslot(target);
    }

    PG_RETURN_VOID();
}
```