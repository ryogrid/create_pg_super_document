# pg_copy_physical_replication_slot_b

## Location
[src/backend/replication/slotfuncs.c:882-891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L882-L891)

## Overview
A SQL-callable function that creates a new physical replication slot by copying the configuration and state from an existing physical replication slot.

## Definition

```c
Datum
pg_copy_physical_replication_slot_b(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL system function that provides a simple interface for copying physical replication slots. It serves as a wrapper around the internal `copy_replication_slot` helper function, specifically configured for physical slots by passing `false` as the second parameter.

The function enables users to duplicate existing physical replication slots while preserving their configuration, LSN positions, and other critical state information. This is particularly useful for creating backup slots, setting up additional standby servers, or managing replication topology changes.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the arguments passed to the SQL function:
  - First argument: Source slot name (Name type)
  - Second argument: Destination slot name (Name type)
  - Optional third argument: Whether the new slot should be temporary (boolean)

## Dependencies
- Functions called/Symbols referenced:
  - [copy_replication_slot](../c/copy_replication_slot.md) (with logical_slot=false)
- Called from (representative examples):
  - SQL interface as pg_copy_physical_replication_slot function
  - Database administrators and replication management systems

## Notes and Other Information
- This is part of PostgreSQL's replication slot management system
- The function is exposed to SQL as `pg_copy_physical_replication_slot`
- Only works with physical replication slots; attempting to copy a logical slot will result in an error
- Physical replication slots track WAL position for streaming replication but do not decode logical changes
- Returns a composite type containing the new slot name and LSN information
- The copied slot will have the same restart_lsn as the source slot at the time of copying

## Simplified Source

```c
Datum
pg_copy_physical_replication_slot_b(PG_FUNCTION_ARGS)
{
    // Delegate to shared copy function with logical=false flag
    return copy_replication_slot(fcinfo, false);
}
```