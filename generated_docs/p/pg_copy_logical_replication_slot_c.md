# pg_copy_logical_replication_slot_c

## Location
[src/backend/replication/slotfuncs.c:870-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L870-L875)

## Overview
A SQL-callable function that creates a new logical replication slot by copying the configuration and state from an existing logical replication slot.

## Definition


## Detailed Description
This function is a PostgreSQL system function that provides a simple interface for copying logical replication slots. It serves as a wrapper around the internal `copy_replication_slot` helper function, specifically configured for logical slots by passing `true` as the second parameter.

The function enables users to duplicate existing logical replication slots while preserving their configuration, LSN positions, and other critical state information. This is particularly useful for creating backup slots or distributing replication workload across multiple consumers.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the arguments passed to the SQL function:
  - First argument: Source slot name (Name type)
  - Second argument: Destination slot name (Name type)
  - Optional third argument: Whether the new slot should be temporary (boolean)
  - Optional fourth argument: Plugin name override (Name type)

## Dependencies
- Functions called/Symbols referenced:
  - [copy_replication_slot](../c/copy_replication_slot.md) (with logical_slot=true)
- Called from (representative examples):
  - SQL interface as pg_copy_logical_replication_slot function
  - Database administrators and replication management systems

## Notes and Other Information
- This is part of PostgreSQL's replication slot management system
- The function is exposed to SQL as `pg_copy_logical_replication_slot`
- Only works with logical replication slots; attempting to copy a physical slot will result in an error
- The copied slot inherits most properties from the source slot but does not copy the failover option to prevent synchronization issues
- Returns a composite type containing the new slot name and confirmed flush LSN