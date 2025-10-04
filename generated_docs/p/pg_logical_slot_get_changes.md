# pg_logical_slot_get_changes

## Location
[src/backend/replication/logical/logicalfuncs.c:331-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logicalfuncs.c#L331-L339)

## Overview
SQL-callable function that returns logical replication changes in textual format while consuming and advancing the replication slot position.

## Definition
```c
Datum pg_logical_slot_get_changes(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a PostgreSQL SQL function that provides access to logical replication changes through the SQL interface. It serves as a thin wrapper around the core logical decoding functionality, specifically configured to return textual output and confirm/consume the processed changes by advancing the replication slot position. When called, it retrieves logical changes from a replication slot, formats them as text, and moves the slot forward so subsequent calls will return newer changes.

This function is typically used when applications want to consume logical replication changes in a streaming fashion, where each call processes new changes and the slot position is permanently advanced.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` macro which provides access to:
  - Slot name (text): Name of the logical replication slot to read from
  - LSN limit (pg_lsn, optional): Maximum LSN position to read up to  
  - Row limit (integer, optional): Maximum number of changes to return
  - Options (text array): Configuration options for the logical decoding plugin

## Dependencies
- Functions called/Symbols referenced:
  - [pg_logical_slot_get_changes_guts](pg_logical_slot_get_changes_guts.md) (core implementation function)
- Called from:
  - SQL queries (this is a public SQL-callable function registered in the system catalog)

## Notes and Other Information
- This is a public SQL function accessible through PostgreSQL SQL interface
- Returns a set of rows with columns: (lsn pg_lsn, xid xid, data text)
- The function confirms processed changes (confirm=true parameter to guts function)
- Uses textual output format (binary=false parameter to guts function)
- Advances the replication slot confirmed_flush_lsn position after processing
- Changes returned by this function are consumed and will not be returned by subsequent calls
- Requires appropriate permissions to access logical replication slots
- Part of PostgreSQL logical replication SQL API alongside pg_logical_slot_peek_changes
- Located in src/backend/replication/logical/logicalfuncs.c:331-339

## Simplified Source

```c
Datum pg_logical_slot_get_changes(PG_FUNCTION_ARGS)
{
    // Call core implementation with confirm=true (advance slot) and binary=false (text output)
    return pg_logical_slot_get_changes_guts(fcinfo, true, false);
}
```