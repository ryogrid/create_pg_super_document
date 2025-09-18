# pg_logical_slot_peek_changes

## Location
src/backend/replication/logical/logicalfuncs.c: 340 - 348

## Overview
SQL-callable function that returns logical replication changes in textual format without consuming or advancing the replication slot position.

## Definition
```c
Datum pg_logical_slot_peek_changes(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a PostgreSQL SQL function that provides non-destructive access to logical replication changes through the SQL interface. It serves as a thin wrapper around the core logical decoding functionality, specifically configured to return textual output without confirming/consuming the processed changes. The replication slot position remains unchanged after calling this function, allowing the same changes to be retrieved again in subsequent calls.

This function is typically used for monitoring, debugging, or preview purposes where applications want to examine available changes without permanently advancing the slot position. It provides a "peek" functionality that allows inspection of what changes would be returned by pg_logical_slot_get_changes.

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
- The function does not confirm processed changes (confirm=false parameter to guts function)
- Uses textual output format (binary=false parameter to guts function)
- Does not advance the replication slot confirmed_flush_lsn position after processing
- Changes returned by this function can be retrieved again by subsequent calls
- Useful for examining available changes without consumption for monitoring/debugging
- Requires appropriate permissions to access logical replication slots
- Complementary function to pg_logical_slot_get_changes (peek vs consume behavior)
- Part of PostgreSQL logical replication SQL API
- Located in src/backend/replication/logical/logicalfuncs.c:340-348