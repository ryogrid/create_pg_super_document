# pg_table_is_visible

## Location
src/backend/catalog/namespace.c: 4894 - 4907

## Overview
SQL-callable function that determines whether a table, view, or other relation is visible in the current search path.

## Definition


## Detailed Description
The  function is a SQL-callable wrapper around the internal  function. It takes an OID of a relation (table, view, sequence, etc.) and returns a boolean indicating whether that relation is visible in the current search path without explicit schema qualification.

The function handles race conditions gracefully by returning NULL if the object no longer exists, rather than failing. This behavior was introduced in PostgreSQL 8.4 to avoid errors when queries using MVCC snapshots encounter objects that have been dropped after the snapshot was taken but are still visible to the transaction.

The function uses an up-to-date snapshot internally, which may see objects as already gone when they're still visible to the transaction snapshot, hence the NULL return for missing objects.

## Parameters / Member Variables
- Parameter 0 (accessed via ): The OID of the relation to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs the actual visibility check
  - : Macro to extract OID parameter from function arguments
  - : Macro to return NULL when object is missing
  - : Macro to return boolean result

- Called from (representative examples):
  - Various psql describe commands (\dt, \dv, etc.)
  - pg_dump for determining visible tables
  - System view definitions in system_views.sql
  - Tab completion functionality in psql

## Notes and Other Information
- Defined in the pg_proc catalog with provolatile='s' (stable) and procost='10'
- Returns NULL for nonexistent objects (since PostgreSQL 8.4) to handle race conditions
- Extensively used throughout PostgreSQL tools for filtering visible relations
- Part of the family of visibility functions (pg_type_is_visible, pg_function_is_visible, etc.)
- The function signature in SQL is: pg_table_is_visible(table oid) → boolean