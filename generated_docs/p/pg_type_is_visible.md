# pg_type_is_visible

## Location
[src/backend/catalog/namespace.c:4908-4921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4908-L4921)

## Overview
SQL-callable function that determines whether a data type is visible in the current search path without explicit schema qualification.

## Definition

```c
Datum
pg_type_is_visible(PG_FUNCTION_ARGS)
```
## Detailed Description
The `pg_type_is_visible` function is a SQL-callable wrapper around the internal `TypeIsVisibleExt` function. It takes an OID of a data type and returns a boolean indicating whether that type is visible in the current search path without requiring explicit schema qualification.

Like other visibility functions, it handles race conditions gracefully by returning NULL if the type no longer exists, rather than failing. This behavior prevents errors when queries using MVCC snapshots encounter types that have been dropped after the snapshot was taken but are still visible to the transaction.

The function uses an up-to-date snapshot internally, which may see objects as already gone when they're still visible to the transaction snapshot.

## Parameters / Member Variables
- Parameter 0 (accessed via `PG_GETARG_OID(0)`): The OID of the data type to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [TypeIsVisibleExt](../T/TypeIsVisibleExt.md): Core function that performs the actual type visibility check
  - `PG_GETARG_OID`: Macro to extract OID parameter from function arguments
  - `PG_RETURN_NULL`: Macro to return NULL when object is missing
  - `PG_RETURN_BOOL`: Macro to return boolean result

- Called from (representative examples):
  - psql describe commands for type-related queries
  - System view definitions that filter visible types
  - Tab completion functionality in psql for type names

## Notes and Other Information
- Defined in the pg_proc catalog as a stable function with cost 10
- Returns NULL for nonexistent types (since PostgreSQL 8.4) to handle race conditions
- Used throughout PostgreSQL tools for filtering visible data types
- Part of the family of visibility functions for different object types
- The function signature in SQL is: pg_type_is_visible(type oid) → boolean