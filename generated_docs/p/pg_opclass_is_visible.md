# pg_opclass_is_visible

## Location
[src/backend/catalog/namespace.c:4950-4963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4950-L4963)

## Overview
SQL-callable function that determines whether an operator class is visible in the current search path without explicit schema qualification.

## Definition


## Detailed Description
The `pg_opclass_is_visible` function is a SQL-callable wrapper around the internal `OpclassIsVisibleExt` function. It takes an OID of an operator class and returns a boolean indicating whether that operator class is visible in the current search path without requiring explicit schema qualification.

This function follows the same pattern as other visibility functions, handling race conditions gracefully by returning NULL if the operator class no longer exists, rather than failing. This behavior prevents errors when queries using MVCC snapshots encounter operator classes that have been dropped after the snapshot was taken but are still visible to the transaction.

Operator classes are critical components of PostgreSQL's indexing system, defining how data types can be indexed and what operators can be used with specific index access methods. The visibility check is important when determining which operator class to use for index creation or when resolving operator class names in DDL statements.

## Parameters / Member Variables
- Parameter 0 (accessed via `PG_GETARG_OID(0)`): The OID of the operator class to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [OpclassIsVisibleExt](../O/OpclassIsVisibleExt.md): Core function that performs the actual operator class visibility check
  - `PG_GETARG_OID`: Macro to extract OID parameter from function arguments
  - `PG_RETURN_NULL`: Macro to return NULL when object is missing
  - `PG_RETURN_BOOL`: Macro to return boolean result

- Called from (representative examples):
  - Index creation operations that need to resolve operator class names
  - psql describe commands for operator class queries
  - System views that display visible operator classes
  - [Query](../Q/Query.md) planner when determining available index access methods

## Notes and Other Information
- Defined in the pg_proc catalog as a stable function with cost 10
- Returns NULL for nonexistent operator classes (since PostgreSQL 8.4) to handle race conditions
- Used throughout PostgreSQL tools for filtering visible operator classes
- Part of the family of visibility functions for different object types
- The function signature in SQL is: pg_opclass_is_visible(opclass oid) → boolean
- Critical for operator class resolution during index creation and maintenance
- Operator classes are tied to specific index access methods (B-tree, Hash, GiST, GIN, SP-GiST, BRIN)
- Visibility determination is important when multiple operator classes for the same data type exist in different schemas