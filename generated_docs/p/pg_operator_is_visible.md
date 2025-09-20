# pg_operator_is_visible

## Location
[src/backend/catalog/namespace.c:4936-4949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4936-L4949)

## Overview
SQL-callable function that determines whether an operator is visible in the current search path without explicit schema qualification.

## Definition

```c
Datum
pg_operator_is_visible(PG_FUNCTION_ARGS)
```
## Detailed Description
The `pg_operator_is_visible` function is a SQL-callable wrapper around the internal `OperatorIsVisibleExt` function. It takes an OID of an operator and returns a boolean indicating whether that operator is visible in the current search path without requiring explicit schema qualification.

This function follows the same pattern as other visibility functions, handling race conditions gracefully by returning NULL if the operator no longer exists, rather than failing. This behavior prevents errors when queries using MVCC snapshots encounter operators that have been dropped after the snapshot was taken but are still visible to the transaction.

The function uses an up-to-date snapshot internally, which may see objects as already gone when they're still visible to the transaction snapshot. This is particularly important for operators since they are frequently resolved during query planning and execution.

## Parameters / Member Variables
- Parameter 0 (accessed via `PG_GETARG_OID(0)`): The OID of the operator to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [OperatorIsVisibleExt](../O/OperatorIsVisibleExt.md): Core function that performs the actual operator visibility check
  - `PG_GETARG_OID`: Macro to extract OID parameter from function arguments
  - `PG_RETURN_NULL`: Macro to return NULL when object is missing
  - `PG_RETURN_BOOL`: Macro to return boolean result

- Called from (representative examples):
  - psql describe commands for operator-related queries (\do)
  - System views that display visible operators
  - Tab completion functionality in psql for operator names
  - Operator resolution during query planning and execution

## Notes and Other Information
- Defined in the pg_proc catalog as a stable function with cost 10
- Returns NULL for nonexistent operators (since PostgreSQL 8.4) to handle race conditions
- Used throughout PostgreSQL tools for filtering visible operators
- Part of the family of visibility functions for different object types
- The function signature in SQL is: pg_operator_is_visible(operator oid) → boolean
- Critical for operator resolution when multiple operators with the same name exist in different schemas
- Operators can be overloaded based on operand types, making visibility determination important for correct resolution