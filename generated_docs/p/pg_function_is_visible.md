# pg_function_is_visible

## Location
[src/backend/catalog/namespace.c:4922-4935](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4922-L4935)

## Overview
SQL-callable function that determines whether a function is visible in the current search path without explicit schema qualification.

## Definition

```c
Datum
pg_function_is_visible(PG_FUNCTION_ARGS)
```
## Detailed Description
The `pg_function_is_visible` function is a SQL-callable wrapper around the internal `FunctionIsVisibleExt` function. It takes an OID of a function and returns a boolean indicating whether that function is visible in the current search path without requiring explicit schema qualification.

This function follows the same pattern as other visibility functions, handling race conditions gracefully by returning NULL if the function no longer exists, rather than failing. This behavior prevents errors when queries using MVCC snapshots encounter functions that have been dropped after the snapshot was taken but are still visible to the transaction.

The function uses an up-to-date snapshot internally, which may see objects as already gone when they're still visible to the transaction snapshot.

## Parameters / Member Variables
- Parameter 0 (accessed via `PG_GETARG_OID(0)`): The OID of the function to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionIsVisibleExt](../F/FunctionIsVisibleExt.md): Core function that performs the actual function visibility check
  - `PG_GETARG_OID`: Macro to extract OID parameter from function arguments
  - `PG_RETURN_NULL`: Macro to return NULL when object is missing
  - `PG_RETURN_BOOL`: Macro to return boolean result

- Called from (representative examples):
  - psql describe commands for function-related queries (\df, \ef, etc.)
  - System views that display visible functions
  - Tab completion functionality in psql for function names
  - Function lookup operations in various PostgreSQL utilities

## Notes and Other Information
- Defined in the pg_proc catalog as a stable function with cost 10
- Returns NULL for nonexistent functions (since PostgreSQL 8.4) to handle race conditions
- Used throughout PostgreSQL tools for filtering visible functions
- Part of the family of visibility functions for different object types
- The function signature in SQL is: pg_function_is_visible(function oid) → boolean
- Critical for function resolution when multiple functions with the same name exist in different schemas

## Simplified Source

```c
Datum pg_function_is_visible(PG_FUNCTION_ARGS)
{
    // Extract the function OID from function arguments
    Oid oid = PG_GETARG_OID(0);
    bool result;
    bool is_missing = false;

    // Check if function is visible in current search path
    result = FunctionIsVisibleExt(oid, &is_missing);

    // Return NULL if function doesn't exist (avoids race conditions)
    if (is_missing)
        PG_RETURN_NULL();

    // Return boolean result indicating visibility
    PG_RETURN_BOOL(result);
}
```