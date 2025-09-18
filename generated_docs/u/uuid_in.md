# uuid_in

## Location
src/backend/utils/adt/uuid.c: 42 - 52

## Overview
PostgreSQL input function that converts a string representation of a UUID into internal UUID binary format.

## Definition
```c
Datum uuid_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The `uuid_in` function is a PostgreSQL input function that parses a string representation of a UUID and converts it to the internal binary format (`pg_uuid_t`). This function is called automatically by PostgreSQL when converting string literals or text values to UUID type values. It serves as the standard entry point for UUID input operations in the type system.

The function allocates memory for a new UUID structure and delegates the actual parsing work to the `string_to_uuid` helper function, which handles the detailed validation and conversion logic.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function call information including:
  - The input string (retrieved via `PG_GETARG_CSTRING(0)`)
  - Function call context information via `fcinfo`

## Dependencies
- Functions called/Symbols referenced:
  - `palloc` (memory allocation)
  - `string_to_uuid` (actual UUID parsing logic)
  - `PG_GETARG_CSTRING` (argument retrieval macro)
  - `PG_RETURN_UUID_P` (return value macro)
- Types used:
  - `pg_uuid_t` (internal UUID structure)
- Called from:
  - PostgreSQL type system (automatically during type coercion)

## Notes and Other Information
- This is a PostgreSQL internal function registered in the type system for UUID input
- Memory allocation uses `palloc`, which is PostgreSQL's memory context-aware allocator
- Error handling and validation are performed by the `string_to_uuid` function
- The function follows PostgreSQL's standard input function signature pattern