# uuid_in

## Location
[src/backend/utils/adt/uuid.c:42-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L42-L52)

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

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [string_to_uuid](../s/string_to_uuid.md) (actual UUID parsing logic)
  - `PG_GETARG_CSTRING` (argument retrieval macro)
  - `PG_RETURN_UUID_P` (return value macro)
- Types used:
  - [pg_uuid_t](../p/pg_uuid_t.md) (internal UUID structure)
- Called from:
  - PostgreSQL type system (automatically during type coercion)

## Notes and Other Information
- This is a PostgreSQL internal function registered in the type system for UUID input
- Memory allocation uses `palloc`, which is PostgreSQL's memory context-aware allocator
- Error handling and validation are performed by the `string_to_uuid` function
- The function follows PostgreSQL's standard input function signature pattern

## Simplified Source

```c
Datum
uuid_in(PG_FUNCTION_ARGS)
{
    char *uuid_str = PG_GETARG_CSTRING(0);
    pg_uuid_t *uuid;

    // Allocate memory for UUID structure
    uuid = (pg_uuid_t *) palloc(sizeof(*uuid));

    // Parse string into UUID format
    string_to_uuid(uuid_str, uuid, fcinfo->context);

    // Return the converted UUID
    PG_RETURN_UUID_P(uuid);
}
```