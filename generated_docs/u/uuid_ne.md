# uuid_ne

## Location
[src/backend/utils/adt/uuid.c:219-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L219-L228)

## Overview
Implements the not equal comparison operator (!=) for PostgreSQL UUID data type, returning true if the two UUID values are not equal.

## Definition
```c
Datum uuid_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
The `uuid_ne` function is a PostgreSQL built-in function that compares two UUID values and returns a boolean result indicating whether they are not equal. The comparison is performed using the internal `uuid_internal_cmp` function, where a non-zero result indicates the UUIDs are different. This function is commonly used in SQL WHERE clauses and other conditional expressions requiring UUID inequality testing. The function follows PostgreSQL's standard function calling convention and returns a Datum containing a boolean value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1` (pg_uuid_t*): Pointer to the first UUID value to compare
  - `arg2` (pg_uuid_t*): Pointer to the second UUID value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_UUID_P`: Macro to extract UUID arguments from function call
  - [uuid_internal_cmp](uuid_internal_cmp.md): Internal function performing the actual UUID comparison
  - `PG_RETURN_BOOL`: Macro to return boolean result
  - [pg_uuid_t](../p/pg_uuid_t.md): UUID data type structure
- Called from (representative examples):
  - SQL queries using UUID != or <> operators
  - Conditional expressions in stored procedures
  - [Query](../Q/Query.md) optimization and filtering operations

## Notes and Other Information
- The function returns true when `uuid_internal_cmp(arg1, arg2) != 0`
- Part of the UUID data type operator family in PostgreSQL
- Complements the uuid_eq function for equality testing
- The comparison is byte-wise, ensuring exact UUID matching
- Essential for UUID-based filtering and conditional logic in database applications