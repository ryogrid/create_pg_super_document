# uuid_lt

## Location
src/backend/utils/adt/uuid.c: 174 - 182

## Overview
SQL-callable function that tests whether the first UUID argument is less than the second UUID argument.

## Definition
```c
Datum uuid_lt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than comparison operator (`<`) for the UUID data type in PostgreSQL. It extracts two UUID arguments from the function call context using PostgreSQL's function call convention, delegates the actual comparison to `uuid_internal_cmp`, and returns a boolean result indicating whether the first UUID is lexicographically less than the second. This function is directly callable from SQL queries using the `<` operator between UUID values.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` macro to access arguments:
  - First argument (index 0): Left operand UUID value
  - Second argument (index 1): Right operand UUID value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_UUID_P (macro to extract UUID argument)
  - [uuid_internal_cmp](uuid_internal_cmp.md) (core comparison function)
  - PG_RETURN_BOOL (macro to return boolean result)
  - [pg_uuid_t](../p/pg_uuid_t.md) (UUID data type structure)
- Called from (representative examples):
  - SQL queries using `<` operator
  - B-tree index operations
  - Sort operations

## Notes and Other Information
This function follows PostgreSQL's standard pattern for implementing SQL operators as C functions. It uses PostgreSQL's function call convention with `PG_FUNCTION_ARGS` and the associated macros for argument extraction and return value handling. The comparison semantics are purely lexicographic based on the binary UUID representation.