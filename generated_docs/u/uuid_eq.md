# uuid_eq

## Location
[src/backend/utils/adt/uuid.c:192-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L192-L200)

## Overview
SQL-callable function that tests whether two UUID arguments are equal.

## Definition
```c
Datum uuid_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the equality comparison operator (`=`) for the UUID data type in PostgreSQL. It extracts two UUID arguments from the function call context, uses `uuid_internal_cmp` to perform byte-wise comparison, and returns true if both UUIDs are identical. This function is essential for equality tests, hash joins, unique constraints, and primary key lookups involving UUID columns.

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
  - SQL queries using `=` operator
  - Hash table operations
  - Unique constraint checking
  - Primary key lookups

## Notes and Other Information
Equality comparison is fundamental for UUID operations and is heavily used in database operations like joins, lookups, and constraint enforcement. The function returns true only when `uuid_internal_cmp` returns exactly zero, indicating that all 16 bytes of the UUID data are identical. This ensures that UUIDs are compared as complete binary values rather than as structured data.