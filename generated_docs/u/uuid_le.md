# uuid_le

## Location
[src/backend/utils/adt/uuid.c:183-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L183-L191)

## Overview
SQL-callable function that tests whether the first UUID argument is less than or equal to the second UUID argument.

## Definition
```c
Datum uuid_le(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than-or-equal-to comparison operator (`<=`) for the UUID data type in PostgreSQL. It extracts two UUID arguments from the function call context, uses `uuid_internal_cmp` to perform the comparison, and returns true if the first UUID is lexicographically less than or equal to the second UUID. This function enables SQL queries to use the `<=` operator between UUID values for range queries and sorting operations.

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
  - SQL queries using `<=` operator
  - B-tree index range scans
  - [Sort](../S/Sort.md) and merge operations

## Notes and Other Information
Like other UUID comparison functions, this follows PostgreSQL's standard function call convention and delegates to `uuid_internal_cmp` for the actual comparison logic. The `<=` operation returns true when `uuid_internal_cmp` returns a value less than or equal to zero, indicating the first UUID is not greater than the second.