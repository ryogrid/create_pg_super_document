# uuid_ge

## Location
[src/backend/utils/adt/uuid.c:201-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L201-L209)

## Overview
SQL-callable function that tests whether the first UUID argument is greater than or equal to the second UUID argument.

## Definition
```c
Datum uuid_ge(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the greater-than-or-equal-to comparison operator (`>=`) for the UUID data type in PostgreSQL. It extracts two UUID arguments from the function call context, delegates the comparison to `uuid_internal_cmp`, and returns true if the first UUID is lexicographically greater than or equal to the second UUID. This function supports range queries, sorting operations, and other SQL operations that require `>=` comparison semantics.

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
  - SQL queries using `>=` operator
  - B-tree index range scans
  - [Sort](../S/Sort.md) and ordering operations
  - [Range](../R/Range.md) constraint checking

## Notes and Other Information
This function complements the other UUID comparison operators to provide a complete set of relational operators. It returns true when `uuid_internal_cmp` returns a value greater than or equal to zero, indicating that the first UUID is not less than the second in lexicographic order. Together with the other comparison functions, it enables full range query support and proper sorting behavior for UUID columns.