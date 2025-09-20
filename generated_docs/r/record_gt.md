# record_gt

## Location
[src/backend/utils/adt/rowtypes.c:1295-1300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1295-L1300)

## Overview
Compares two records (row types) to determine if the first record is greater than the second record.

## Definition

```c
Datum
record_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the "greater than" comparison operator for PostgreSQL record types. It is a simple wrapper around the  function that performs a comprehensive lexicographic comparison of two records. The function returns true if  returns a positive value, indicating that the first record is ordered after the second record.

Like , the actual comparison logic is delegated to , which provides the foundation for all ordering operations on record types by implementing lexicographic comparison semantics.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - : First HeapTupleHeader to compare (argument 0)
  - : Second HeapTupleHeader to compare (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - : Performs the comprehensive record comparison and returns comparison result (-1, 0, +1)
- Called from (representative examples):
  - Used by PostgreSQL's type system for > operations on record types
  - B-tree index operations requiring record ordering

## Notes and Other Information
- Complement to  in the complete set of comparison operators for PostgreSQL record types
- Enables greater-than comparisons in ORDER BY clauses, WHERE conditions, and other SQL constructs
- Like all ordering functions, requires that all column types have comparison operators (btree support)
- Follows the same lexicographic ordering rules as  but returns true for the opposite condition
- Inherits all the robust error handling, type checking, and NULL handling behavior from 