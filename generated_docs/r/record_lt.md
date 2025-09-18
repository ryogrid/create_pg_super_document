# record_lt

## Location
[src/backend/utils/adt/rowtypes.c:1289-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1289-L1294)

## Overview
Compares two records (row types) to determine if the first record is less than the second record.

## Definition


## Detailed Description
The  function implements the "less than" comparison operator for PostgreSQL record types. It is a simple wrapper around the  function that performs a comprehensive lexicographic comparison of two records. The function returns true if  returns a negative value, indicating that the first record is ordered before the second record.

The actual comparison logic is delegated to , which handles:
- Type compatibility checking
- Column-by-column lexicographic comparison
- NULL value ordering (NULLs are considered greater than non-NULLs)
- Handling of dropped columns
- Type-specific comparison operators for each column

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - : First HeapTupleHeader to compare (argument 0)
  - : Second HeapTupleHeader to compare (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - : Performs the comprehensive record comparison and returns comparison result (-1, 0, +1)
- Called from (representative examples):
  - Used by PostgreSQL's type system for < operations on record types
  - B-tree index operations requiring record ordering

## Notes and Other Information
- Part of the complete set of comparison operators for PostgreSQL record types that enable ordering
- Unlike , this function requires that all column types have comparison operators (btree support)
- The comparison follows lexicographic ordering: compares fields left-to-right until finding a difference
- Inherits all error handling and type checking behavior from 
- Essential for supporting ORDER BY clauses, B-tree indexes, and other operations requiring record ordering