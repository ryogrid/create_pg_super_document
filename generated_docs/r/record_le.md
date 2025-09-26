# record_le

## Location
[src/backend/utils/adt/rowtypes.c:1301-1306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1301-L1306)

## Overview
Compares two records (row types) to determine if the first record is less than or equal to the second record.

## Definition

```c
Datum
record_le(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the "less than or equal to" comparison operator for PostgreSQL record types. It is a simple wrapper around the  function that returns true if  returns a value less than or equal to zero, indicating that the first record is either ordered before the second record or is equal to it.

This function completes the set of comparison operators for record types by providing the <= operation, which is essential for range queries, sorting operations, and other SQL constructs that require inclusive comparisons.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - : First HeapTupleHeader to compare (argument 0)
  - : Second HeapTupleHeader to compare (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - : Performs the comprehensive record comparison and returns comparison result (-1, 0, +1)
- Called from (representative examples):
  - Used by PostgreSQL's type system for <= operations on record types
  - [Range](../R/Range.md) queries and BETWEEN clauses involving record types

## Notes and Other Information
- Combines the functionality of both  and  by accepting both less-than and equal results from 
- Part of the complete comparison operator family (=, !=, <, <=, >, >=) for PostgreSQL record types
- Essential for implementing inclusive range comparisons in SQL queries
- Like other ordering functions, requires that all column types support comparison operators
- Inherits the same lexicographic comparison semantics and error handling from the underlying  function