# oidvectorlt

## Location
[src/backend/utils/adt/oid.c:360-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L360-L367)

## Overview
PostgreSQL comparison function that determines if the first oidvector is less than the second oidvector, implementing the "<" operator for oidvector data types.

## Definition

```c
Datum
oidvectorlt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the "less than" operator (<) for oidvector data types in PostgreSQL. It utilizes the  comparison function to perform a comprehensive comparison between two oidvector values and returns true if the first vector is lexicographically less than the second vector.

The comparison follows a hierarchical approach: vectors are first compared by their dimensions (length), and if dimensions are equal, they are compared element-by-element from left to right. The first vector is considered "less than" the second if it has fewer dimensions, or if it has the same number of dimensions but contains a smaller OID value at the first differing position.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to:
  - Argument 0: First oidvector (left operand)
  - Argument 1: Second oidvector (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - : Core comparison function that returns integer comparison result (-1, 0, or 1)
  - : Macro to extract int32 value from Datum
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator dispatch)

## Notes and Other Information
- This function is part of PostgreSQL's operator implementation system for oidvector types
- Returns boolean true when the comparison result is negative (first < second), false otherwise
- The underlying  function returns negative values when the first argument is less than the second
- Typically invoked through SQL expressions using the < operator on oidvector columns
- Essential for ORDER BY clauses and other comparison operations involving oidvector data
- Located in src/backend/utils/adt/oid.c:360-367