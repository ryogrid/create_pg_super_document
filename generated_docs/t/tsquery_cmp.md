# tsquery_cmp

## Location
[src/backend/utils/adt/tsquery_op.c:215-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L215-L226)

## Overview
PostgreSQL SQL-callable function that compares two tsquery objects and returns their relative ordering as an integer.

## Definition


## Detailed Description
The  function serves as the SQL-accessible interface for comparing tsquery objects. It extracts two tsquery parameters from the function arguments, delegates the actual comparison to the internal  function, and returns the comparison result as a PostgreSQL Datum.

This function is typically used by PostgreSQL's indexing and sorting infrastructure to establish ordering relationships between tsquery values. It enables tsquery objects to be used in ORDER BY clauses, B-tree indexes, and other operations that require comparison semantics.

The function properly manages memory by freeing copied tsquery objects after the comparison is complete, following PostgreSQL's memory management conventions for function arguments.

## Parameters / Member Variables
- : First tsquery object to compare
- : Second tsquery object to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY_COPY
  - [CompareTSQ](../C/CompareTSQ.md)
  - PG_FREE_IF_COPY
  - PG_RETURN_INT32
- Data structures used:
  - TSQuery

## Notes and Other Information
- Returns -1 if first argument < second argument
- Returns 0 if arguments are equal  
- Returns 1 if first argument > second argument
- Used by PostgreSQL's comparison operators and indexing system
- Follows PostgreSQL's fmgr (function manager) calling convention
- Properly handles memory management of copied tsquery arguments
- Part of the tsquery comparison operator family
- Enables tsquery objects to be sorted and indexed efficiently