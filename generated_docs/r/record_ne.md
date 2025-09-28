# record_ne

## Location
[src/backend/utils/adt/rowtypes.c:1283-1288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1283-L1288)

## Overview
Compares two records (row types) for inequality, returning true if the records are not equal.

## Definition

```c
Datum
record_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a simple wrapper around  that negates the equality result. It provides the "not equal" comparison operator for PostgreSQL record types by calling  with the same function call information and returning the logical negation of the result.

This implementation leverages all the complex comparison logic already present in , including type checking, column-by-column comparison, NULL handling, and structural validation, while simply inverting the final boolean result.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - : First HeapTupleHeader to compare (argument 0)
  - : Second HeapTupleHeader to compare (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - : Performs the actual equality comparison
  - : Converts Datum result to boolean
- Called from (representative examples):
  - Used by PostgreSQL's type system for != operations on record types

## Notes and Other Information
- Extremely lightweight implementation that delegates all comparison logic to 
- Inherits all the same behavior regarding type mismatches, column count differences, and NULL handling from 
- Part of the complete set of comparison operators for PostgreSQL record types
- The  parameter is passed through unchanged to , maintaining all context and argument information

## Simplified Source

```c
// Simplified version of record_ne
Datum record_ne(PG_FUNCTION_ARGS) {
    // Simply negate the result of record equality
    PG_RETURN_BOOL(!DatumGetBool(record_eq(fcinfo)));
}
```

Key simplifications made:
- The function is already extremely simple - just negates record_eq result
- No additional simplification needed as it's a one-line wrapper
- Maintains the same semantics and error handling through record_eq delegation