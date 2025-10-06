# range_union

## Location
[src/backend/utils/adt/rangetypes.c:1098-1113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1098-L1113)

## Overview
The range_union function computes the set union of two range values, requiring them to be adjacent or overlapping to ensure a contiguous result.

## Definition
Datum range_union(PG_FUNCTION_ARGS)

## Detailed Description
The range_union function is a PostgreSQL built-in function that implements the range union operation (A ∪ B) with strict adjacency/overlap requirements. It takes two range arguments and returns their union, but only if the result would be a single contiguous range. The function serves as the SQL-callable wrapper around range_union_internal, always calling it in strict mode (strict=true) to ensure the result is mathematically valid for SQL range operations.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro to access arguments:
  - r1: First range argument obtained via PG_GETARG_RANGE_P(0)
  - r2: Second range argument obtained via PG_GETARG_RANGE_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](range_get_typcache.md)
  - RangeTypeGetOid
  - [range_union_internal](range_union_internal.md)
  - PG_RETURN_RANGE_P
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Always operates in strict mode, requiring ranges to be adjacent or overlapping
- Throws an error if the union would result in a non-contiguous range
- Delegates actual computation to range_union_internal with strict=true parameter
- Part of PostgreSQL's range type system for SQL-level range operations
- Commonly used in SQL queries with the + operator for ranges
- Essential for range arithmetic in PostgreSQL applications

## Simplified Source

```c
Datum range_union(PG_FUNCTION_ARGS) {
    RangeType *r1 = PG_GETARG_RANGE_P(0);
    RangeType *r2 = PG_GETARG_RANGE_P(1);

    // Get type cache for range operations
    TypeCacheEntry *typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    // Delegate to internal implementation with strict=true
    PG_RETURN_RANGE_P(range_union_internal(typcache, r1, r2, true));
}
```