# positionjoinsel

## Location
[src/backend/utils/adt/geo_selfuncs.c:73-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_selfuncs.c#L73-L85)

## Overview
A join selectivity estimation function for geometric positional operators used in join operations between tables.

## Definition
```c
Datum positionjoinsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `positionjoinsel` function provides join selectivity estimation for geometric operators that test positional relationships between geometric objects when joining two tables. This function is the join counterpart to `positionsel`, used by PostgreSQL's query optimizer to estimate how many rows will be produced when joining tables using geometric predicates that test strict positional relationships.

Like its non-join counterpart, this function handles selectivity estimation for operators that determine if geometric objects from one table are positioned relative to objects in another table (strictly left of, right of, above, or below). It returns the same selectivity value of 0.1 (10%) as `positionsel`.

The consistent selectivity value between join and non-join scenarios reflects the assumption that positional relationships maintain similar statistical properties whether applied as filters on a single table or as join conditions between tables.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_FLOAT8 (PostgreSQL macro for returning float8 values)
- Called from (representative examples):
  - Used by PostgreSQL's query optimizer for join selectivity estimation of geometric positional operators

## Notes and Other Information
- Returns a join selectivity of 0.1 (10%), same as the non-join `positionsel`
- Companion function to `positionsel` for join scenarios
- Part of the geometric selectivity function family in geo_selfuncs.c
- Specifically handles strict positional relationships in join conditions
- The selectivity value is higher than area-based join operations, reflecting the more common nature of positional relationships

## Simplified Source

```c
Datum positionjoinsel(PG_FUNCTION_ARGS) {
    // Return join selectivity estimate for positional operators
    PG_RETURN_FLOAT8(0.1);
}
```

This join selectivity function estimates how likely geometric objects from one table are to be positioned relative to objects in another table (left of, right of, above, below) during join operations. It returns 10%, matching its non-join counterpart `positionsel`.