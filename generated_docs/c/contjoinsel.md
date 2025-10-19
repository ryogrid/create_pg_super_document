# contjoinsel

## Location
[src/backend/utils/adt/geo_selfuncs.c:92-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_selfuncs.c#L92-L95)

## Overview
A join selectivity function for geometric containment operators that estimates the selectivity of joins involving box containment and "contained by" operations.

## Definition

```c
Datum
contjoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a PostgreSQL join selectivity estimator function specifically designed for geometric operators that involve containment relationships, such as "contains" (@>) and "contained by" (<@) operators for geometric data types (box, polygon, circle, point, etc.). This function is registered in the system catalog as the join selectivity estimator (oprjoin) for various geometric containment operators.

The function implements a very simple selectivity estimation strategy, returning a constant value of 0.001 (0.1%). This reflects the assumption that containment relationships are relatively rare compared to other geometric relationships like overlap. The small selectivity value is intentionally conservative to encourage the optimizer to use geometric indexes (typically GiST indexes) when available.

As noted in the source file comments, these selectivity estimates are considered "bogus" placeholders - without knowledge of the actual key distribution in the index, accurate selectivity prediction is not possible. The current implementation prioritizes ensuring that geometric indexes are utilized by the query planner.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function call context, though this particular function doesn't examine any of the provided arguments
## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_FLOAT8 (macro for returning float8 values)
- Called from (representative examples):
  - Used as oprjoin selectivity estimator for geometric containment operators in pg_operator catalog
  - Applied to operators like polygon @> polygon, box <@ box, circle @> point, etc.

## Notes and Other Information
- This function is part of PostgreSQL's cost-based query optimizer infrastructure
- Returns a hardcoded selectivity estimate of 0.001 (0.1%)
- Used for containment operations which are considered tighter constraints than overlap operations (which use areasel/areajoinsel with 0.5% selectivity)
- The function is registered in src/include/catalog/pg_proc.dat with OID and marked as stable (provolatile => 's')
- Associated with numerous geometric operators in pg_operator.dat including polygon, box, circle, and point containment operations
- Part of the geo_selfuncs.c file which contains selectivity functions specifically for geometric operators
- The low selectivity value is designed to encourage index usage, as GiST indexes are particularly beneficial for geometric queries
- Future improvements would require statistical analysis of actual geometric data distributions to provide more accurate estimates

## Simplified Source

```c
Datum contjoinsel(PG_FUNCTION_ARGS) {
    // Return conservative join selectivity estimate for containment operations
    // Similar to contsel but for join operations
    PG_RETURN_FLOAT8(0.001);
}
```