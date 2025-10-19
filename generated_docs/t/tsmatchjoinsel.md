# tsmatchjoinsel

## Location
[src/backend/tsearch/ts_selfuncs.c:139-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_selfuncs.c#L139-L149)

## Overview
Provides join selectivity estimation for the "@@" operator between tsvector and tsquery data types in join operations.

## Definition

```c
Datum
tsmatchjoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a join selectivity estimation function for the "@@" text search match operator when used in join conditions. Unlike restriction selectivity (handled by ), join selectivity estimates the fraction of row pairs that will satisfy the join condition between two relations.

Currently, this function uses a simple implementation that returns a default selectivity estimate without performing sophisticated analysis. This is a common pattern in PostgreSQL for operators where accurate join selectivity estimation is complex or not yet implemented.

## Parameters / Member Variables
The function uses PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS):
- : PlannerInfo pointer containing planner context information
- : OID of the operator being estimated
- : List of arguments to the operator
- : SpecialJoinInfo containing join-specific information

## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_TS_MATCH_SEL: Default selectivity constant used as fallback
- Called from (representative examples):
  - PostgreSQL query planner during join selectivity estimation phase

## Notes and Other Information
- Currently implements a placeholder/stub approach, always returning DEFAULT_TS_MATCH_SEL
- The comment "for the moment we just punt" indicates this is a simplified implementation
- Future enhancements could analyze the structure of TSQuery expressions and TSVector statistics to provide more accurate join selectivity estimates
- Part of PostgreSQL's text search infrastructure (tsearch module)
- Registered in PostgreSQL's system catalogs as the join selectivity estimator for the @@ operator

## Simplified Source

```c
Datum tsmatchjoinsel(PG_FUNCTION_ARGS) {
    // Return default selectivity estimate for tsvector @@ tsquery joins
    PG_RETURN_FLOAT8(DEFAULT_TS_MATCH_SEL);
}
```