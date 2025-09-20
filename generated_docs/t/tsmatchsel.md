# tsmatchsel

## Location
[src/backend/tsearch/ts_selfuncs.c:67-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_selfuncs.c#L67-L138)

## Overview
Computes the selectivity estimate for the "@@" operator between tsvector and tsquery data types in PostgreSQL's text search functionality.

## Definition

```c
structure, must punt */
		selec = DEFAULT_TS_MATCH_SEL;
```
## Detailed Description
 is a selectivity estimation function for the "@@" text search match operator. It calculates the probability that a tsvector @@ tsquery or tsquery @@ tsvector expression will return true. This function is critical for the PostgreSQL query planner to make informed decisions about query execution plans involving full-text search operations.

The function handles both orientations of the match operator (tsvector @@ tsquery and tsquery @@ tsvector) by identifying which operand is the variable and which is the constant. It delegates the actual selectivity computation to  when dealing with a TSQuery constant.

## Parameters / Member Variables
The function uses PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS):
- : PlannerInfo pointer containing planner context information
- : OID of the operator being estimated (currently unused)
- : List of arguments to the operator
- : Relation ID for the variable (used in join contexts)

## Dependencies
- Functions called/Symbols referenced:
  - get_restriction_variable: Extracts variable and constant from operator arguments
  - [tsquerysel](tsquerysel.md): Performs the actual selectivity calculation for TSQuery
  - ReleaseVariableStats: Cleans up variable statistics
  - CLAMP_PROBABILITY: Ensures selectivity is within valid range [0,1]
- Constants used:
  - DEFAULT_TS_MATCH_SEL: Default selectivity when computation is not possible
  - TSQUERYOID: Type OID for TSQuery data type
  - TSVECTOROID: Type OID for TSVector data type
- Called from (representative examples):
  - PostgreSQL query planner during selectivity estimation phase

## Notes and Other Information
- Returns DEFAULT_TS_MATCH_SEL when the expression structure cannot be analyzed
- Handles NULL constants by returning 0.0 selectivity (since @@ is strict)
- Requires one operand to be a variable and the other to be a TSQuery constant
- The function is registered in PostgreSQL's system catalogs as the selectivity estimator for the @@ operator
- Part of PostgreSQL's text search infrastructure (tsearch module)