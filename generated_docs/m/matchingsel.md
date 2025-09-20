# matchingsel

## Location
[src/backend/utils/adt/selfuncs.c:3261-3278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L3261-L3278)

## Overview
A generic selectivity estimation function for matching-type operators that provides reasonable default estimates for operators with match-like behavior on data types with standard statistics.

## Definition

```c
typedef struct
{
	Node	   *var;			/* might be an expression, not just a Var */
	RelOptInfo *rel;			/* relation it belongs to */
	double		ndistinct;		/* # distinct values */
	bool		isdefault;		/* true if DEFAULT_NUM_DISTINCT was used */
} GroupVarInfo;
```
## Detailed Description
This function serves as a generic selectivity estimator for operators that have "matching" semantics - typically operators that test for similarity or pattern matching rather than exact equality. It's designed for use with operators that operate on data types for which PostgreSQL collects standard statistics and where the default estimate (twice DEFAULT_EQ_SEL) provides a reasonable approximation.

The function acts as a PostgreSQL function interface wrapper around the generic restriction selectivity logic, using a predefined default selectivity constant (DEFAULT_MATCHING_SEL) that is typically higher than equality selectivity since matching operators tend to be less selective than exact equality comparisons.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that expands to:
  - : PlannerInfo structure containing query planning context
  - : OID of the operator being analyzed
  - : List of arguments to the operator
  - : Relation ID for variable references (0 if not a simple var)
  - : Collation to use for the operation (extracted via PG_GET_COLLATION)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION: Macro to extract collation from function call context
  - generic_restriction_selectivity: Core selectivity estimation logic
  - DEFAULT_MATCHING_SEL: Default selectivity constant for matching operations
- Called from (representative examples):
  - No direct references found - likely registered as selectivity function in pg_proc catalog

## Notes and Other Information
- Returns a Datum containing a float8 selectivity estimate between 0.0 and 1.0
- Uses DEFAULT_MATCHING_SEL as the fallback estimate when statistics are unavailable
- Intended for operators like pattern matching, similarity testing, or fuzzy matching
- The function assumes that "matching" operations are generally less selective than equality but more selective than inequality
- Registers as a selectivity estimation function that can be associated with operators in the PostgreSQL catalog
- Part of PostgreSQL's extensible selectivity estimation framework allowing custom operators to provide reasonable cost estimates