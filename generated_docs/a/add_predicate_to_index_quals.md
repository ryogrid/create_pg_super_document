# add_predicate_to_index_quals

## Location
[src/backend/utils/adt/selfuncs.c:6833-6853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6833-L6853)

## Overview
Augments index qualifier lists with partial index predicates to improve selectivity estimation accuracy while avoiding redundant clauses.

## Definition

```c
List *
add_predicate_to_index_quals(IndexOptInfo *index, List *indexQuals)
```
## Detailed Description
The  function handles the integration of partial index predicates with explicitly given index qualifiers to produce more accurate selectivity estimates. When an index has a WHERE clause (making it a partial index), this function intelligently combines the index predicate with the query's index qualifiers.

The function implements a sophisticated redundancy detection mechanism to avoid adding predicate clauses that can be proven to be implied by existing index qualifiers. This prevents  from being misled into computing overly conservative (too-low) selectivity estimates due to apparently redundant conditions.

For example, when a query uses qualifier "x = 42" with a partial index "WHERE x >= 40 AND x < 50", the function recognizes that the range predicate is already satisfied by the equality condition and avoids adding redundant constraints.

The function returns a mixed list containing both RestrictInfo nodes (from indexQuals) and raw expression nodes (from the index predicate), which is acceptable for the intended use cases in selectivity estimation.

## Parameters / Member Variables
- `*index`: IndexOptInfo structure containing information about the index, including its predicate (indpred)
- `*indexQuals`: List of RestrictInfo nodes representing the explicitly given index qualifiers from the query
## Dependencies
- Functions called/Symbols referenced:
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [list_concat](../l/list_concat.md)
  - list_make1
  - lfirst
- Called from (representative examples):
  - [genericcostestimate](../g/genericcostestimate.md)
  - [btcostestimate](../b/btcostestimate.md)
  - [gincostestimate](../g/gincostestimate.md)

## Notes and Other Information
- Returns the original indexQuals unchanged if the index has no predicate (not a partial index)
- The bias toward partial indexes when redundancy isn't detected is considered acceptable behavior
- Handles complex implication scenarios but may miss some redundancy cases, leading to conservative estimates
- The mixed return type (RestrictInfo and raw Node types) is specifically designed for compatibility with predicate_implied_by() and clauselist_selectivity()
- Only adds predicate clauses that cannot be proven to be implied by existing qualifiers