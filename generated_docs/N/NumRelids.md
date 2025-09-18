# NumRelids

## Location
src/backend/optimizer/util/clauses.c: 2130 - 2146

## Overview
Counts the number of different base relations referenced in a given clause, excluding outer join relations from the count.

## Definition
```c
int NumRelids(PlannerInfo *root, Node *clause)
```

## Detailed Description
This function (formerly known as clause_relids) analyzes a clause and returns the count of distinct base relations it references. The function performs the following operations:

1. Extracts all relation identifiers (varnos) from the clause using pull_varnos()
2. Removes any outer join relations from the set, as these are handled specially in query planning
3. Counts the remaining relation identifiers
4. Cleans up allocated memory and returns the count

This information is crucial for query optimization decisions, particularly in determining join order, selectivity estimation, and clause placement within the query plan. The exclusion of outer join relations ensures that the count reflects only the base relations that truly constrain the clause's evaluation.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and metadata
- `clause`: The expression node whose relation references are to be counted

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varnos](../p/pull_varnos.md) (extracts all relation identifiers from the clause)
  - [bms_del_members](../b/bms_del_members.md) (removes outer join relations from the relation set)
  - [bms_num_members](../b/bms_num_members.md) (counts the number of relations in the bitmap set)
  - [bms_free](../b/bms_free.md) (deallocates the bitmap memory)
- Called from (representative examples):
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md) (for selectivity estimation)
  - [treat_as_join_clause](../t/treat_as_join_clause.md) (to determine join clause handling)
  - [rowcomparesel](../r/rowcomparesel.md) (in selectivity functions)

## Notes and Other Information
- Formerly named clause_relids, renamed for clarity
- Excludes outer join relations from the count as they require special handling in query planning
- Used extensively in query optimization for join ordering and selectivity calculations
- Returns 0 for pseudo-constant clauses that reference no base relations
- Memory management is handled internally - the function cleans up any allocated bitmap structures