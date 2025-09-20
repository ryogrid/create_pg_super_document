# eclass_already_used

## Location
[src/backend/optimizer/path/indxpath.c:678-709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L678-L709)

## Overview
Determines whether any join clause usable with a given set of relation IDs was derived from a specified EquivalenceClass, preventing redundant parameterizations.

## Definition

```c
static bool
eclass_already_used(EquivalenceClass *parent_ec, Relids oldrelids,
					List *indexjoinclauses)
```
## Detailed Description
This function implements a crucial optimization in PostgreSQL's parameterized index path generation by detecting when multiple join clauses derived from the same EquivalenceClass would create functionally equivalent parameterizations. 

The function iterates through a list of join clauses and checks if any clause meets both conditions:
1. **Same EquivalenceClass origin**: The clause's parent_ec matches the specified EquivalenceClass
2. **Subset relationship**: The clause's relation set (clause_relids) is a subset of the provided oldrelids

When both conditions are met, it means that combining the current clause with the old relation set would not produce a meaningfully different parameterization than what would already be available from the existing clause derived from the same EquivalenceClass.

This check is essential because EquivalenceClasses can generate multiple logically equivalent join clauses, and without this filtering, the optimizer would waste time exploring redundant combinations that don't improve query execution.

## Parameters / Member Variables
- : EquivalenceClass to check for previous usage
- : Set of relation IDs from a previously considered combination
- : List of IndexClause structures containing join clauses to examine

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md)
- Called from (representative examples):
  - [consider_index_join_outer_rels](../c/consider_index_join_outer_rels.md)

## Notes and Other Information
- Part of the heuristic system that prevents exponential explosion in parameterized path generation
- Works in conjunction with other filters (like bms_subset_compare) to eliminate redundant work
- Specifically targets EquivalenceClass-derived clauses, which are particularly prone to generating equivalent alternatives
- Returns true immediately upon finding the first matching clause, implementing short-circuit evaluation
- Essential for maintaining reasonable planning time in queries with complex join conditions and multiple EquivalenceClasses