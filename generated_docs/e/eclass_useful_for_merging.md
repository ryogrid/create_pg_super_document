# eclass_useful_for_merging

## Location
[src/backend/optimizer/path/equivclass.c:3207-3264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L3207-L3264)

## Overview
Detects whether an equivalence class could produce any mergejoinable join clauses against a specified relation as a heuristic test for query optimization.

## Definition

```c
bool
eclass_useful_for_merging(PlannerInfo *root,
						  EquivalenceClass *eclass,
						  RelOptInfo *rel)
```
## Detailed Description
This function performs a heuristic test to determine if an equivalence class (EC) could potentially produce mergejoinable join clauses when joining with a specified relation. The function is designed to be optimistic - it's better to return "yes" incorrectly than "no", as this is used for optimization decisions rather than correctness.

The function checks several conditions:
1. Ensures the EC is not merged, constant, or single-member
2. Handles child relations by considering their topmost parent
3. Verifies that not all EC members are already in the target relation
4. Looks for at least one EC member that doesn't overlap with the target relation

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and information
- : EquivalenceClass to test for potential merge join clause generation
- : RelOptInfo representing the relation to test merging potential against

## Dependencies
- Functions called/Symbols referenced:
  - IS_OTHER_REL (macro for checking relation type)
  - bms_is_empty (bitmap set emptiness check)
  - [bms_is_subset](../b/bms_is_subset.md) (bitmap set subset test)
  - [bms_overlap](../b/bms_overlap.md) (bitmap set overlap test)
  - list_length (list length utility)
- Called from (representative examples):
  - [pathkeys_useful_for_merging](../p/pathkeys_useful_for_merging.md) (src/backend/optimizer/path/pathkeys.c:2054)

## Notes and Other Information
- This is explicitly designed as a heuristic that can be overoptimistic
- The function doesn't check ec_broken flag, relying on member analysis for efficiency
- Child relations are handled by examining their topmost parent relations
- Returns false for constant or single-member equivalence classes as they won't generate useful join clauses
- The function ignores cross-type operator availability details for performance reasons