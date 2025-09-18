# reduce_unique_semijoins

## Location
src/backend/optimizer/plan/analyzejoins.c: 730 - 805

## Overview
Optimizes semijoins by converting them to plain inner joins when the inner relation is provably unique for the join clauses, eliminating unnecessary semijoin overhead.

## Definition
```c
void reduce_unique_semijoins(PlannerInfo *root)
```

## Detailed Description
This function performs a query optimization by identifying semijoins that can be safely converted to inner joins. The transformation is valid when the inner relation of a semijoin is guaranteed to be unique for the join conditions, meaning each row from the outer relation will match at most one row from the inner relation.

The function works by:
1. Scanning the join_info_list to identify semijoin operations
2. Checking if the semijoin has a single base relation on the right-hand side
3. Verifying that the inner relation supports distinctness analysis
4. Computing the relevant join clauses (both explicit and EC-derived)
5. Testing whether the inner relation is unique for those clauses
6. If uniqueness is proven, removing the SpecialJoinInfo to allow the semijoin to be treated as an inner join

This optimization can significantly improve query performance by enabling more efficient join algorithms and removing the need for duplicate elimination that semijoins typically require.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing query planning information, including the join_info_list to be processed

## Dependencies
- Functions called/Symbols referenced:
  - bms_get_singleton_member (checks for single-member bitmapset)
  - find_base_rel (locates base relation information)
  - rel_supports_distinctness (checks if relation supports uniqueness analysis)
  - bms_union (combines bitmapsets)
  - list_concat (concatenates lists)
  - generate_join_implied_equalities (creates implied join conditions from equivalence classes)
  - innerrel_is_unique (tests uniqueness of inner relation for given conditions)
  - foreach_delete_current (removes current list element during iteration)
- Called from (representative examples):
  - query_planner

## Notes and Other Information
- This optimization happens after reduce_outer_joins because sufficient information for uniqueness analysis is not available during that earlier phase
- The function only considers semijoins to single base relations, as multi-relation right-hand sides are too complex for this analysis
- When a semijoin is reduced, only the SpecialJoinInfo is removed; the join type in the query jointree is left unchanged since it won't be consulted again
- The function processes both explicit join clauses and equivalence class-derived join clauses
- This is a global optimization function called during the main query planning phase
- Located in src/backend/optimizer/plan/analyzejoins.c at lines 730-805