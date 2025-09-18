# join_is_removable

## Location
src/backend/optimizer/plan/analyzejoins.c: 160 - 328

## Overview
Determines whether a special join (specifically a left join) can be eliminated from the query plan because it will not affect the result set.

## Definition


## Detailed Description
This function implements a critical optimization in PostgreSQL's query planner by analyzing whether a left join can be safely removed without changing the query semantics. The optimization is based on the principle that a left join can be eliminated if:

1. The inner relation can produce at most one matching row for each outer relation row (uniqueness/distinctness)
2. No attributes from the inner relation are needed above the join level
3. No PlaceHolderVars require the inner relation to be evaluated

The function performs comprehensive checks including:
- Verifying the join is a left join to a single base relation
- Ensuring the inner relation is not the query's result relation (important for MERGE operations)
- Checking that the inner relation supports distinctness proofs
- Verifying no inner relation attributes are referenced above the join
- Analyzing PlaceHolderVar dependencies
- Collecting mergejoinable equality clauses that can prove distinctness
- Attempting to prove the inner relation is distinct for the collected clauses

## Parameters / Member Variables
- : PlannerInfo structure containing all planner context and state information
- : SpecialJoinInfo structure describing the specific outer join being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - bms_get_singleton_member: Extracts single member from bitmap set
  - find_base_rel: Locates base relation information
  - rel_supports_distinctness: Checks if relation can have distinctness proven
  - bms_union, bms_copy, bms_add_member: Bitmap set operations
  - bms_is_subset, bms_overlap, bms_is_member: Bitmap set comparisons
  - pull_varnos: Extracts variable relation IDs from expressions
  - clause_sides_match_join: Validates clause structure for join
  - rel_is_distinct_for: Attempts to prove relation distinctness
  - RINFO_IS_PUSHED_DOWN: Macro to check if restriction is pushed down

- Called from (representative examples):
  - remove_useless_joins: Main join elimination function

## Notes and Other Information
- Only handles left joins to single base relations; other join types and complex structures are not supported
- The function includes special handling for MERGE operations by preventing elimination of joins to result relations
- PlaceHolderVar analysis is particularly complex due to the need to ensure evaluation locations remain valid after join removal
- Uses mergejoinable clauses as the basis for distinctness proofs, as these behave like equality for btree operations
- Includes optimizations like starting attribute checks from max_attr and counting down, assuming system attributes are less likely to be referenced
- The distinctness proof is currently the only method implemented, though comments suggest future extensions for other proof methods