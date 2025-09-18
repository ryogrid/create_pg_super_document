# select_outer_pathkeys_for_merge

## Location
src/backend/optimizer/path/pathkeys.c: 1639 - 1834

## Overview
This function builds a pathkey list representing a possible sort ordering that can be used with given mergeclauses for merge join operations.

## Definition


## Detailed Description
The function creates an optimal pathkey ordering for the outer relation in a merge join, prioritizing query_pathkeys compatibility and equivalence class popularity. The algorithm works in several phases:

1. **Extract and Score Equivalence Classes**: Collects unique equivalence classes from mergeclauses and scores them based on their potential for future joins (popularity)
2. **Query Pathkeys Matching**: Attempts to match or use a prefix of root->query_pathkeys to avoid additional sorting or enable incremental sorts
3. **Popularity-Based Ordering**: Adds remaining equivalence classes in order of popularity (highest score first)

Key optimization strategies:
- Prefers matching query_pathkeys when all ECs are available
- Uses query_pathkeys prefix when it covers the entire join condition
- Prioritizes "popular" equivalence classes (those with more unmatched members) for better higher-level merge join opportunities

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context, including query_pathkeys
- : List of RestrictInfos for mergejoin clauses marked with outer_is_left indicators
- : The join relation being constructed, used to determine which equivalence class members are potential future join partners

## Dependencies
- Functions called/Symbols referenced:
  - update_mergeclause_eclasses
  - bms_overlap
  - list_copy
  - list_copy_head
  - make_canonical_pathkey
  - linitial_oid
  - pathkey_is_redundant
  - EquivalenceClass
  - EquivalenceMember
  - PathKey
  - BTLessStrategyNumber
- Called from (representative examples):
  - sort_inner_and_outer (src/backend/optimizer/path/joinpath.c:1379)

## Notes and Other Information
- Returns NIL if no mergeclauses are provided
- Assumes a sort is required, so doesn't try to match existing outer relation ordering
- Uses a simple selection sort algorithm for ordering equivalence classes by popularity (acceptable for typically small lists)
- Popularity scoring counts equivalence class members that don't overlap with the current joinrel (potential future join partners)
- The function enables incremental sorting optimizations by trying to match query_pathkeys prefixes
- Creates canonical pathkeys using BTLessStrategyNumber as the default sort strategy