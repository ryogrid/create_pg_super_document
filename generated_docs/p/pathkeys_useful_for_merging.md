# pathkeys_useful_for_merging

## Location
src/backend/optimizer/path/pathkeys.c: 2033 - 2099

## Overview
This function counts the number of pathkeys that may be useful for mergejoins above the given relation.

## Definition


## Detailed Description
The function evaluates the usefulness of pathkeys for potential merge join operations by determining how many leading pathkeys can participate in merge joins. It uses two primary strategies to identify useful pathkeys:

1. **Equivalence Class Analysis**: Checks if the pathkey's equivalence class contains members from relations not yet joined, indicating potential for mergejoin clauses
2. **Join Info Inspection**: Searches the relation's joininfo list for non-EquivalenceClass-derivable join clauses that might still be mergejoinable

Key optimization features:
- **Direction Filtering**: Only considers pathkeys in the "right" merge direction using right_merge_direction() heuristic to avoid doubling mergejoin paths
- **Early Termination**: Stops counting when encountering the first non-useful pathkey, since subsequent pathkeys become useless for merging
- **Overoptimistic Approach**: May count pathkeys as useful even if corresponding joinclauses require different sets of relations, prioritizing simplicity over precision

The function supports the path optimization strategy of only retaining useful pathkey prefixes in add_path().

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : RelOptInfo for the relation being analyzed for potential merge join participation
- : List of pathkeys to evaluate for merge join usefulness

## Dependencies
- Functions called/Symbols referenced:
  - right_merge_direction
  - eclass_useful_for_merging
  - update_mergeclause_eclasses
  - PathKey
- Called from (representative examples):
  - truncate_useless_pathkeys (src/backend/optimizer/path/pathkeys.c:2219)

## Notes and Other Information
- This is a static function within the pathkeys.c module
- Part of the pathkey usefulness checking infrastructure that optimizes path consideration in add_path()
- Uses heuristic to prefer merge directions that match ORDER BY clauses to avoid final sort steps
- Considers pathkeys potentially useful if they correspond to merge ordering of either side of any joinclause
- The overoptimistic approach trades precision for computational efficiency
- Critical for preventing add_path() from considering paths with spuriously different orderings