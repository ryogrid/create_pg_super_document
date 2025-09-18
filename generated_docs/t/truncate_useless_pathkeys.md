# truncate_useless_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:2212-2257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L2212-L2257)

## Overview
Shortens a given pathkey list to only include the useful pathkeys by evaluating their utility across multiple optimization contexts (merging, ordering, grouping, and set operations).

## Definition


## Detailed Description
This function optimizes pathkey lists by truncating them to retain only the pathkeys that provide value for query execution. It evaluates the usefulness of pathkeys across four different optimization contexts:

1. **Merging**: Uses  to determine how many pathkeys are beneficial for merge join operations
2. **Ordering**: Uses  to count pathkeys useful for ORDER BY clause optimization
3. **Grouping**: Uses  to evaluate pathkeys beneficial for GROUP BY operations
4. **Set Operations**: Uses  to assess pathkeys useful for UNION/INTERSECT/EXCEPT operations

The function takes the maximum count from all four evaluations, ensuring that pathkeys useful in any context are preserved. This approach optimizes memory usage and processing efficiency by eliminating pathkeys that provide no optimization benefit while preserving those that could improve performance in any scenario.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and various pathkey requirements
- : RelOptInfo structure representing the relation being optimized
- : List of PathKey structures to evaluate and potentially truncate

## Dependencies
- Functions called/Symbols referenced:
  - [pathkeys_useful_for_merging](../p/pathkeys_useful_for_merging.md) (evaluates merge join utility)
  - [pathkeys_useful_for_ordering](../p/pathkeys_useful_for_ordering.md) (evaluates ORDER BY utility)
  - [pathkeys_useful_for_grouping](../p/pathkeys_useful_for_grouping.md) (evaluates GROUP BY utility)  
  - [pathkeys_useful_for_setop](../p/pathkeys_useful_for_setop.md) (evaluates set operation utility)
  - [list_copy_head](../l/list_copy_head.md) (creates truncated copy of list)
- Called from (representative examples):
  - [build_index_paths](../b/build_index_paths.md)
  - [build_join_pathkeys](../b/build_join_pathkeys.md)

## Notes and Other Information
- Returns NIL if no pathkeys are useful (nuseful == 0)
- Returns the original list unchanged if all pathkeys are useful to avoid unnecessary copying
- Uses  to create a truncated copy when only some pathkeys are useful
- The function safely avoids modifying the input list destructively
- This optimization is crucial for maintaining efficient pathkey management throughout query planning
- Used extensively in index path building and join pathkey construction