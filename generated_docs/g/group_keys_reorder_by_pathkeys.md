# group_keys_reorder_by_pathkeys

## Location
src/backend/optimizer/path/pathkeys.c: 368 - 464

## Overview
Reorders GROUP BY pathkeys and clauses to match a given input pathkey ordering, optimizing for cases where existing sort order can be leveraged for grouping operations.

## Definition
```c
static int group_keys_reorder_by_pathkeys(List *pathkeys, List **group_pathkeys,
                                         List **group_clauses,
                                         int num_groupby_pathkeys)
```

## Detailed Description
This function reorders GROUP BY pathkeys and their corresponding clauses to align with an input pathkey ordering, which is crucial for optimizing grouping operations that can take advantage of existing sort orders. The function processes only the first num_groupby_pathkeys items to avoid issues with aggregate pathkeys that don't reference the query targetlist.

The algorithm walks through the input pathkeys and searches for matching GROUP BY keys within the specified range. For each matching pathkey found, it appends both the pathkey and its corresponding sort group clause to new reordered lists. The process stops as soon as a pathkey without a matching GROUP BY key is encountered, since subsequent pathkeys cannot contribute to the grouping evaluation. Finally, any remaining group pathkeys are appended to maintain completeness.

## Parameters / Member Variables
- `pathkeys`: Input list of pathkeys that defines the desired ordering
- `group_pathkeys`: Pointer to GROUP BY pathkeys list to reorder (modified to point to new list)
- `group_clauses`: Pointer to GROUP BY clauses list to reorder (modified to point to new list)  
- `num_groupby_pathkeys`: Number of first group_pathkeys items to consider for matching

## Dependencies
- Functions called/Symbols referenced:
  - list_copy_head (creates subset of pathkeys)
  - PathKey (pathkey structure type)
  - SortGroupClause (sort group clause type)
  - foreach_current_index (gets current list iteration index)
  - list_member_ptr (checks pointer membership in list)
  - get_sortgroupref_clause_noerr (retrieves sort group clause safely)
  - list_concat_unique_ptr (concatenates lists avoiding duplicates)
  - list_free (releases memory)
- Called from:
  - get_useful_group_keys_orderings

## Notes and Other Information
This function is static and serves as a helper for get_useful_group_keys_orderings. It's designed to handle the complexity of matching pathkeys with GROUP BY clauses while avoiding issues with aggregate pathkeys that have invalid sortref values. The function returns the number of successfully matched pathkeys, which indicates how many GROUP BY keys can benefit from the existing sort order. The reordering is essential for incremental sort and other optimization techniques that can leverage partial ordering.