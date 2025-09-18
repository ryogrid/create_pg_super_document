# get_useful_group_keys_orderings

## Location
src/backend/optimizer/path/pathkeys.c: 465 - 555

## Overview
Determines which orderings of GROUP BY keys are potentially interesting for optimization, considering both the original query ordering and path-based reorderings that can leverage existing sort orders.

## Definition
```c
List *get_useful_group_keys_orderings(PlannerInfo *root, Path *path)
```

## Detailed Description
This function generates a list of GroupByOrdering items representing different potentially useful orderings of GROUP BY keys. It always includes the original GROUP BY ordering as processed by preprocess_groupclause() to match the target ORDER BY clause. Additionally, if group-by reordering is enabled and conditions are met, it considers reordering GROUP BY keys to match the input path's ordering, which can enable efficient incremental sorting.

The function evaluates whether alternative orderings are beneficial by checking if the path has a useful sort order that doesn't already contain the required group pathkeys. It uses group_keys_reorder_by_pathkeys to attempt reordering and only includes the alternative if it provides a meaningful benefit (either through incremental sort capabilities or complete ordering match). The function includes comprehensive assertion checking in debug builds to ensure consistency of the generated orderings.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and GROUP BY information
- `path`: Input path whose sort ordering may influence GROUP BY key reordering

## Dependencies
- Functions called/Symbols referenced:
  - GroupByOrdering (structure for storing pathkey/clause pairs)
  - makeNode (creates new node structures)
  - pathkeys_contained_in (checks if path ordering satisfies group requirements)
  - group_keys_reorder_by_pathkeys (reorders keys to match path)
  - compare_pathkeys (compares pathkey lists)
  - PATHKEYS_EQUAL (comparison result constant)
  - linitial_node (gets first list element safely)
  - for_each_from (iteration macro)
  - list_difference (computes list differences)
  - forboth (iterates two lists simultaneously)
- Called from (representative examples):
  - add_paths_to_grouping_rel
  - create_partial_grouping_paths

## Notes and Other Information
This function is controlled by the enable_group_by_reordering GUC parameter and does not operate on queries with grouping sets, which have their own complex ordering logic. The function is essential for enabling incremental sort optimizations in GROUP BY operations, allowing the planner to take advantage of existing sort orders to minimize sorting costs. Debug builds include extensive assertion checking to validate the consistency and completeness of generated orderings.