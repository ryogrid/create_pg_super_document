# get_useful_group_keys_orderings

## Location
[src/backend/optimizer/path/pathkeys.c:465-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L465-L555)

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
  - [GroupByOrdering](../G/GroupByOrdering.md) (structure for storing pathkey/clause pairs)
  - makeNode (creates new node structures)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md) (checks if path ordering satisfies group requirements)
  - [group_keys_reorder_by_pathkeys](group_keys_reorder_by_pathkeys.md) (reorders keys to match path)
  - [compare_pathkeys](../c/compare_pathkeys.md) (compares pathkey lists)
  - PATHKEYS_EQUAL (comparison result constant)
  - linitial_node (gets first list element safely)
  - for_each_from (iteration macro)
  - [list_difference](../l/list_difference.md) (computes list differences)
  - forboth (iterates two lists simultaneously)
- Called from (representative examples):
  - [add_paths_to_grouping_rel](../a/add_paths_to_grouping_rel.md)
  - [create_partial_grouping_paths](../c/create_partial_grouping_paths.md)

## Notes and Other Information
This function is controlled by the enable_group_by_reordering GUC parameter and does not operate on queries with grouping sets, which have their own complex ordering logic. The function is essential for enabling incremental sort optimizations in GROUP BY operations, allowing the planner to take advantage of existing sort orders to minimize sorting costs. Debug builds include extensive assertion checking to validate the consistency and completeness of generated orderings.

## Simplified Source

```c
List *
get_useful_group_keys_orderings(PlannerInfo *root, Path *path) {
    Query *parse = root->parse;
    List *infos = NIL;
    GroupByOrdering *info;

    // Always include the original GROUP BY ordering
    info = makeNode(GroupByOrdering);
    info->pathkeys = root->group_pathkeys;
    info->clauses = root->processed_groupClause;
    infos = lappend(infos, info);

    // Early exit if reordering disabled or grouping sets present
    if (!enable_group_by_reordering || parse->groupingSets)
        return infos;

    // Try reordering GROUP BY keys to match path ordering
    if (path->pathkeys &&
        !pathkeys_contained_in(path->pathkeys, root->group_pathkeys)) {
        List *pathkeys = root->group_pathkeys;
        List *clauses = root->processed_groupClause;
        int n;

        // Attempt to reorder group keys to match path
        n = group_keys_reorder_by_pathkeys(path->pathkeys, &pathkeys, &clauses,
                                          root->num_groupby_pathkeys);

        // Include reordered version if beneficial
        if (n > 0 &&
            (enable_incremental_sort || n == root->num_groupby_pathkeys) &&
            compare_pathkeys(pathkeys, root->group_pathkeys) != PATHKEYS_EQUAL) {
            info = makeNode(GroupByOrdering);
            info->pathkeys = pathkeys;
            info->clauses = clauses;
            infos = lappend(infos, info);
        }
    }

#ifdef USE_ASSERT_CHECKING
    // Validate consistency of generated orderings
    if (list_length(infos) > 1) {
        GroupByOrdering *pinfo = linitial_node(GroupByOrdering, infos);
        ListCell *lc;

        for_each_from(lc, infos, 1) {
            info = lfirst_node(GroupByOrdering, lc);
            Assert(list_length(info->clauses) == list_length(pinfo->clauses));
            Assert(list_length(info->pathkeys) == list_length(pinfo->pathkeys));
            Assert(list_difference(info->clauses, pinfo->clauses) == NIL);
            Assert(list_difference_ptr(info->pathkeys, pinfo->pathkeys) == NIL);

            // Verify pathkey/clause correspondence
            ListCell *lc1, *lc2;
            forboth(lc1, info->clauses, lc2, info->pathkeys) {
                SortGroupClause *sgc = lfirst_node(SortGroupClause, lc1);
                PathKey *pk = lfirst_node(PathKey, lc2);
                Assert(pk->pk_eclass->ec_sortref == sgc->tleSortGroupRef);
            }
        }
    }
#endif

    return infos;
}
```