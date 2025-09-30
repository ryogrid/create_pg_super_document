# group_keys_reorder_by_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:368-464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L368-L464)

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
  - [list_copy_head](../l/list_copy_head.md) (creates subset of pathkeys)
  - [PathKey](../P/PathKey.md) (pathkey structure type)
  - [SortGroupClause](../S/SortGroupClause.md) (sort group clause type)
  - foreach_current_index (gets current list iteration index)
  - [list_member_ptr](../l/list_member_ptr.md) (checks pointer membership in list)
  - [get_sortgroupref_clause_noerr](get_sortgroupref_clause_noerr.md) (retrieves sort group clause safely)
  - [list_concat_unique_ptr](../l/list_concat_unique_ptr.md) (concatenates lists avoiding duplicates)
  - [list_free](../l/list_free.md) (releases memory)
- Called from:
  - [get_useful_group_keys_orderings](get_useful_group_keys_orderings.md)

## Notes and Other Information
This function is static and serves as a helper for get_useful_group_keys_orderings. It's designed to handle the complexity of matching pathkeys with GROUP BY clauses while avoiding issues with aggregate pathkeys that have invalid sortref values. The function returns the number of successfully matched pathkeys, which indicates how many GROUP BY keys can benefit from the existing sort order. The reordering is essential for incremental sort and other optimization techniques that can leverage partial ordering.

## Simplified Source

```c
static int
group_keys_reorder_by_pathkeys(List *pathkeys, List **group_pathkeys,
                               List **group_clauses, int num_groupby_pathkeys)
{
    List *new_group_pathkeys = NIL;
    List *new_group_clauses = NIL;
    List *grouping_pathkeys;
    ListCell *lc;
    int n;

    if (pathkeys == NIL || *group_pathkeys == NIL)
        return 0;

    // Create subset of pathkeys to avoid aggregate pathkey issues
    grouping_pathkeys = list_copy_head(*group_pathkeys, num_groupby_pathkeys);

    // Walk input pathkeys and find matching GROUP BY keys
    foreach(lc, pathkeys) {
        PathKey *pathkey = (PathKey *) lfirst(lc);
        SortGroupClause *sgc;

        // Stop if we've exceeded the groupby range, can't find the pathkey,
        // or the pathkey has no sortref
        if (foreach_current_index(lc) >= num_groupby_pathkeys ||
            !list_member_ptr(grouping_pathkeys, pathkey) ||
            pathkey->pk_eclass->ec_sortref == 0)
            break;

        // Look up the corresponding GROUP BY clause
        sgc = get_sortgroupref_clause_noerr(pathkey->pk_eclass->ec_sortref,
                                           *group_clauses);
        if (!sgc)
            break;  // No matching grouping clause

        Assert(OidIsValid(sgc->sortop));

        // Add matching pathkey and clause to reordered lists
        new_group_pathkeys = lappend(new_group_pathkeys, pathkey);
        new_group_clauses = lappend(new_group_clauses, sgc);
    }

    // Count how many pathkeys matched
    n = list_length(new_group_pathkeys);

    // Combine reordered keys with remaining keys (maintaining uniqueness)
    *group_pathkeys = list_concat_unique_ptr(new_group_pathkeys, *group_pathkeys);
    *group_clauses = list_concat_unique_ptr(new_group_clauses, *group_clauses);

    list_free(grouping_pathkeys);
    return n;  // Number of pathkeys that matched
}
```