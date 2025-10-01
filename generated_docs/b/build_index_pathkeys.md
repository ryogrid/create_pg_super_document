# build_index_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:738-841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L738-L841)

## Overview
Builds a pathkeys list that describes the ordering induced by an index scan using the given index, supporting both forward and backward scan directions.

## Definition

```c
List *
build_index_pathkeys(PlannerInfo *root,
					 IndexOptInfo *index,
					 ScanDirection scandir)
```
## Detailed Description
This function constructs a canonical pathkeys list representing the sort order that would be produced by scanning a given index. It is a fundamental component of PostgreSQL's index-based query optimization, allowing the planner to understand what orderings are naturally available from index scans without requiring additional sorting operations.

The function iterates through the key columns of the index (excluding INCLUDE columns which don't affect ordering), creating pathkeys for each column that can contribute to the sort order. For backward scans, it reverses the sort order and null positioning. The algorithm stops early if it encounters a column that is not relevant to the current query (not part of an EquivalenceClass), except for boolean columns which have special handling.

The resulting pathkeys list is canonical, meaning redundant pathkeys are removed, and may contain fewer entries than the number of index key columns.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and metadata
- : IndexOptInfo structure describing the index, including sort operators and column information  
- : Scan direction (forward or backward) for which to build pathkeys

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsBackward
  - [make_pathkey_from_sortinfo](../m/make_pathkey_from_sortinfo.md)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md)
  - [indexcol_is_bool_constant_for_query](../i/indexcol_is_bool_constant_for_query.md)
  - [IndexOptInfo](../I/IndexOptInfo.md) (type)
  - ScanDirection (type)
  - [PathKey](../P/PathKey.md) (type)
- Called from (representative examples):
  - [build_index_paths](build_index_paths.md)

## Notes and Other Information
- Returns NIL for non-orderable indexes (those without sort operator families)
- Only processes key columns of indexes, skipping INCLUDE columns which don't affect ordering
- Handles boolean index columns specially, allowing continuation past boolean columns that are constant for the query
- The caller should use truncate_useless_pathkeys() to potentially remove additional unnecessary pathkeys
- Part of the NEW PATHKEY FORMATION infrastructure in PostgreSQL's query planner
- Supports both forward and backward index scans with appropriate sort order reversal

## Simplified Source

```c
List *
build_index_pathkeys(PlannerInfo *root, IndexOptInfo *index, ScanDirection scandir)
{
    List *pathkeys = NIL;
    int i = 0;

    // Return NIL for non-orderable indexes
    if (index->sortopfamily == NULL)
        return NIL;

    // Process each key column (skip INCLUDE columns)
    foreach(lc, index->indextlist)
    {
        TargetEntry *indextle = (TargetEntry *) lfirst(lc);

        if (i >= index->nkeycolumns)
            break;  // Skip INCLUDE columns

        // Determine sort order based on scan direction
        bool reverse_sort = ScanDirectionIsBackward(scandir) ?
                           !index->reverse_sort[i] : index->reverse_sort[i];
        bool nulls_first = ScanDirectionIsBackward(scandir) ?
                          !index->nulls_first[i] : index->nulls_first[i];

        // Try to create a pathkey for this index column
        PathKey *pathkey = make_pathkey_from_sortinfo(root, indextle->expr,
                                                     index->sortopfamily[i],
                                                     index->opcintype[i],
                                                     index->indexcollations[i],
                                                     reverse_sort, nulls_first,
                                                     0, index->rel->relids, false);

        if (pathkey)
        {
            // Add pathkey if it's not redundant
            if (!pathkey_is_redundant(pathkey, pathkeys))
                pathkeys = lappend(pathkeys, pathkey);
        }
        else
        {
            // Stop unless this is a boolean constant column
            if (!indexcol_is_bool_constant_for_query(root, index, i))
                break;
        }

        i++;
    }

    return pathkeys;
}
```