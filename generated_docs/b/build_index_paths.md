# build_index_paths

## Location
[src/backend/optimizer/path/indxpath.c:804-1085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L804-L1085)

## Overview
 constructs zero or more IndexPaths (and partial IndexPaths) for a given index and set of index clauses, supporting both forward and backward scans when beneficial.

## Definition

```c
struct all matching IndexPaths for the relation.
 *
 * Here we must scan all indexes of the relation, since a bitmap OR tree
 * can use multiple indexes.
 *
 * The caller actually supplies two lists of restriction clauses: some
 * "current" ones and some "other" ones.  Both lists can be used freely
 * to match keys of the index, but an index must use at least one of the
 * "current" clauses to be considered usable.  The motivation for this is
 * examples like
 *		WHERE (x = 42) AND (... OR (y = 52 AND z = 77) OR ....)
 * While we are considering the y/z subclause of the OR, we can use "x = 42"
 * as one of the available index conditions;
```
## Detailed Description
This comprehensive function builds IndexPaths through a systematic 5-step process:

1. **Clause Combination**: Combines per-column IndexClause lists into an overall ordered list (by index key column), handling ScalarArrayOpExpr clauses based on index AM support and caller preferences.

2. **Pathkey Analysis**: Computes pathkeys describing the index's ordering and determines how many are useful for the current query, considering both natural index ordering and distance ordering operators.

3. **Index-Only Scan Check**: Determines if an index-only scan is possible by checking if all required columns are available in the index.

4. **Forward Scan Generation**: Creates IndexPaths for forward scans when there are relevant restriction clauses, useful pathkeys, useful predicates, or index-only scan possibilities. Also considers parallel index scans when appropriate.

5. **Backward Scan Generation**: For ordered indexes, generates backward scan IndexPaths when the reverse ordering would be useful for the query.

The function handles different scan types (ST_INDEXSCAN, ST_BITMAPSCAN, ST_ANYSCAN) and ensures compatibility with the index's access method capabilities.

## Parameters / Member Variables
- : PlannerInfo containing planner state and configuration
- : RelOptInfo representing the heap relation being scanned  
- : IndexOptInfo describing the index for path generation
- : IndexClauseSet containing indexable clauses organized by column
- : Whether the index has a useful predicate for this query context
- : ScanTypeControl indicating desired scan types (plain, bitmap, or both)
- : Optional flag to skip ScalarArrayOpExpr clauses unsupported by index AM

## Dependencies
- Functions called/Symbols referenced:
  - [create_index_path](../c/create_index_path.md)
  - [build_index_pathkeys](build_index_pathkeys.md)
  - [check_index_only](../c/check_index_only.md)
  - [match_pathkeys_to_index](../m/match_pathkeys_to_index.md)
  - [get_loop_count](../g/get_loop_count.md)
  - [has_useful_pathkeys](../h/has_useful_pathkeys.md)
  - [truncate_useless_pathkeys](../t/truncate_useless_pathkeys.md)
  - [add_partial_path](../a/add_partial_path.md)
- Called from (representative examples):
  - [get_index_paths](../g/get_index_paths.md)
  - [build_paths_for_OR](build_paths_for_OR.md)

## Notes and Other Information
- Returns paths to caller rather than immediately submitting them via add_path()
- Handles both regular and parallel index scans when conditions are met
- Supports incremental sort by matching prefixes of query pathkeys to index ordering
- Enforces amoptionalkey restrictions for indexes that require at least one matching clause
- The function can return an empty list if no viable paths can be constructed
- Parallel index scans are not supported for bitmap scans

## Simplified Source

```c
static List *
build_index_paths(PlannerInfo *root, RelOptInfo *rel,
                  IndexOptInfo *index, IndexClauseSet *clauses,
                  bool useful_predicate, ScanTypeControl scantype,
                  bool *skip_nonnative_saop)
{
    List *result = NIL;
    List *index_clauses = NIL;
    Relids outer_relids;
    double loop_count;
    List *useful_pathkeys = NIL;
    bool index_only_scan;

    // Check that index supports the desired scan type
    switch (scantype) {
        case ST_INDEXSCAN:
            if (!index->amhasgettuple) return NIL;
            break;
        case ST_BITMAPSCAN:
            if (!index->amhasgetbitmap) return NIL;
            break;
        case ST_ANYSCAN:
            break; // either scan type OK
    }

    // Step 1: Combine per-column clauses into overall clause list
    outer_relids = bms_copy(rel->lateral_relids);
    for (int indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
        ListCell *lc;
        foreach(lc, clauses->indexclauses[indexcol]) {
            IndexClause *iclause = (IndexClause *) lfirst(lc);
            RestrictInfo *rinfo = iclause->rinfo;

            // Skip unsupported ScalarArrayOpExpr if requested
            if (skip_nonnative_saop && !index->amsearcharray &&
                IsA(rinfo->clause, ScalarArrayOpExpr)) {
                *skip_nonnative_saop = true;
                continue;
            }

            index_clauses = lappend(index_clauses, iclause);
            outer_relids = bms_add_members(outer_relids, rinfo->clause_relids);
        }

        // Check amoptionalkey restriction for first column
        if (index_clauses == NIL && !index->amoptionalkey)
            return NIL;
    }

    outer_relids = bms_del_member(outer_relids, rel->relid);
    loop_count = get_loop_count(root, rel->relid, outer_relids);

    // Step 2: Compute useful pathkeys for ordering
    bool pathkeys_useful = (scantype != ST_BITMAPSCAN && has_useful_pathkeys(root, rel));
    bool index_ordered = (index->sortopfamily != NULL);

    if (index_ordered && pathkeys_useful) {
        List *index_pathkeys = build_index_pathkeys(root, index, ForwardScanDirection);
        useful_pathkeys = truncate_useless_pathkeys(root, rel, index_pathkeys);
    } else if (index->amcanorderbyop && pathkeys_useful) {
        // Handle distance ordering for queries like nearest neighbor
        List *orderbyclauses, *orderbyclausecols;
        match_pathkeys_to_index(index, root->query_pathkeys,
                               &orderbyclauses, &orderbyclausecols);
        if (list_length(root->query_pathkeys) == list_length(orderbyclauses))
            useful_pathkeys = root->query_pathkeys;
        else
            useful_pathkeys = list_copy_head(root->query_pathkeys,
                                           list_length(orderbyclauses));
    }

    // Step 3: Check if index-only scan is possible
    index_only_scan = (scantype != ST_BITMAPSCAN && check_index_only(rel, index));

    // Step 4: Generate forward scan path if worthwhile
    if (index_clauses != NIL || useful_pathkeys != NIL ||
        useful_predicate || index_only_scan) {

        IndexPath *ipath = create_index_path(root, index, index_clauses,
                                           orderbyclauses, orderbyclausecols,
                                           useful_pathkeys, ForwardScanDirection,
                                           index_only_scan, outer_relids,
                                           loop_count, false);
        result = lappend(result, ipath);

        // Consider parallel index scan if appropriate
        if (index->amcanparallel && rel->consider_parallel &&
            outer_relids == NULL && scantype != ST_BITMAPSCAN) {
            IndexPath *parallel_path = create_index_path(root, index, index_clauses,
                                                       orderbyclauses, orderbyclausecols,
                                                       useful_pathkeys, ForwardScanDirection,
                                                       index_only_scan, outer_relids,
                                                       loop_count, true);
            if (parallel_path->path.parallel_workers > 0)
                add_partial_path(rel, (Path *) parallel_path);
            else
                pfree(parallel_path);
        }
    }

    // Step 5: Generate backward scan path for ordered indexes
    if (index_ordered && pathkeys_useful) {
        List *backward_pathkeys = build_index_pathkeys(root, index, BackwardScanDirection);
        List *useful_backward = truncate_useless_pathkeys(root, rel, backward_pathkeys);

        if (useful_backward != NIL) {
            IndexPath *backward_path = create_index_path(root, index, index_clauses,
                                                       NIL, NIL, useful_backward,
                                                       BackwardScanDirection, index_only_scan,
                                                       outer_relids, loop_count, false);
            result = lappend(result, backward_path);

            // Parallel backward scan if appropriate
            if (index->amcanparallel && rel->consider_parallel &&
                outer_relids == NULL && scantype != ST_BITMAPSCAN) {
                IndexPath *parallel_backward = create_index_path(root, index, index_clauses,
                                                               NIL, NIL, useful_backward,
                                                               BackwardScanDirection, index_only_scan,
                                                               outer_relids, loop_count, true);
                if (parallel_backward->path.parallel_workers > 0)
                    add_partial_path(rel, (Path *) parallel_backward);
                else
                    pfree(parallel_backward);
            }
        }
    }

    return result;
}
```