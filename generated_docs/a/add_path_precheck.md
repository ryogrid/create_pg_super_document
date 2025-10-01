# add_path_precheck

## Location
[src/backend/optimizer/util/pathnode.c:642-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L642-L746)

## Overview
Performs a lightweight check to determine whether a proposed path could potentially be accepted before creating the full Path structure.

## Definition

```c
structure that can cope with them.
 *
 *	  Because we don't consider parameterized paths here, we also don't
 *	  need to consider the row counts as a measure of quality: every path will
 *	  produce the same number of rows.  Neither do we need to consider startup
 *	  costs: parallelism is only used for plans that will be run to completion.
 *	  Therefore, this routine is much simpler than add_path: it needs to
 *	  consider only pathkeys and total cost.
 *
 *	  As with add_path, we pfree paths that are found to be dominated by
 *	  another partial path;
```
## Detailed Description
This function provides an optimization for the path creation process by performing a preliminary check before the expensive Path structure creation. It determines if a proposed path with given characteristics could possibly be accepted into the pathlist.

The function searches for existing paths with the same parameterization that would dominate the proposed path on all criteria:
- Total cost (with fuzzy comparison using STD_FUZZ_FACTOR)  
- Startup cost (if consider_startup is true)
- Pathkeys (equal or better ordering)

Key assumptions:
- Row count estimates are too expensive to compute for prechecking
- Paths with superset parameterization generate fewer rows
- Paths with different parameterizations cannot dominate each other

The function leverages the fact that pathlist is sorted by total_cost to exit early when encountering more expensive paths.

## Parameters / Member Variables
- : RelOptInfo structure containing the existing pathlist
- : Estimated startup cost for the proposed path
- : Estimated total cost for the proposed path  
- : Sort ordering for the proposed path
- : Set of required outer relations for parameterization

## Dependencies
- Functions called/Symbols referenced:
  - [compare_pathkeys](../c/compare_pathkeys.md)
  - [bms_equal](../b/bms_equal.md)
  - PATH_REQ_OUTER
  - STD_FUZZ_FACTOR (constant)
  - PATHKEYS_EQUAL, PATHKEYS_BETTER2 (enum values)
  - Cost (type)
  - [PathKeysComparison](../P/PathKeysComparison.md) (type)
- Called from (representative examples):
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)  
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [add_partial_path_precheck](add_partial_path_precheck.md)

## Notes and Other Information
This function is an important performance optimization that avoids creating Path structures that would be immediately discarded. It follows the same policy as add_path regarding parameterized paths having no pathkeys. The early exit capability based on sorted pathlist can significantly reduce planning time for relations with many potential paths.

## Simplified Source

```c
bool add_path_precheck(RelOptInfo *parent_rel,
                      Cost startup_cost, Cost total_cost,
                      List *pathkeys, Relids required_outer) {
    // Parameterized paths are treated as having no pathkeys
    List *new_path_pathkeys = required_outer ? NIL : pathkeys;

    // Determine if startup cost matters for this path type
    bool consider_startup = required_outer ?
        parent_rel->consider_param_startup : parent_rel->consider_startup;

    // Check against existing paths for domination
    ListCell *lc;
    foreach(lc, parent_rel->pathlist) {
        Path *old_path = (Path *) lfirst(lc);

        // Early exit: pathlist is sorted by total cost
        if (total_cost <= old_path->total_cost * STD_FUZZ_FACTOR)
            break;

        // Check if old path dominates new path on costs
        bool old_wins_total = (total_cost > old_path->total_cost * STD_FUZZ_FACTOR);
        bool old_wins_startup = !consider_startup ||
            (startup_cost > old_path->startup_cost * STD_FUZZ_FACTOR);

        if (old_wins_total && old_wins_startup) {
            // Old path wins on costs, check pathkeys
            List *old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
            PathKeysComparison keyscmp = compare_pathkeys(new_path_pathkeys, old_path_pathkeys);

            // Old path dominates if pathkeys are equal or better
            if ((keyscmp == PATHKEYS_EQUAL || keyscmp == PATHKEYS_BETTER2) &&
                bms_equal(required_outer, PATH_REQ_OUTER(old_path))) {
                return false;  // New path is dominated, reject it
            }
        }
    }

    return true;  // New path might be worth adding
}
```