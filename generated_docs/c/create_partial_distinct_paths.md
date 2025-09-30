# create_partial_distinct_paths

## Location
[src/backend/optimizer/plan/planner.c:4900-5098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L4900-L5098)

## Overview
Creates partial distinct paths for parallel execution by processing input relation's partial paths and adding unique/aggregate paths to the UPPERREL_PARTIAL_DISTINCT relation, with Gather/GatherMerge paths on top to remove duplicates from parallel workers.

## Definition

```c
static void
create_partial_distinct_paths(PlannerInfo *root, RelOptInfo *input_rel,
							  RelOptInfo *final_distinct_rel,
							  PathTarget *target)
```
## Detailed Description
This function is responsible for creating partial execution paths for DISTINCT operations in parallel query execution. It processes the input relation's partial paths and generates appropriate paths for the partial distinct phase of query execution. The function handles both sort-based and hash-based approaches to eliminate duplicates within each parallel worker, then creates Gather paths to combine results from multiple workers. The final step involves calling create_final_distinct_paths to handle any remaining duplicates that may arise from combining parallel worker results.

The function implements several optimization strategies:
- Uses incremental sorting when paths are partially sorted
- Applies limit paths when all tuples have the same distinct value
- Creates hash aggregate paths when hashing is possible and enabled
- Integrates with FDW (Foreign Data Wrapper) systems for distributed query processing
- Supports extension hooks for custom path generation

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and configuration
- : RelOptInfo for the input relation containing partial paths to process  
- : RelOptInfo for the final distinct relation where complete paths will be stored
- : PathTarget specifying the target list and sorting requirements for the distinct operation

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [get_sortgrouplist_exprs](../g/get_sortgrouplist_exprs.md)
  - [estimate_num_groups](../e/estimate_num_groups.md)
  - [grouping_is_sortable](../g/grouping_is_sortable.md)
  - [pathkeys_count_contained_in](../p/pathkeys_count_contained_in.md)
  - [create_sort_path](create_sort_path.md)
  - [create_incremental_sort_path](create_incremental_sort_path.md)
  - [create_limit_path](create_limit_path.md)
  - [create_upper_unique_path](create_upper_unique_path.md)
  - [create_agg_path](create_agg_path.md)
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md)
  - [create_final_distinct_paths](create_final_distinct_paths.md)
- Called from:
  - [create_distinct_paths](create_distinct_paths.md)

## Notes and Other Information
- Early returns if input relation has no partial paths or uses DISTINCT ON (which cannot be parallelized)
- Handles the special case where distinct_pathkeys is NIL by applying limit paths to restrict each worker to 1 tuple
- Respects enable_hashagg and enable_incremental_sort configuration parameters
- Preserves FDW relationship information from input to partial distinct relation
- Uses AGGSPLIT_SIMPLE for hash aggregation in the partial phase
- The function is part of the upper-level query planning infrastructure for parallel DISTINCT operations

## Simplified Source

```c
static void
create_partial_distinct_paths(PlannerInfo *root, RelOptInfo *input_rel,
                             RelOptInfo *final_distinct_rel,
                             PathTarget *target)
{
    RelOptInfo *partial_distinct_rel;
    Query *parse = root->parse;
    double numDistinctRows;
    Path *cheapest_partial_path;

    // Early exit conditions for parallel processing
    if (!input_rel->consider_parallel || input_rel->partial_pathlist == NIL)
        return;

    // Cannot parallelize DISTINCT ON operations
    if (parse->hasDistinctOn)
        return;

    // Set up partial distinct relation
    partial_distinct_rel = fetch_upper_rel(root, UPPERREL_PARTIAL_DISTINCT, NULL);
    partial_distinct_rel->reltarget = target;
    partial_distinct_rel->consider_parallel = input_rel->consider_parallel;

    // Copy FDW information from input relation
    partial_distinct_rel->serverid = input_rel->serverid;
    partial_distinct_rel->userid = input_rel->userid;
    partial_distinct_rel->useridiscurrent = input_rel->useridiscurrent;
    partial_distinct_rel->fdwroutine = input_rel->fdwroutine;

    cheapest_partial_path = linitial(input_rel->partial_pathlist);

    // Estimate distinct rows per worker
    List *distinctExprs = get_sortgrouplist_exprs(root->processed_distinctClause, parse->targetList);
    numDistinctRows = estimate_num_groups(root, distinctExprs, cheapest_partial_path->rows, NULL, NULL);

    // Try sort-based distinct paths
    if (grouping_is_sortable(root->processed_distinctClause)) {
        foreach(lc, input_rel->partial_pathlist) {
            Path *input_path = (Path *) lfirst(lc);
            Path *sorted_path;
            bool is_sorted;
            int presorted_keys;

            // Check if path is already sorted appropriately
            is_sorted = pathkeys_count_contained_in(root->distinct_pathkeys,
                                                   input_path->pathkeys, &presorted_keys);

            if (is_sorted) {
                sorted_path = input_path;
            } else {
                // Skip non-essential paths (except cheapest)
                if (input_path != cheapest_partial_path &&
                    (presorted_keys == 0 || !enable_incremental_sort))
                    continue;

                // Create sort or incremental sort path
                if (presorted_keys == 0 || !enable_incremental_sort)
                    sorted_path = create_sort_path(root, partial_distinct_rel, input_path,
                                                 root->distinct_pathkeys, -1.0);
                else
                    sorted_path = create_incremental_sort_path(root, partial_distinct_rel, input_path,
                                                             root->distinct_pathkeys, presorted_keys, -1.0);
            }

            // Handle special case: all tuples have same distinct value
            if (root->distinct_pathkeys == NIL) {
                Node *limitCount = makeConst(INT8OID, -1, InvalidOid, sizeof(int64),
                                           Int64GetDatum(1), false, FLOAT8PASSBYVAL);
                add_partial_path(partial_distinct_rel,
                               create_limit_path(root, partial_distinct_rel, sorted_path,
                                               NULL, limitCount, LIMIT_OPTION_COUNT, 0, 1));
            } else {
                add_partial_path(partial_distinct_rel,
                               create_upper_unique_path(root, partial_distinct_rel, sorted_path,
                                                      list_length(root->distinct_pathkeys), numDistinctRows));
            }
        }
    }

    // Try hash-based distinct paths
    if (enable_hashagg && grouping_is_hashable(root->processed_distinctClause)) {
        add_partial_path(partial_distinct_rel,
                       create_agg_path(root, partial_distinct_rel, cheapest_partial_path,
                                     cheapest_partial_path->pathtarget, AGG_HASHED,
                                     AGGSPLIT_SIMPLE, root->processed_distinctClause,
                                     NIL, NULL, numDistinctRows));
    }

    // Allow FDW to add foreign paths
    if (partial_distinct_rel->fdwroutine && partial_distinct_rel->fdwroutine->GetForeignUpperPaths)
        partial_distinct_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_PARTIAL_DISTINCT,
                                                             input_rel, partial_distinct_rel, NULL);

    // Allow extensions to add custom paths
    if (create_upper_paths_hook)
        (*create_upper_paths_hook)(root, UPPERREL_PARTIAL_DISTINCT,
                                 input_rel, partial_distinct_rel, NULL);

    // Generate gather paths and final distinct paths if we have partial paths
    if (partial_distinct_rel->partial_pathlist != NIL) {
        generate_useful_gather_paths(root, partial_distinct_rel, true);
        set_cheapest(partial_distinct_rel);

        // Create final distinct paths to remove duplicates from parallel workers
        create_final_distinct_paths(root, partial_distinct_rel, final_distinct_rel);
    }
}
```