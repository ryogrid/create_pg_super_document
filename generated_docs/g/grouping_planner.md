# grouping_planner

## Location
[src/backend/optimizer/plan/planner.c:1335-2076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L1335-L2076)

## Overview
Performs comprehensive planning steps related to grouping, aggregation, window functions, and other high-level query operations on top of the basic scan/join paths produced by query_planner.

## Definition
```c
static void grouping_planner(PlannerInfo *root, double tuple_fraction, SetOperationStmt *setops)
```

## Detailed Description
This function is the core high-level planner that adds all required top-level processing to the scan/join paths produced by query_planner. It handles the planning of complex SQL operations including:

- **Set Operations**: Plans UNION, INTERSECT, EXCEPT operations through plan_set_operations
- **Grouping and Aggregation**: Creates paths for GROUP BY clauses and aggregate functions
- **Window Functions**: Plans window function execution with proper ordering
- **Sorting**: Implements ORDER BY clauses with various optimization strategies
- **DISTINCT Operations**: Plans DISTINCT clause execution
- **Row Locking**: Adds LockRows nodes for FOR UPDATE/SHARE clauses
- **LIMIT/OFFSET**: Implements result limiting with cost estimation
- **DML Operations**: Adds ModifyTable nodes for INSERT/UPDATE/DELETE/MERGE

The function works by creating a series of upper relations (upperrels) that represent different processing stages, with each stage building upon the previous one. It carefully manages PathTargets to ensure that each stage produces the exact columns needed by subsequent stages.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context and accumulated state
- `tuple_fraction`: Expected fraction of result tuples to be retrieved (0 = all tuples, 0-1 = fraction, ≥1 = absolute count for LIMIT)  
- `setops`: SetOperationStmt for set operation subqueries, or NULL for regular queries

## Dependencies
- Functions called/Symbols referenced:
  - [preprocess_limit](../p/preprocess_limit.md), plan_set_operations, preprocess_grouping_sets
  - [preprocess_targetlist](../p/preprocess_targetlist.md), preprocess_aggrefs, find_window_functions
  - [query_planner](../q/query_planner.md), create_pathtarget, create_grouping_paths
  - [create_window_paths](../c/create_window_paths.md), create_distinct_paths, create_ordered_paths
  - [create_lockrows_path](../c/create_lockrows_path.md), create_limit_path, create_modifytable_path
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)

## Notes and Other Information
- Located in src/backend/optimizer/plan/planner.c:1335-2076
- This is a static function that serves as the main orchestrator for high-level query planning
- The function carefully manages parallel safety throughout the planning process
- Creates and populates the final UPPERREL_FINAL relation that contains all viable execution paths
- Handles both regular queries and set operations with different code paths
- Supports foreign data wrapper (FDW) integration through GetForeignUpperPaths callbacks
- Does not call set_cheapest() - leaves this to the caller
- The function manages complex inheritance hierarchies for DML operations on partitioned tables

## Simplified Source
```c
static void grouping_planner(PlannerInfo *root, double tuple_fraction, SetOperationStmt *setops)
{
    Query *parse = root->parse;
    int64 offset_est = 0;
    int64 count_est = 0;
    double limit_tuples = -1.0;
    PathTarget *final_target;
    RelOptInfo *current_rel;
    RelOptInfo *final_rel;

    // Handle LIMIT/OFFSET preprocessing
    if (parse->limitCount || parse->limitOffset) {
        tuple_fraction = preprocess_limit(root, tuple_fraction, &offset_est, &count_est);
        if (count_est > 0 && offset_est >= 0)
            limit_tuples = (double) count_est + (double) offset_est;
    }

    root->tuple_fraction = tuple_fraction;

    if (parse->setOperations) {
        // Handle set operations (UNION, INTERSECT, EXCEPT)
        current_rel = plan_set_operations(root);
        root->processed_tlist = postprocess_setop_tlist(copyObject(root->processed_tlist), parse->targetList);
        final_target = current_rel->cheapest_total_path->pathtarget;
        root->sort_pathkeys = make_pathkeys_for_sortclauses(root, parse->sortClause, root->processed_tlist);
    } else {
        // Regular query planning

        // Preprocess grouping operations
        if (parse->groupingSets) {
            preprocess_grouping_sets(root);
        } else if (parse->groupClause) {
            root->processed_groupClause = preprocess_groupclause(root, NIL);
            remove_useless_groupby_columns(root);
        }

        // Preprocess target list and aggregates
        preprocess_targetlist(root);
        if (parse->hasAggs) {
            preprocess_aggrefs(root, (Node *) root->processed_tlist);
            preprocess_aggrefs(root, (Node *) parse->havingQual);
        }

        // Handle window functions
        if (parse->hasWindowFuncs) {
            wflists = find_window_functions((Node *) root->processed_tlist, list_length(parse->windowClause));
            if (wflists->numWindowFuncs > 0) {
                optimize_window_clauses(root, wflists);
                activeWindows = select_active_windows(root, wflists);
            }
        }

        // Set limit for scan/join planning
        if (parse->groupClause || parse->groupingSets || parse->distinctClause ||
            parse->hasAggs || parse->hasWindowFuncs || parse->hasTargetSRFs || root->hasHavingQual)
            root->limit_tuples = -1.0;
        else
            root->limit_tuples = limit_tuples;

        // Generate scan/join paths
        current_rel = query_planner(root, standard_qp_callback, &qp_extra);

        // Create path targets for different phases
        final_target = create_pathtarget(root, root->processed_tlist);
        // ... [target creation logic simplified]

        // Apply targets to paths
        apply_scanjoin_target_to_paths(root, current_rel, scanjoin_targets,
                                       scanjoin_targets_contain_srfs,
                                       scanjoin_target_parallel_safe,
                                       scanjoin_target_same_exprs);

        // Create upper-level paths as needed
        if (have_grouping) {
            current_rel = create_grouping_paths(root, current_rel, grouping_target,
                                               grouping_target_parallel_safe, gset_data);
        }

        if (activeWindows) {
            current_rel = create_window_paths(root, current_rel, grouping_target,
                                            sort_input_target, sort_input_target_parallel_safe,
                                            wflists, activeWindows);
        }

        if (parse->distinctClause) {
            current_rel = create_distinct_paths(root, current_rel, sort_input_target);
        }
    }

    // Handle ORDER BY
    if (parse->sortClause) {
        current_rel = create_ordered_paths(root, current_rel, final_target,
                                         final_target_parallel_safe,
                                         have_postponed_srfs ? -1.0 : limit_tuples);
    }

    // Build final output paths
    final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);
    final_rel->consider_parallel = current_rel->consider_parallel &&
                                   is_parallel_safe(root, parse->limitOffset) &&
                                   is_parallel_safe(root, parse->limitCount);

    // Add final processing steps to each path
    foreach(lc, current_rel->pathlist) {
        Path *path = (Path *) lfirst(lc);

        // Add row locking if needed
        if (parse->rowMarks) {
            path = (Path *) create_lockrows_path(root, final_rel, path,
                                               root->rowMarks, assign_special_exec_param(root));
        }

        // Add LIMIT/OFFSET if needed
        if (limit_needed(parse)) {
            path = (Path *) create_limit_path(root, final_rel, path,
                                            parse->limitOffset, parse->limitCount,
                                            parse->limitOption, offset_est, count_est);
        }

        // Add ModifyTable for DML operations
        if (parse->commandType != CMD_SELECT) {
            // ... [simplified DML path creation]
            path = (Path *) create_modifytable_path(root, final_rel, path,
                                                  parse->commandType, parse->canSetTag,
                                                  parse->resultRelation, rootRelation,
                                                  root->partColsUpdated, resultRelations,
                                                  updateColnosLists, withCheckOptionLists,
                                                  returningLists, rowMarks, parse->onConflict,
                                                  mergeActionLists, mergeJoinConditions,
                                                  assign_special_exec_param(root));
        }

        add_path(final_rel, path);
    }

    // Allow FDW and extensions to add paths
    if (final_rel->fdwroutine && final_rel->fdwroutine->GetForeignUpperPaths)
        final_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_FINAL, current_rel, final_rel, &extra);

    if (create_upper_paths_hook)
        (*create_upper_paths_hook)(root, UPPERREL_FINAL, current_rel, final_rel, &extra);
}
```