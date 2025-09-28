# standard_planner

## Location
[src/backend/optimizer/plan/planner.c:288-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L288-L628)

## Overview
The standard_planner function is PostgreSQL's core query planning implementation that converts a parsed Query structure into an optimized executable PlannedStmt, handling parallelism assessment, cost optimization, and plan finalization.

## Definition
```c
PlannedStmt *standard_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
```

## Detailed Description
The standard_planner function implements PostgreSQL's complete query planning pipeline. It begins by setting up global planner state (PlannerGlobal) and assessing parallel execution feasibility based on query characteristics, system configuration, and safety constraints. The function determines the optimal tuple fraction for cursor operations, delegates to subquery_planner for the core planning work, selects the best execution path, and converts it to a Plan tree.

Key responsibilities include:
- Initializing global planner state and configuration
- Evaluating parallel execution opportunities and constraints  
- Handling cursor-specific optimizations (fast-start vs. complete results)
- Managing scrollable cursor requirements with materialization
- Optionally adding debug Gather nodes for testing
- Finalizing parameter handling and plan references
- Configuring JIT compilation flags based on cost thresholds
- Building the final PlannedStmt result structure

The function implements sophisticated parallel planning logic, considering factors like backend mode, command type, parallel hazard levels, and debug settings.

## Parameters / Member Variables
- `parse`: Pointer to the Query structure containing the parsed and analyzed SQL statement
- `query_string`: Original SQL query string for debugging and logging purposes
- `cursorOptions`: Bitmask controlling cursor behavior (CURSOR_OPT_FAST_PLAN, CURSOR_OPT_SCROLL, CURSOR_OPT_PARALLEL_OK)
- `boundParams`: ParamListInfo containing bound parameter values for parameterized queries

## Dependencies
- Functions called/Symbols referenced:
  - [subquery_planner](subquery_planner.md) (core recursive planning function)
  - [fetch_upper_rel](../f/fetch_upper_rel.md) (retrieve final relation)
  - [get_cheapest_fractional_path](../g/get_cheapest_fractional_path.md) (path selection)
  - [create_plan](../c/create_plan.md) (path to plan conversion)
  - [ExecSupportsBackwardScan](../E/ExecSupportsBackwardScan.md) (scrollability check)
  - [materialize_finished_plan](../m/materialize_finished_plan.md) (materialization for scrolling)
  - [SS_finalize_plan](../S/SS_finalize_plan.md) (parameter finalization)
  - [set_plan_references](set_plan_references.md) (reference resolution)
  - [max_parallel_hazard](../m/max_parallel_hazard.md) (parallel safety assessment)
- Called from (representative examples):
  - [planner](../p/planner.md) (main entry point)
  - [delay_execution_planner](../d/delay_execution_planner.md) (test module)

## Notes and Other Information
- Modifies the input Query structure, requiring copying for multiple planning attempts
- Implements comprehensive parallel execution assessment with multiple safety checks
- Handles special cases for CREATE TABLE AS, SELECT INTO, and CREATE MATERIALIZED VIEW in parallel mode
- Supports debug modes for parallel query testing (debug_parallel_query settings)
- Configures JIT compilation based on cost thresholds and enabled features
- Manages complex cursor optimization strategies based on expected result set usage
- Located in src/backend/optimizer/plan/planner.c:288-628

## Simplified Source

```c
// Simplified version of standard_planner
PlannedStmt *standard_planner(Query *parse, const char *query_string, int cursorOptions,
                              ParamListInfo boundParams) {
    PlannedStmt *result;
    PlannerGlobal *glob;
    double tuple_fraction;
    PlannerInfo *root;
    RelOptInfo *final_rel;
    Path *best_path;
    Plan *top_plan;

    // Set up global planner state
    glob = makeNode(PlannerGlobal);
    glob->boundParams = boundParams;
    // Initialize other global state fields...

    // Assess parallel execution feasibility
    if ((cursorOptions & CURSOR_OPT_PARALLEL_OK) != 0 &&
        IsUnderPostmaster &&
        parse->commandType == CMD_SELECT &&
        !parse->hasModifyingCTE &&
        max_parallel_workers_per_gather > 0 &&
        !IsParallelWorker()) {
        glob->maxParallelHazard = max_parallel_hazard(parse);
        glob->parallelModeOK = (glob->maxParallelHazard != PROPARALLEL_UNSAFE);
    } else {
        glob->maxParallelHazard = PROPARALLEL_UNSAFE;
        glob->parallelModeOK = false;
    }

    // Enable parallel mode for debug if safe
    glob->parallelModeNeeded = glob->parallelModeOK &&
                               (debug_parallel_query != DEBUG_PARALLEL_OFF);

    // Determine tuple fraction for optimization
    if (cursorOptions & CURSOR_OPT_FAST_PLAN) {
        tuple_fraction = cursor_tuple_fraction;
        if (tuple_fraction >= 1.0)
            tuple_fraction = 0.0;
        else if (tuple_fraction <= 0.0)
            tuple_fraction = 1e-10;
    } else {
        tuple_fraction = 0.0;  // need all tuples
    }

    // Core planning: convert query to optimized plan
    root = subquery_planner(glob, parse, NULL, false, tuple_fraction, NULL);

    // Select best execution path
    final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);
    best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);

    // Convert path to executable plan
    top_plan = create_plan(root, best_path);

    // Add materialization for scrollable cursors if needed
    if (cursorOptions & CURSOR_OPT_SCROLL) {
        if (!ExecSupportsBackwardScan(top_plan))
            top_plan = materialize_finished_plan(top_plan);
    }

    // Add debug Gather node if testing parallel execution
    if (debug_parallel_query != DEBUG_PARALLEL_OFF &&
        top_plan->parallel_safe &&
        (top_plan->initPlan == NIL ||
         debug_parallel_query != DEBUG_PARALLEL_REGRESS)) {
        Gather *gather = makeNode(Gather);
        // Configure gather node for testing...
        top_plan = &gather->plan;
    }

    // Finalize parameter handling
    if (glob->paramExecTypes != NIL) {
        SS_finalize_plan(root, top_plan);
        // Finalize subplans...
    }

    // Set final plan references
    top_plan = set_plan_references(root, top_plan);

    // Build result PlannedStmt
    result = makeNode(PlannedStmt);
    result->commandType = parse->commandType;
    result->queryId = parse->queryId;
    result->hasReturning = (parse->returningList != NIL);
    result->hasModifyingCTE = parse->hasModifyingCTE;
    result->canSetTag = parse->canSetTag;
    result->transientPlan = glob->transientPlan;
    result->dependsOnRole = glob->dependsOnRole;
    result->parallelModeNeeded = glob->parallelModeNeeded;
    result->planTree = top_plan;
    result->rtable = glob->finalrtable;
    result->permInfos = glob->finalrteperminfos;
    result->resultRelations = glob->resultRelations;
    result->appendRelations = glob->appendRelations;
    result->subplans = glob->subplans;
    result->rewindPlanIDs = glob->rewindPlanIDs;
    result->rowMarks = glob->finalrowmarks;
    result->relationOids = glob->relationOids;
    result->invalItems = glob->invalItems;
    result->paramExecTypes = glob->paramExecTypes;
    result->utilityStmt = parse->utilityStmt;
    result->stmt_location = parse->stmt_location;
    result->stmt_len = parse->stmt_len;

    // Configure JIT compilation based on cost
    result->jitFlags = PGJIT_NONE;
    if (jit_enabled && jit_above_cost >= 0 &&
        top_plan->total_cost > jit_above_cost) {
        result->jitFlags |= PGJIT_PERFORM;

        if (jit_optimize_above_cost >= 0 &&
            top_plan->total_cost > jit_optimize_above_cost)
            result->jitFlags |= PGJIT_OPT3;
        if (jit_inline_above_cost >= 0 &&
            top_plan->total_cost > jit_inline_above_cost)
            result->jitFlags |= PGJIT_INLINE;

        if (jit_expressions)
            result->jitFlags |= PGJIT_EXPR;
        if (jit_tuple_deforming)
            result->jitFlags |= PGJIT_DEFORM;
    }

    // Cleanup and return
    if (glob->partition_directory != NULL)
        DestroyPartitionDirectory(glob->partition_directory);

    return result;
}
```

Key simplifications made:
- Preserved the essential planning pipeline: setup, assess parallelism, plan, finalize
- Maintained core parallel execution logic and safety checks
- Kept cursor optimization and scrollable cursor handling
- Simplified complex initialization and cleanup operations
- Focused on the main query planning workflow
- Retained JIT configuration logic
- Abstracted detailed subplan and parameter handling