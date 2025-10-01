# ExplainNode

## Location
[src/backend/commands/explain.c:1367-2428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L1367-L2428)

## Overview
The main function that generates detailed explanation output for a single plan node in PostgreSQL's EXPLAIN command.

## Definition
```c
static void ExplainNode(PlanState *planstate, List *ancestors, const char *relationship, const char *plan_name, ExplainState *es)
```

## Detailed Description
The `ExplainNode` function is the core function responsible for generating detailed textual or structured output for individual plan nodes in PostgreSQL's EXPLAIN functionality. It handles all major plan node types including scans, joins, aggregates, sorts, and many others. The function performs several key operations:

1. **Node Type Identification**: Uses a large switch statement to identify the specific plan node type and set appropriate display names for both text and structured output formats.

2. **Instrumentation Data Processing**: Extracts and formats execution statistics when ANALYZE option is used, including timing information, row counts, and loop counts.

3. **Worker State Management**: Handles parallel query execution details, including per-worker statistics when available.

4. **Format-specific Output**: Generates different output formats (text, JSON, XML, YAML) based on the ExplainState configuration.

5. **Node-specific Details**: Calls specialized functions to display details specific to each node type, such as index conditions, join conditions, sort keys, etc.

The function is highly recursive through its interaction with other explain functions and handles the complete tree traversal for plan explanation.

## Parameters / Member Variables
- `planstate`: PlanState node containing both plan structure and execution instrumentation data
- `ancestors`: List of parent Plan and SubPlan nodes for parameter interpretation context
- `relationship`: String describing relationship to parent node (e.g., "Outer", "Inner"), can be NULL at top level
- `plan_name`: Optional name to attach to the node, typically for subplans
- `es`: ExplainState structure containing output format, verbosity options, and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - [ExplainCreateWorkersState](ExplainCreateWorkersState.md)
  - [ExplainOpenGroup](ExplainOpenGroup.md)/ExplainCloseGroup
  - [ExplainPropertyText](ExplainPropertyText.md)/ExplainPropertyBool/ExplainPropertyFloat/ExplainPropertyInteger
  - [ExplainIndentText](ExplainIndentText.md)
  - [InstrEndLoop](../I/InstrEndLoop.md)
  - [show_plan_tlist](../s/show_plan_tlist.md)
  - [show_scan_qual](../s/show_scan_qual.md)
  - [show_upper_qual](../s/show_upper_qual.md)
  - Various node-specific show functions
- Called from (representative examples):
  - [ExplainPrintPlan](ExplainPrintPlan.md)
  - [ExplainSubPlans](ExplainSubPlans.md)

## Notes and Other Information
- Function exceeds 1000 lines due to comprehensive handling of all PostgreSQL plan node types
- Supports both text and structured output formats with different formatting logic
- Handles instrumentation cleanup through InstrEndLoop calls
- Manages indentation for text format output to create readable nested structure
- Includes detailed per-worker execution statistics for parallel queries
- Handles cost estimation display when costs option is enabled
- Contains extensive node-type-specific logic for displaying relevant execution details
- Critical function for PostgreSQL query analysis and performance debugging

## Simplified Source

```c
static void
ExplainNode(PlanState *planstate, List *ancestors,
            const char *relationship, const char *plan_name,
            ExplainState *es)
{
    Plan *plan = planstate->plan;
    const char *pname, *sname;
    const char *strategy = NULL, *partialmode = NULL, *operation = NULL;
    ExplainWorkersState *save_workers_state = es->workers_state;
    bool haschildren;

    // Setup per-worker output buffers if needed
    if (planstate->worker_instrument && es->analyze && !es->hide_workers)
        es->workers_state = ExplainCreateWorkersState(planstate->worker_instrument->num_workers);
    else
        es->workers_state = NULL;

    // Identify plan node type and set display names
    switch (nodeTag(plan)) {
        case T_SeqScan:
            pname = sname = "Seq Scan";
            break;
        case T_IndexScan:
            pname = sname = "Index Scan";
            break;
        case T_NestLoop:
            pname = sname = "Nested Loop";
            break;
        case T_HashJoin:
            pname = "Hash";
            sname = "Hash Join";
            break;
        case T_Agg:
            {
                Agg *agg = (Agg *) plan;
                sname = "Aggregate";
                switch (agg->aggstrategy) {
                    case AGG_PLAIN:
                        pname = "Aggregate";
                        strategy = "Plain";
                        break;
                    case AGG_HASHED:
                        pname = "HashAggregate";
                        strategy = "Hashed";
                        break;
                    // ... other strategies
                }
                // Handle partial/finalize modes
                if (DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
                    partialmode = "Partial";
                else if (DO_AGGSPLIT_COMBINE(agg->aggsplit))
                    partialmode = "Finalize";
            }
            break;
        // ... other node types
        default:
            pname = sname = "???";
            break;
    }

    ExplainOpenGroup("Plan", relationship ? NULL : "Plan", true, es);

    // Format output based on format type
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        // Text format: compact, human-readable
        if (plan->parallel_aware)
            appendStringInfoString(es->str, "Parallel ");
        appendStringInfoString(es->str, pname);
    } else {
        // Structured format: use properties
        ExplainPropertyText("Node Type", sname, es);
        if (strategy)
            ExplainPropertyText("Strategy", strategy, es);
        if (relationship)
            ExplainPropertyText("Parent Relationship", relationship, es);
        ExplainPropertyBool("Parallel Aware", plan->parallel_aware, es);
    }

    // Show node-specific details (scan targets, join conditions, etc.)
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_IndexScan:
            ExplainScanTarget((Scan *) plan, es);
            break;
        case T_NestLoop:
        case T_HashJoin:
            // Show join type and conditions
            show_join_details(plan, planstate, ancestors, es);
            break;
        // ... other node types
    }

    // Show cost estimates if enabled
    if (es->costs) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfo(es->str, "  (cost=%.2f..%.2f rows=%.0f width=%d)",
                           plan->startup_cost, plan->total_cost,
                           plan->plan_rows, plan->plan_width);
        } else {
            ExplainPropertyFloat("Startup Cost", NULL, plan->startup_cost, 2, es);
            ExplainPropertyFloat("Total Cost", NULL, plan->total_cost, 2, es);
        }
    }

    // Clean up instrumentation and show actual execution stats
    if (planstate->instrument)
        InstrEndLoop(planstate->instrument);

    if (es->analyze && planstate->instrument && planstate->instrument->nloops > 0) {
        double nloops = planstate->instrument->nloops;
        double startup_ms = 1000.0 * planstate->instrument->startup / nloops;
        double total_ms = 1000.0 * planstate->instrument->total / nloops;
        double rows = planstate->instrument->ntuples / nloops;

        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (es->timing)
                appendStringInfo(es->str, " (actual time=%.3f..%.3f rows=%.0f loops=%.0f)",
                               startup_ms, total_ms, rows, nloops);
        } else {
            if (es->timing) {
                ExplainPropertyFloat("Actual Startup Time", "ms", startup_ms, 3, es);
                ExplainPropertyFloat("Actual Total Time", "ms", total_ms, 3, es);
            }
            ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
        }
    }

    if (es->format == EXPLAIN_FORMAT_TEXT)
        appendStringInfoChar(es->str, '\n');

    // Show per-worker details if available
    if (es->workers_state && es->verbose)
        show_worker_details(planstate, es);

    // Show verbose target list if requested
    if (es->verbose)
        show_plan_tlist(planstate, ancestors, es);

    // Show qualifiers, sort keys, and other node-specific info
    show_node_quals_and_keys(plan, planstate, ancestors, es);

    // Show buffer and WAL usage
    if (es->buffers && planstate->instrument)
        show_buffer_usage(es, &planstate->instrument->bufusage);

    // Handle child plans
    haschildren = planstate->initPlan || outerPlanState(planstate) ||
                  innerPlanState(planstate) || IsA(plan, Append) ||
                  IsA(plan, SubqueryScan) || planstate->subPlan;

    if (haschildren) {
        ExplainOpenGroup("Plans", "Plans", false, es);
        ancestors = lcons(plan, ancestors);

        // Show init plans, left/right children, special children, subplans
        if (planstate->initPlan)
            ExplainSubPlans(planstate->initPlan, ancestors, "InitPlan", es);

        if (outerPlanState(planstate))
            ExplainNode(outerPlanState(planstate), ancestors, "Outer", NULL, es);

        if (innerPlanState(planstate))
            ExplainNode(innerPlanState(planstate), ancestors, "Inner", NULL, es);

        // Handle special child plans for Append, BitmapAnd, etc.
        show_special_children(plan, planstate, ancestors, es);

        if (planstate->subPlan)
            ExplainSubPlans(planstate->subPlan, ancestors, "SubPlan", es);

        ancestors = list_delete_first(ancestors);
        ExplainCloseGroup("Plans", "Plans", false, es);
    }

    // Cleanup
    if (es->workers_state)
        ExplainFlushWorkersState(es);
    es->workers_state = save_workers_state;

    ExplainCloseGroup("Plan", relationship ? NULL : "Plan", true, es);
}
```