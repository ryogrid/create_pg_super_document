# ExplainPrintPlan

## Location
[src/backend/commands/explain.c:877-941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L877-L941)

## Overview
ExplainPrintPlan converts a QueryDesc's plan tree into textual representation and handles the core logic of generating EXPLAIN output for the execution plan structure.

## Definition

```c
void
ExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc)
```
## Detailed Description
ExplainPrintPlan is responsible for the main task of converting an execution plan tree into human-readable or structured output. It performs several critical setup operations before generating the plan output:

1. **Plan Tree Setup**: Initializes ExplainState fields specific to the current plan tree, including the planned statement, range table, and deparse context for SQL reconstruction
2. **Relation Analysis**: Pre-scans the plan tree to determine which relations are actually used and creates appropriate names for display
3. **Special Gather Handling**: Implements special logic for "invisible" Gather nodes used in regression testing to ensure consistent output between parallel and non-parallel execution modes
4. **Plan Tree Traversal**: Recursively processes the entire plan tree starting from the top-level plan state
5. **Configuration Display**: Optionally includes modified GUC settings that affect query planning
6. **Query Identifier**: Shows the query identifier when verbose mode is enabled (except in regression testing mode)

The function coordinates the overall EXPLAIN output generation process, delegating specific formatting tasks to specialized functions while managing the global state needed for proper plan visualization.

## Parameters / Member Variables
- `*es`: ExplainState containing output formatting options, buffers, and state information for the current explain operation
- `*queryDesc`: QueryDesc containing the planned statement, execution state, and other query metadata needed for plan explanation
## Dependencies
- Functions called/Symbols referenced:
  - [ExplainPreScanNode](ExplainPreScanNode.md)
  - [select_rtable_names_for_explain](../s/select_rtable_names_for_explain.md)
  - [deparse_context_for_plan_tree](../d/deparse_context_for_plan_tree.md)
  - [ExplainNode](ExplainNode.md)
  - [ExplainPrintSettings](ExplainPrintSettings.md)
  - [ExplainPropertyInteger](ExplainPropertyInteger.md)
  - outerPlanState
- Called from (representative examples):
  - [ExplainOnePlan](ExplainOnePlan.md)

## Notes and Other Information
- The function will not work correctly on utility statements (only works with planned queries)
- Special handling exists for "invisible" Gather nodes to support regression testing with different debug_parallel_query settings
- The function assumes that ExplainState's basic fields (options, output buffer, formatting state) are already properly initialized
- [Query](../Q/Query.md) identifiers are displayed as signed 64-bit integers to match pg_stat_statements output format
- [Plan](../P/Plan.md)-tree-specific fields in ExplainState are initialized by this function and used by subsequent explain operations
- The deparse context created here enables proper SQL fragment reconstruction throughout the explanation process

## Simplified Source

```c
void ExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc)
{
    Bitmapset  *rels_used = NULL;
    PlanState  *ps;

    // Initialize ExplainState for this plan tree
    Assert(queryDesc->plannedstmt != NULL);
    es->pstmt = queryDesc->plannedstmt;
    es->rtable = queryDesc->plannedstmt->rtable;

    // Pre-scan to determine which relations are used
    ExplainPreScanNode(queryDesc->planstate, &rels_used);
    es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
    es->deparse_cxt = deparse_context_for_plan_tree(queryDesc->plannedstmt,
                                                   es->rtable_names);
    es->printed_subplans = NULL;

    // Handle special "invisible" Gather nodes for regression testing
    ps = queryDesc->planstate;
    if (IsA(ps, GatherState) && ((Gather *) ps->plan)->invisible) {
        ps = outerPlanState(ps);
        es->hide_workers = true;
    }

    // Process the entire plan tree
    ExplainNode(ps, NIL, NULL, NULL, es);

    // Show modified GUC settings if requested
    ExplainPrintSettings(es);

    // Show query identifier in verbose mode (except regression testing)
    if (es->verbose && queryDesc->plannedstmt->queryId != UINT64CONST(0) &&
        compute_query_id != COMPUTE_QUERY_ID_REGRESS) {
        ExplainPropertyInteger("Query Identifier", NULL,
                              (int64) queryDesc->plannedstmt->queryId, es);
    }
}
```