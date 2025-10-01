# show_plan_tlist

## Location
[src/backend/commands/explain.c:2429-2486](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2429-L2486)

## Overview
Displays the target list (output columns) of a plan node in PostgreSQL's EXPLAIN output when verbose mode is enabled.

## Definition
```c
static void show_plan_tlist(PlanState *planstate, List *ancestors, ExplainState *es)
```

## Detailed Description
The `show_plan_tlist` function is responsible for displaying the target list (output columns) of a plan node as part of PostgreSQL's EXPLAIN VERBOSE output. The target list represents the expressions that the plan node will output, including both regular result columns and internal "junk" columns used for system purposes.

The function includes logic to suppress target list display for certain node types where the output would not be helpful or could be confusing:
- Append, MergeAppend, and RecursiveUnion nodes (their target lists aren't particularly meaningful)
- ForeignScan nodes that execute direct INSERT/UPDATE/DELETE operations (to avoid showing confusing subplan expressions and junk columns)

For displayable target lists, the function deparses each target entry expression into a readable string format using the query's deparse context, then outputs the results as a list under the "Output" property.

## Parameters / Member Variables
- `planstate`: PlanState structure containing the plan node with its target list
- `ancestors`: List of parent Plan nodes used for expression deparsing context
- `es`: ExplainState structure containing output format settings and deparse context

## Dependencies
- Functions called/Symbols referenced:
  - [set_deparse_context_plan](set_deparse_context_plan.md)
  - [deparse_expression](../d/deparse_expression.md)
  - [ExplainPropertyList](../E/ExplainPropertyList.md)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- Only called when EXPLAIN VERBOSE is used, controlled by the es->verbose flag in ExplainNode
- Returns early for empty target lists (common in bitmap index scans)
- Uses table prefixes in expressions when multiple relations are involved
- Includes both regular and "resjunk" target entries in the output
- The useprefix flag determines whether to include table prefixes based on the number of tables in the range table
- Essential for understanding what expressions a plan node computes and outputs
- Helps in query debugging and optimization by showing intermediate results

## Simplified Source

```c
static void
show_plan_tlist(PlanState *planstate, List *ancestors, ExplainState *es)
{
    Plan *plan = planstate->plan;
    List *result = NIL;
    bool useprefix;

    // Skip if no target list (bitmap indexscans have empty target lists)
    if (plan->targetlist == NIL)
        return;

    // Skip for certain node types where target lists aren't helpful
    if (IsA(plan, Append) || IsA(plan, MergeAppend) || IsA(plan, RecursiveUnion))
        return;

    // Skip ForeignScan nodes doing direct INSERT/UPDATE/DELETE
    if (IsA(plan, ForeignScan) && ((ForeignScan *) plan)->operation != CMD_SELECT)
        return;

    // Set up context for expression deparsing
    List *context = set_deparse_context_plan(es->deparse_cxt, plan, ancestors);
    useprefix = list_length(es->rtable) > 1;

    // Deparse each target entry into readable text
    foreach(lc, plan->targetlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(lc);
        result = lappend(result, deparse_expression((Node *) tle->expr, context, useprefix, false));
    }

    // Display the output list
    ExplainPropertyList("Output", result, es);
}
```