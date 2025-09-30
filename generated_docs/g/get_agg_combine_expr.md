# get_agg_combine_expr

## Location
[src/backend/utils/adt/ruleutils.c:10699-10714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10699-L10714)

## Overview
A specialized callback function used in PostgreSQL's parallel query execution to deparse combining aggregate expressions by locating and formatting the corresponding partial aggregate.

## Definition

```c
static void
get_agg_combine_expr(Node *node, deparse_context *context, void *callback_arg)
```
## Detailed Description
This function serves as a callback helper specifically for deparsing combining aggregates in PostgreSQL's parallel query execution system. When parallel workers execute aggregate operations, they produce partial results that must be combined in the final step. This function is called by resolve_special_varno when processing a combining aggregate to locate the corresponding partial aggregate expression.

The function validates that the node is indeed an Aggref (aggregate reference), then delegates the actual deparsing work to get_agg_expr, passing along the original aggregate reference for context. This ensures that the partial aggregate is displayed correctly in the context of the combining operation.

## Parameters / Member Variables
- : Pointer to the Node that should contain the partial Aggref to be deparsed
- : Deparse context containing the output buffer and formatting preferences
- : Void pointer that contains the original Aggref for context (cast from Aggref*)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro to check node type)
  - elog (error logging function)
  - [get_agg_expr](get_agg_expr.md) (main aggregate expression deparsing function)
  - [Aggref](../A/Aggref.md) (aggregate reference node structure)
  - ERROR (error level constant)
- Called from:
  - [get_agg_expr_helper](get_agg_expr_helper.md) (via resolve_special_varno callback mechanism)

## Notes and Other Information
- This function is specifically designed for PostgreSQL's parallel query execution infrastructure
- Part of the rule deparsing system used for displaying views, rules, and constraints
- The callback design allows resolve_special_varno to handle complex variable resolution scenarios
- Includes error checking to ensure type safety when processing aggregate nodes
- The original_aggref parameter provides necessary context for proper formatting of the combining aggregate
- This function bridges the gap between the special variable resolution system and aggregate expression deparsing

## Simplified Source

```c
static void
get_agg_combine_expr(Node *node, deparse_context *context, void *callback_arg)
{
    Aggref *aggref;
    Aggref *original_aggref = callback_arg;

    // Validate that the node is actually an Aggref
    if (!IsA(node, Aggref))
        elog(ERROR, "combining Aggref does not point to an Aggref");

    aggref = (Aggref *) node;

    // Deparse the partial aggregate using the main aggregate expression function
    get_agg_expr(aggref, context, original_aggref);
}
```