# show_grouping_set_keys

## Location
[src/backend/commands/explain.c:2661-2738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2661-L2738)

## Overview
Displays the detailed key information for individual grouping sets within complex GROUP BY operations, handling both hashed and sorted aggregation strategies.

## Definition
```c
static void show_grouping_set_keys(PlanState *planstate,
                                   Agg *aggnode, Sort *sortnode,
                                   List *context, bool useprefix,
                                   List *ancestors, ExplainState *es)
```

## Detailed Description
This function displays the specific grouping keys for individual grouping sets within complex aggregation operations like ROLLUP, CUBE, and GROUPING SETS. It handles different aggregation strategies (hashed vs. sorted) and formats the output accordingly, using "Hash Key" for hashed strategies and "Group Key" for sorted strategies. The function iterates through each grouping set, resolving column references to actual expressions, and displays them in a structured format. It also handles associated sort operations when present and manages proper indentation for nested display structures.

## Parameters / Member Variables
- `planstate`: Pointer to the PlanState of the child plan, used for accessing the target list and column information
- `aggnode`: Pointer to the Agg plan node containing grouping set configuration and strategy information
- `sortnode`: Pointer to optional Sort plan node for sorted aggregation strategies (can be NULL)
- `context`: Deparse context list for resolving column names and expressions
- `useprefix`: Boolean flag indicating whether to use table prefixes in column names
- `ancestors`: List of ancestor plan nodes in the execution tree, used for context in expression deparsing
- `es`: ExplainState containing formatting options and output settings for the EXPLAIN command

## Dependencies
- Functions called/Symbols referenced:
  - [show_sort_group_keys](show_sort_group_keys.md): Displays sort key information when sortnode is present
  - [ExplainOpenGroup](../E/ExplainOpenGroup.md): Opens grouping sections in the EXPLAIN output structure
  - [ExplainCloseGroup](../E/ExplainCloseGroup.md): Closes grouping sections in the EXPLAIN output structure
  - [ExplainPropertyText](../E/ExplainPropertyText.md): Displays simple text properties (for empty grouping sets)
  - [ExplainPropertyListNested](../E/ExplainPropertyListNested.md): Displays nested list properties for grouping keys
  - [get_tle_by_resno](../g/get_tle_by_resno.md): Retrieves target list entries by result number
  - [deparse_expression](../d/deparse_expression.md): Converts expression nodes to readable string representation
  - `lfirst`: List traversal macro for accessing list elements
  - `lfirst_int`: List traversal macro for integer elements
  - `[lappend](../l/lappend.md)`: Appends elements to lists
  - `elog`: Error logging function for missing target entries
  - Constants: `AGG_HASHED`, `AGG_MIXED`, `EXPLAIN_FORMAT_TEXT`, `NIL`
- Called from (representative examples):
  - [show_grouping_sets](show_grouping_sets.md): Called twice per grouping set chain - once for main set (line 2645) and once for each chained aggregation (line 2653)

## Notes and Other Information
- This function is part of PostgreSQL's EXPLAIN command infrastructure located in src/backend/commands/explain.c:2661-2738
- It handles different key naming based on aggregation strategy: "Hash Key"/"Hash Keys" for hashed aggregation, "Group Key"/"Group Keys" for sorted aggregation
- Empty grouping sets (like those created by ROLLUP) are displayed as "()" in text format
- The function manages indentation levels when sort operations are present to create proper nesting in text output
- Expression deparsing includes top-level cast information for better readability
- Error handling is provided for cases where expected target list entries are missing
- This is a static function, only accessible within the explain.c compilation unit
- The function supports both text and structured output formats through the ExplainState formatting system

## Simplified Source

```c
static void
show_grouping_set_keys(PlanState *planstate,
                       Agg *aggnode, Sort *sortnode,
                       List *context, bool useprefix,
                       List *ancestors, ExplainState *es)
{
    Plan *plan = planstate->plan;
    char *exprstr;
    ListCell *lc;
    List *gsets = aggnode->groupingSets;
    AttrNumber *keycols = aggnode->grpColIdx;

    // Choose key names based on aggregation strategy
    const char *keyname, *keysetname;
    if (aggnode->aggstrategy == AGG_HASHED || aggnode->aggstrategy == AGG_MIXED)
    {
        keyname = "Hash Key";
        keysetname = "Hash Keys";
    }
    else
    {
        keyname = "Group Key";
        keysetname = "Group Keys";
    }

    ExplainOpenGroup("Grouping Set", NULL, true, es);

    // Show sort keys if present
    if (sortnode)
    {
        show_sort_group_keys(planstate, "Sort Key",
                             sortnode->numCols, 0, sortnode->sortColIdx,
                             sortnode->sortOperators, sortnode->collations,
                             sortnode->nullsFirst, ancestors, es);
        if (es->format == EXPLAIN_FORMAT_TEXT)
            es->indent++;
    }

    ExplainOpenGroup(keysetname, keysetname, false, es);

    // Process each grouping set
    foreach(lc, gsets)
    {
        List *result = NIL;
        ListCell *lc2;

        // Build expression list for this grouping set
        foreach(lc2, (List *) lfirst(lc))
        {
            Index i = lfirst_int(lc2);
            AttrNumber keyresno = keycols[i];
            TargetEntry *target = get_tle_by_resno(plan->targetlist, keyresno);

            if (!target)
                elog(ERROR, "no tlist entry for key %d", keyresno);

            // Convert expression to string with top-level cast info
            exprstr = deparse_expression((Node *) target->expr, context,
                                         useprefix, true);
            result = lappend(result, exprstr);
        }

        // Display the grouping set (empty sets show as "()")
        if (!result && es->format == EXPLAIN_FORMAT_TEXT)
            ExplainPropertyText(keyname, "()", es);
        else
            ExplainPropertyListNested(keyname, result, es);
    }

    ExplainCloseGroup(keysetname, keysetname, false, es);

    if (sortnode && es->format == EXPLAIN_FORMAT_TEXT)
        es->indent--;

    ExplainCloseGroup("Grouping Set", NULL, true, es);
}
```