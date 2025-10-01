# deparse_context_for_plan_tree

## Location
[src/backend/utils/adt/ruleutils.c:3707-3775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3707-L3775)

## Overview
Builds a deparse context for a Plan tree, setting up the range table and append relation information for efficient expression deparsing across multiple plan nodes.

## Definition
```c
List *deparse_context_for_plan_tree(PlannedStmt *pstmt, List *rtable_names)
```

## Detailed Description
This function creates a deparse context optimized for Plan tree deparsing by using the plan's range table to resolve Vars. Since column name initialization is expensive for large range tables and needs to be the same for every expression in the Plan tree, this function performs the setup once for reuse across multiple expressions. The context includes support for append relations (used in partitioning) and subplans. The resulting context must be further configured with set_deparse_context_plan() before use.

## Parameters / Member Variables
- `pstmt`: The PlannedStmt containing the range table and append relation information
- `rtable_names`: Per-RTE alias names assigned by select_rtable_names_for_explain, providing display names for range table entries

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (for deparse_namespace allocation)
  - [list_length](../l/list_length.md) (for determining table count)
  - lfirst_node (for iterating append relations)
  - [set_simple_column_names](../s/set_simple_column_names.md)
  - [AppendRelInfo](../A/AppendRelInfo.md) (structure type)
- Called from (representative examples):
  - [ExplainPrintPlan](../E/ExplainPrintPlan.md) (src/backend/commands/explain.c:888)

## Notes and Other Information
This function is specifically designed for EXPLAIN output generation where many expressions from the same plan tree need to be deparsed. The expensive column name setup is done once and reused, providing significant performance benefits for complex plans with large range tables. The function handles append relations by creating an array indexed by child relation ID, which is essential for proper variable resolution in partitioned tables. The context remains incomplete until set_deparse_context_plan() is called to specify the current plan node being processed. Join RTEs will produce somewhat bogus column name results, but this doesn't affect correctness since plan trees don't contain join alias Vars.

## Simplified Source

```c
List *
deparse_context_for_plan_tree(PlannedStmt *pstmt, List *rtable_names)
{
    deparse_namespace *dpns;

    // Allocate and initialize the deparse namespace
    dpns = (deparse_namespace *) palloc0(sizeof(deparse_namespace));

    // Set up basic plan tree information
    dpns->rtable = pstmt->rtable;
    dpns->rtable_names = rtable_names;
    dpns->subplans = pstmt->subplans;
    dpns->ctes = NIL;

    // Handle append relations (used for partitioning)
    if (pstmt->appendRelations) {
        int ntables = list_length(dpns->rtable);
        ListCell *lc;

        // Create array indexed by child relation ID
        dpns->appendrels = (AppendRelInfo **)
            palloc0((ntables + 1) * sizeof(AppendRelInfo *));

        foreach(lc, pstmt->appendRelations) {
            AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);
            Index child_relid = appinfo->child_relid;

            Assert(child_relid > 0 && child_relid <= ntables);
            Assert(dpns->appendrels[child_relid] == NULL);
            dpns->appendrels[child_relid] = appinfo;
        }
    } else {
        dpns->appendrels = NULL;
    }

    // Set up column name aliases for simple variables
    set_simple_column_names(dpns);

    // Return single-level namespace stack
    return list_make1(dpns);
}
```