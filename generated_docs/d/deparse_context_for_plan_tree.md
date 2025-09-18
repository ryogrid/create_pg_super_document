# deparse_context_for_plan_tree

## Location
src/backend/utils/adt/ruleutils.c: 3707 - 3775

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
  - palloc0 (for deparse_namespace allocation)
  - list_length (for determining table count)
  - lfirst_node (for iterating append relations)
  - set_simple_column_names
  - AppendRelInfo (structure type)
- Called from (representative examples):
  - ExplainPrintPlan (src/backend/commands/explain.c:888)

## Notes and Other Information
This function is specifically designed for EXPLAIN output generation where many expressions from the same plan tree need to be deparsed. The expensive column name setup is done once and reused, providing significant performance benefits for complex plans with large range tables. The function handles append relations by creating an array indexed by child relation ID, which is essential for proper variable resolution in partitioned tables. The context remains incomplete until set_deparse_context_plan() is called to specify the current plan node being processed. Join RTEs will produce somewhat bogus column name results, but this doesn't affect correctness since plan trees don't contain join alias Vars.