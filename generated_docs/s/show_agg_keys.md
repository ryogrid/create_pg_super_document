# show_agg_keys

## Location
src/backend/commands/explain.c: 2607 - 2629

## Overview
Displays the grouping keys for an Agg (aggregation) node during query execution plan explanation.

## Definition
```c
static void show_agg_keys(AggState *astate, List *ancestors,
                          ExplainState *es)
```

## Detailed Description
This function displays the grouping keys used by an Agg plan node during EXPLAIN command output. Aggregation nodes perform grouping operations like GROUP BY clauses in SQL queries. The function handles two types of grouping: standard grouping (using `show_sort_group_keys`) and advanced grouping sets (using `show_grouping_sets`). It only displays grouping information when there are actual grouping columns (`numCols > 0`) or grouping sets defined. The function temporarily modifies the ancestors list to include the current plan for proper context when displaying nested grouping information.

## Parameters / Member Variables
- `astate`: Pointer to the AggState containing the runtime state and plan information for the aggregation operation
- `ancestors`: List of ancestor plan nodes in the execution tree, used for context in the explanation output
- `es`: ExplainState containing formatting options and output settings for the EXPLAIN command

## Dependencies
- Functions called/Symbols referenced:
  - `[show_grouping_sets](show_grouping_sets.md)`: Displays advanced grouping set information when grouping sets are used
  - `[show_sort_group_keys](show_sort_group_keys.md)`: Displays standard grouping key information with "Group Key" label
  - `outerPlanState`: Accesses the child plan state for key column references
  - `[lcons](../l/lcons.md)`: Adds current plan to ancestors list for context
  - `list_delete_first`: Removes the added plan from ancestors list after processing
  - `Agg`: Plan node structure containing aggregation configuration
  - `[AggState](../A/AggState.md)`: Runtime state structure for aggregation operations
  - `ExplainState`: State structure for EXPLAIN command formatting
- Called from (representative examples):
  - `[ExplainNode](../E/ExplainNode.md)`: Main function that handles explanation of different plan node types (at line 2201)

## Notes and Other Information
- This function is part of PostgreSQL's EXPLAIN command infrastructure located in src/backend/commands/explain.c:2607-2629
- It specifically handles the T_Agg case in the ExplainNode function
- The function only displays output when there are actual grouping columns (`plan->numCols > 0`) or grouping sets (`plan->groupingSets`)
- For grouping sets (advanced GROUP BY features like ROLLUP, CUBE), it calls `show_grouping_sets` instead of the standard key display
- For standard grouping, it passes NULL for sort operators, collations, and null handling since these don't apply to grouping keys
- The ancestors list manipulation ensures proper column name resolution by referencing the child plan's target list
- This is a static function, only accessible within the explain.c compilation unit