# show_grouping_sets

## Location
[src/backend/commands/explain.c:2630-2660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2630-L2660)

## Overview
Displays detailed grouping set information for advanced GROUP BY operations including ROLLUP, CUBE, and GROUPING SETS during query execution plan explanation.

## Definition
```c
static void show_grouping_sets(PlanState *planstate, Agg *agg,
                               List *ancestors, ExplainState *es)
```

## Detailed Description
This function displays comprehensive information about grouping sets, which are advanced GROUP BY features in SQL that allow multiple grouping levels in a single query. It handles complex aggregation scenarios like ROLLUP, CUBE, and explicit GROUPING SETS clauses. The function sets up a deparse context for proper column name resolution, opens a "Grouping Sets" section in the EXPLAIN output, displays the main grouping set keys, and then iterates through any chained aggregation nodes to show additional grouping levels. Each level may have associated sorting operations that are also displayed.

## Parameters / Member Variables
- `planstate`: Pointer to the PlanState of the child plan, used for column name resolution and context
- `agg`: Pointer to the Agg plan node containing grouping set configuration and chain information
- `ancestors`: List of ancestor plan nodes in the execution tree, used for context in the explanation output
- `es`: ExplainState containing formatting options and output settings for the EXPLAIN command

## Dependencies
- Functions called/Symbols referenced:
  - [set_deparse_context_plan](set_deparse_context_plan.md): Sets up context for column name deparsing
  - [ExplainOpenGroup](../E/ExplainOpenGroup.md): Opens a grouping section in the EXPLAIN output
  - [ExplainCloseGroup](../E/ExplainCloseGroup.md): Closes the grouping section in the EXPLAIN output  
  - [show_grouping_set_keys](show_grouping_set_keys.md): Displays the actual grouping keys for each level
  - `lfirst`: List traversal macro for iterating through the aggregation chain
  - `list_length`: Utility function to determine if table prefixes are needed
  - [PlanState](../P/PlanState.md): Runtime state structure for plan execution
  - `Agg`: Plan node structure containing aggregation and grouping set configuration
  - `Sort`: Plan node structure for sorting operations in chained aggregations
  - `ExplainState`: State structure for EXPLAIN command formatting
- Called from (representative examples):
  - [show_agg_keys](show_agg_keys.md): When the aggregation node has grouping sets defined (at line 2618)

## Notes and Other Information
- This function is part of PostgreSQL's EXPLAIN command infrastructure located in src/backend/commands/explain.c:2630-2660
- It is called from `show_agg_keys` when `plan->groupingSets` is not NULL
- The function handles complex multi-level aggregation chains where each level may have different grouping criteria
- The deparse context setup ensures proper column name resolution across different relation levels
- Table prefixes are used when there are multiple tables in the query or when verbose mode is enabled
- The function displays both the main grouping set and any chained aggregation levels with their associated sort operations
- Grouping sets are a PostgreSQL extension to standard SQL GROUP BY functionality
- This is a static function, only accessible within the explain.c compilation unit