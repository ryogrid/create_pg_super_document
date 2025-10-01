# show_merge_append_keys

## Location
[src/backend/commands/explain.c:2591-2606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2591-L2606)

## Overview
Displays the sort keys for a MergeAppend node during query execution plan explanation.

## Definition
```c
static void show_merge_append_keys(MergeAppendState *mstate,
                                   List *ancestors, ExplainState *es)
```

## Detailed Description
This function displays the sort keys used by a MergeAppend plan node during EXPLAIN command output. MergeAppend is an optimization used when combining results from multiple sorted child plans (like partitioned tables) while maintaining the overall sort order. The function extracts the sort key information from the MergeAppend plan and delegates to `show_sort_group_keys` to format and display the sorting criteria used to merge the child results in sorted order.

## Parameters / Member Variables
- `mstate`: Pointer to the MergeAppendState containing the runtime state and plan information for the merge append operation
- `ancestors`: List of ancestor plan nodes in the execution tree, used for context in the explanation output  
- `es`: ExplainState containing formatting options and output settings for the EXPLAIN command

## Dependencies
- Functions called/Symbols referenced:
  - [show_sort_group_keys](show_sort_group_keys.md): Core function that formats and displays sort key information
  - `[MergeAppend](../M/MergeAppend.md)`: Plan node structure containing sort configuration for merging
  - [MergeAppendState](../M/MergeAppendState.md): Runtime state structure for merge append operations
  - `[ExplainState](../E/ExplainState.md)`: State structure for EXPLAIN command formatting
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md): Main function that handles explanation of different plan node types (at line 2234)

## Notes and Other Information
- This function is part of PostgreSQL's EXPLAIN command infrastructure located in src/backend/commands/explain.c:2591-2606
- It specifically handles the T_MergeAppend case in the ExplainNode function
- Unlike IncrementalSort, MergeAppend has no presorted columns (passes 0 as nPresortedCols parameter)
- The function is used when explaining queries that involve partitioned tables or UNION ALL operations where child results are already sorted
- This is a static function, only accessible within the explain.c compilation unit
- The sort keys shown represent the merge criteria that maintain the sorted order when combining multiple sorted input streams

## Simplified Source

```c
static void
show_merge_append_keys(MergeAppendState *mstate, List *ancestors, ExplainState *es)
{
    MergeAppend *plan = (MergeAppend *) mstate->ps.plan;

    // Display sort keys used for merging sorted child results
    show_sort_group_keys((PlanState *) mstate, "Sort Key",
                         plan->numCols, 0, plan->sortColIdx,
                         plan->sortOperators, plan->collations,
                         plan->nullsFirst, ancestors, es);
}
```