# show_group_keys

## Location
[src/backend/commands/explain.c:2739-2758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2739-L2758)

## Overview
A static function that displays the grouping keys for a Group node in PostgreSQL's query execution plan output during EXPLAIN operations.

## Definition


## Detailed Description
The  function is responsible for displaying the grouping keys used by a Group node in PostgreSQL's query execution plan. This function is part of the EXPLAIN infrastructure and helps users understand which columns are being used for grouping operations. It works by delegating to the more general  function, passing the appropriate parameters specific to grouping operations.

The function operates on the child plan's target list (tlist) to determine and display the grouping columns. It temporarily adds the current Group plan to the ancestors list to maintain proper context during the explanation process.

## Parameters / Member Variables
- : Pointer to the GroupState execution state containing information about the Group node being explained
- : List of ancestor plan nodes used to maintain context during plan traversal
- : Pointer to ExplainState containing formatting and output options for the EXPLAIN operation

## Dependencies
- Functions called/Symbols referenced:
  - [lcons](../l/lcons.md) (adds plan to ancestors list)
  - [show_sort_group_keys](show_sort_group_keys.md) (displays the actual grouping keys)
  - outerPlanState (gets the child plan state)
  - list_delete_first (removes plan from ancestors list)
- Types referenced:
  - [GroupState](../G/GroupState.md) (execution state for Group nodes)
  - ExplainState (state for EXPLAIN operations)
  - Group (Group plan node type)
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md) (main function for explaining plan nodes)

## Notes and Other Information
- This is a static function within explain.c, indicating it's only used internally within the EXPLAIN subsystem
- The function uses "Group Key" as the label when displaying the grouping columns
- The grouping keys refer to columns in the target list of the child plan, not the Group plan itself
- The ancestors list manipulation ensures proper context is maintained during recursive plan explanation
- The function passes NULL values for several parameters to show_sort_group_keys, indicating that Group nodes don't use sorting-specific features like sort operators or collations