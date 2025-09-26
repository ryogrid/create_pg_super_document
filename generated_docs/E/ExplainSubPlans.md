# ExplainSubPlans

## Location
[src/backend/commands/explain.c:4416-4458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4416-L4458)

## Overview
ExplainSubPlans is a static function in PostgreSQL's explain module that handles the explanation of a list of SubPlans (including initPlans) within a query execution plan, ensuring each subplan is printed only once even when referenced multiple times.

## Definition

```c
static void
ExplainSubPlans(List *plans, List *ancestors,
				const char *relationship, ExplainState *es)
```
## Detailed Description
This function iterates through a list of SubPlanState nodes and explains each unique subplan exactly once. It maintains a global tracking mechanism using a bitmapset () to prevent duplicate explanations of the same physical subplan, which can occur when multiple SubPlan nodes reference the same underlying plan (identified by ).

The function is designed to handle cases where the same physical subplan might be referenced from different locations in the execution tree, such as both in a bitmap index scan's index qualification and its parent heap scan's recheck qualification.

## Parameters / Member Variables
- : List of SubPlanState nodes to be explained
- : List of ancestor nodes in the execution tree (should already contain the immediate parent)
- : String describing the relationship between the subplans and their parent node
- : ExplainState structure containing formatting options and tracking information

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (checks if subplan already printed)
  - [bms_add_member](../b/bms_add_member.md) (marks subplan as printed)
  - [lcons](../l/lcons.md) (adds SubPlan to ancestors list)
  - [ExplainNode](ExplainNode.md) (recursively explains the subplan node)
  - [list_delete_first](../l/list_delete_first.md) (removes SubPlan from ancestors list)
- Called from:
  - [ExplainNode](ExplainNode.md) (main plan explanation function)

## Notes and Other Information
- Uses a bitmapset to track printed subplans globally across the entire plan tree
- Temporarily adds each SubPlan node to the ancestors list while explaining it, allowing ruleutils.c to find referents of subplan parameters
- The deduplication logic is crucial for preventing redundant output in complex plans with shared subplans
- File location: src/backend/commands/explain.c:4416-4458