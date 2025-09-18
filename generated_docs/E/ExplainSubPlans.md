# ExplainSubPlans

## Location
src/backend/commands/explain.c: 4416 - 4458

## Overview
ExplainSubPlans is a static function in PostgreSQL's explain module that handles the explanation of a list of SubPlans (including initPlans) within a query execution plan, ensuring each subplan is printed only once even when referenced multiple times.

## Definition


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
  - bms_is_member (checks if subplan already printed)
  - bms_add_member (marks subplan as printed)
  - lcons (adds SubPlan to ancestors list)
  - ExplainNode (recursively explains the subplan node)
  - list_delete_first (removes SubPlan from ancestors list)
- Called from:
  - ExplainNode (main plan explanation function)

## Notes and Other Information
- Uses a bitmapset to track printed subplans globally across the entire plan tree
- Temporarily adds each SubPlan node to the ancestors list while explaining it, allowing ruleutils.c to find referents of subplan parameters
- The deduplication logic is crucial for preventing redundant output in complex plans with shared subplans
- File location: src/backend/commands/explain.c:4416-4458