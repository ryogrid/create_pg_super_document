# ExplainCustomChildren

## Location
[src/backend/commands/explain.c:4459-4480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4459-L4480)

## Overview
ExplainCustomChildren is a static function in PostgreSQL's explain module that handles the explanation of child plans belonging to a CustomScan node in the query execution plan.

## Definition

```c
static void
ExplainCustomChildren(CustomScanState *css, List *ancestors, ExplainState *es)
```
## Detailed Description
This function iterates through the list of child PlanState nodes associated with a CustomScanState and explains each one. It automatically determines the appropriate label ('child' for single children, 'children' for multiple) based on the number of child plans present. The function provides a standardized way to explain the child nodes of custom scan implementations.

## Parameters / Member Variables
- : CustomScanState containing the list of child plan states to be explained
- : List of ancestor nodes in the execution tree for context
- : ExplainState structure containing formatting options and state information

## Dependencies
- Functions called/Symbols referenced:
  - list_length (determines number of child plans)
  - [ExplainNode](ExplainNode.md) (recursively explains each child plan)
- Called from:
  - [ExplainNode](ExplainNode.md) (when processing CustomScan nodes)

## Notes and Other Information
- Uses intelligent labeling: 'child' for single child, 'children' for multiple children
- Iterates through the custom_ps list in the CustomScanState structure
- Passes NULL for the plan name parameter to ExplainNode since custom children typically don't have specific names
- File location: src/backend/commands/explain.c:4459-4480