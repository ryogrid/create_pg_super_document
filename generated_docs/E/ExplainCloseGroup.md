# ExplainCloseGroup

## Location
src/backend/commands/explain.c: 4930 - 4976

## Overview
ExplainCloseGroup closes a group of related objects in EXPLAIN output, providing the proper closing syntax for different output formats (TEXT, XML, JSON, YAML).

## Definition


## Detailed Description
ExplainCloseGroup is responsible for properly terminating a group of related objects that were previously opened by ExplainOpenGroup. The function handles format-specific closing syntax:

- **TEXT format**: No action required (plain text doesn't need explicit grouping syntax)
- **XML format**: Outputs the appropriate XML closing tag and decreases indentation
- **JSON format**: Outputs either '}' (for labeled objects) or ']' (for unlabeled arrays), manages indentation, and removes the top item from the grouping stack
- **YAML format**: Decreases indentation and manages the grouping stack

The function ensures that the EXPLAIN output is properly structured and syntactically correct for each supported format.

## Parameters / Member Variables
- : The type of object being closed (used for XML tag names)
- : The label name for the group (must match the corresponding ExplainOpenGroup call)
- : Boolean flag indicating whether this is a labeled group (affects JSON output - '}' vs ']')
- : ExplainState structure containing formatting information, indentation level, and grouping stack

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainXMLTag](ExplainXMLTag.md) (for XML format closing tags)
  - appendStringInfoChar (for JSON format closing brackets)
  - appendStringInfoSpaces (for proper JSON indentation)
  - list_delete_first (for managing the grouping stack in JSON and YAML formats)
- Called from (representative examples):
  - [ExplainOnePlan](ExplainOnePlan.md)
  - [ExplainPrintSettings](ExplainPrintSettings.md)
  - [ExplainPrintTriggers](ExplainPrintTriggers.md)
  - ExplainPrintJIT
  - [ExplainNode](ExplainNode.md)
  - [show_grouping_sets](../s/show_grouping_sets.md)
  - [show_modifytable_info](../s/show_modifytable_info.md)

## Notes and Other Information
- Parameters must exactly match the corresponding ExplainOpenGroup call to ensure proper nesting and syntax
- The function manages indentation levels and grouping stacks to maintain proper structure across different output formats
- For JSON and YAML formats, the grouping stack is maintained to track nested structures
- Located in src/backend/commands/explain.c:4925-4957