# ExplainJSONLineEnding

## Location
src/backend/commands/explain.c: 5254 - 5273

## Overview
A utility function that properly formats JSON line endings in EXPLAIN output, ensuring correct comma placement between JSON properties.

## Definition
static void ExplainJSONLineEnding(ExplainState *es)

## Detailed Description
This function handles the specific formatting requirements for JSON output in PostgreSQL's EXPLAIN command. JSON format requires commas after each property except the last one. To facilitate proper comma placement, this function manages line endings and comma insertion based on the grouping stack state. The function checks if this is the first property in a group using the grouping stack - if it's not the first property (indicated by a non-zero value), it adds a comma before the newline. Otherwise, it marks that subsequent properties will need commas by setting the stack value to 1.

## Parameters / Member Variables
- es: ExplainState pointer containing the output buffer and formatting state information, including the grouping stack for tracking JSON structure nesting

## Dependencies
- Functions called/Symbols referenced:
  - linitial_int (list manipulation function)
  - appendStringInfoChar (string buffer utility)
  - ExplainState (struct type)
  - EXPLAIN_FORMAT_JSON (format constant)
- Called from (representative examples):
  - [ExplainPropertyList](ExplainPropertyList.md)
  - [ExplainPropertyListNested](ExplainPropertyListNested.md)
  - [ExplainProperty](ExplainProperty.md)
  - [ExplainOpenGroup](ExplainOpenGroup.md)
  - [ExplainDummyGroup](ExplainDummyGroup.md)

## Notes and Other Information
- This is a static function only accessible within the explain.c file
- The function includes an assertion to ensure it's only called when format is JSON
- The grouping_stack is used to track nesting levels and determine comma placement
- Part of PostgreSQL's EXPLAIN output formatting system for structured data formats