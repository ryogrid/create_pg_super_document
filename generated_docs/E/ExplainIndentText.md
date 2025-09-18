# ExplainIndentText

## Location
src/backend/commands/explain.c: 5239 - 5253

## Overview
Indents a text-format line in EXPLAIN output. This function adds appropriate spacing for hierarchical display of execution plan information in TEXT format.

## Definition
static void ExplainIndentText(ExplainState *es)

## Detailed Description
ExplainIndentText handles indentation for TEXT format EXPLAIN output by adding two spaces per indentation level. The function intelligently checks if the current line is empty or ends with a newline before adding indentation, preventing unnecessary indentation when data already exists on the current line (such as when displaying parallel worker information). This ensures proper formatting of the hierarchical execution plan display.

## Parameters / Member Variables
- `es`: ExplainState pointer containing the output string buffer and current indentation level

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (structure type)
  - EXPLAIN_FORMAT_TEXT (enum constant)
  - appendStringInfoSpaces (string formatting function)
- Called from (representative examples):
  - [ExplainOnePlan](ExplainOnePlan.md)
  - ExplainPrintJIT
  - [ExplainNode](ExplainNode.md)
  - [show_sort_info](../s/show_sort_info.md)
  - [show_hash_info](../s/show_hash_info.md)
  - [show_memoize_info](../s/show_memoize_info.md)
  - [show_buffer_usage](../s/show_buffer_usage.md)
  - [ExplainProperty](ExplainProperty.md)

## Notes and Other Information
This is a static function internal to explain.c and only applies to TEXT format output. The function uses an assertion to ensure it's only called for TEXT format. The indentation logic accounts for parallel worker output where content may already exist on the current line, maintaining proper formatting in all scenarios. Each indentation level corresponds to two spaces in the output.