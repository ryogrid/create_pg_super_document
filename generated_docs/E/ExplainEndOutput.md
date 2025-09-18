# ExplainEndOutput

## Location
src/backend/commands/explain.c: 5154 - 5182

## Overview
Emits the end-of-output boilerplate for different EXPLAIN output formats. This function closes the output structure that was opened by ExplainBeginOutput.

## Definition
void ExplainEndOutput(ExplainState *es)

## Detailed Description
ExplainEndOutput provides the complementary closing functionality to ExplainBeginOutput. It generates the appropriate closing markup or structure cleanup for different EXPLAIN output formats. For TEXT format, no action is needed. For XML format, it decreases indentation and closes the root XML element. For JSON format, it decreases indentation, closes the array structure, and cleans up the grouping stack. For YAML format, it only cleans up the grouping stack without additional markup.

## Parameters / Member Variables
- `es`: ExplainState pointer containing the output format specification, string buffer, and formatting state

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (structure type)
  - EXPLAIN_FORMAT_TEXT (enum constant)
  - EXPLAIN_FORMAT_XML (enum constant)
  - EXPLAIN_FORMAT_JSON (enum constant)
  - EXPLAIN_FORMAT_YAML (enum constant)
  - list_delete_first (list manipulation function)
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md)

## Notes and Other Information
This function must be called after ExplainBeginOutput to ensure proper formatting of the output. The grouping_stack cleanup is essential for JSON and YAML formats to maintain proper nesting structure. The function handles indentation management for XML and JSON formats to ensure properly formatted output.