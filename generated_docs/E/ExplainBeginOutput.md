# ExplainBeginOutput

## Location
src/backend/commands/explain.c: 5123 - 5153

## Overview
Emits the start-of-output boilerplate for different EXPLAIN output formats. This function initializes the output structure based on the specified format (TEXT, XML, JSON, or YAML).

## Definition
void ExplainBeginOutput(ExplainState *es)

## Detailed Description
ExplainBeginOutput is responsible for generating the appropriate opening markup or structure for different EXPLAIN output formats. The function inspects the format field in the ExplainState structure and generates format-specific initialization code. For TEXT format, no action is needed. For XML format, it outputs the root XML element with namespace declaration and increases indentation. For JSON format, it starts an array structure and manages the grouping stack. For YAML format, it initializes the grouping stack without additional markup.

## Parameters / Member Variables
- `es`: ExplainState pointer containing the output format specification, string buffer, and formatting state

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (structure type)
  - EXPLAIN_FORMAT_TEXT (enum constant)
  - EXPLAIN_FORMAT_XML (enum constant) 
  - EXPLAIN_FORMAT_JSON (enum constant)
  - EXPLAIN_FORMAT_YAML (enum constant)
  - [lcons_int](../l/lcons_int.md) (list construction function)
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md)

## Notes and Other Information
This function is paired with ExplainEndOutput to provide proper opening and closing structures for each output format. The grouping_stack management is particularly important for JSON and YAML formats to track nested structures during output generation.