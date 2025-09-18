# ExplainSeparatePlans

## Location
src/backend/commands/explain.c: 5183 - 5211

## Overview
Inserts an appropriate separator between multiple plans in EXPLAIN output. This function adds format-specific separators when displaying multiple execution plans.

## Definition
void ExplainSeparatePlans(ExplainState *es)

## Detailed Description
ExplainSeparatePlans handles the insertion of separators between multiple execution plans in EXPLAIN output. The behavior varies by format: for TEXT format, it adds a blank line to visually separate plans; for XML, JSON, and YAML formats, no separator is needed as these structured formats handle plan separation through their inherent structure.

## Parameters / Member Variables
- `es`: ExplainState pointer containing the output format specification and string buffer

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (structure type)
  - EXPLAIN_FORMAT_TEXT (enum constant)
  - EXPLAIN_FORMAT_XML (enum constant)
  - EXPLAIN_FORMAT_JSON (enum constant)
  - EXPLAIN_FORMAT_YAML (enum constant)
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md)
  - [ExplainExecuteQuery](ExplainExecuteQuery.md)

## Notes and Other Information
This function is primarily useful when explaining multiple statements or when EXPLAIN ANALYZE is used with queries that have multiple execution plans. The TEXT format separator improves readability by providing visual separation between plans, while structured formats rely on their native formatting.