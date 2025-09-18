# ExplainResultDesc

## Location
src/backend/commands/explain.c: 389 - 427

## Overview
ExplainResultDesc constructs the result tuple descriptor for EXPLAIN command output, determining the appropriate column type based on the specified format option.

## Definition


## Detailed Description
ExplainResultDesc creates a tuple descriptor that defines the structure of the result set returned by an EXPLAIN command. The function examines the EXPLAIN statement's options to determine the output format and sets the appropriate data type for the single result column. It supports three main formats: TEXT (TEXTOID), XML (XMLOID), and JSON (JSONOID). YAML format is treated as TEXT since PostgreSQL doesn't have a native YAML type.

The function iterates through all format options in the statement (not breaking after the first one) to use the last specified format value, which matches the behavior in ExplainQuery. It then creates a single-column tuple descriptor with the column name 'QUERY PLAN' and the determined data type.

## Parameters / Member Variables
- : ExplainStmt containing the EXPLAIN statement with options that determine the output format

## Dependencies
- Functions called/Symbols referenced:
  - ExplainStmt (struct type)
  - [DefElem](../D/DefElem.md) (struct type)
  - [defGetString](../d/defGetString.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md)
  - UtilityTupleDescriptor

## Notes and Other Information
- Always creates a single-column result with column name 'QUERY PLAN'
- Supports three data types: TEXTOID (default), XMLOID, and JSONOID
- Uses the last format option found, consistent with ExplainQuery's behavior
- YAML format is treated as TEXT type since PostgreSQL lacks native YAML support
- The tuple descriptor has typmod -1 and attndims 0 for the single column