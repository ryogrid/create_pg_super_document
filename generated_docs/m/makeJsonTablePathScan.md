# makeJsonTablePathScan

## Location
src/backend/parser/parse_jsontable.c: 499 - 533

## Overview
Creates a JsonTablePathScan plan node for scanning JSON data along a specified JSONPath with defined error handling behavior and column range specifications.

## Definition
```c
static JsonTablePathScan *makeJsonTablePathScan(JsonTablePathSpec *pathspec, bool errorOnError, int colMin, int colMax, JsonTablePlan *childplan)
```

## Detailed Description
This static function constructs a JsonTablePathScan execution plan node that represents a scan operation over JSON data using a JSONPath expression. The function converts the textual path specification into a compiled JSONPath constant and establishes the column range that this scan will compute in the global flat list of column expressions.

The function handles the compilation of JSONPath expressions by calling the jsonpath_in function to parse and validate the path string. It creates the appropriate plan node structure with error handling specifications and establishes parent-child relationships for nested scanning operations.

Column range parameters (colMin/colMax) define which columns in the global column list are computed by this specific scan, with both set to -1 when all columns are nested and computed by child plans.

## Parameters / Member Variables
- `pathspec`: JsonTablePathSpec containing the JSONPath specification and optional name
- `errorOnError`: Boolean flag indicating whether errors should be propagated or handled gracefully
- `colMin`: Minimum column index in the global column list computed by this scan (-1 if all nested)
- `colMax`: Maximum column index in the global column list computed by this scan (-1 if all nested) 
- `childplan`: Child JsonTablePlan for nested operations (NULL for leaf scans)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates new PostgreSQL parse tree nodes
  - castNode: Safely casts nodes to specific types with type checking
  - [makeConst](makeConst.md): Creates constant value nodes
  - DirectFunctionCall1: Directly calls PostgreSQL functions
  - [jsonpath_in](../j/jsonpath_in.md): Parses and compiles JSONPath expressions
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C strings to PostgreSQL Datum values
  - [makeJsonTablePath](makeJsonTablePath.md): Creates JsonTablePath structures
  - IsA: Type checking macro for node types
  - T_JsonTablePathScan: Node type identifier for path scan plans

- Called from (representative examples):
  - [transformJsonTableColumns](../t/transformJsonTableColumns.md): Main column transformation function that creates scan plans

## Notes and Other Information
- The function validates that the path specification contains a proper A_Const node
- JSONPath compilation occurs at parse time to catch syntax errors early
- Error handling behavior is configurable per scan to support different ON ERROR clauses
- Column range tracking enables efficient execution planning for complex nested structures
- The plan node supports hierarchical child plans for nested JSON table operations