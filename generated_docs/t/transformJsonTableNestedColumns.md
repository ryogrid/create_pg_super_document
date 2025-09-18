# transformJsonTableNestedColumns

## Location
src/backend/parser/parse_jsontable.c: 454 - 498

## Overview
Recursively transforms nested column definitions in JSON_TABLE and creates child execution plans for evaluating their row patterns, combining multiple nested columns using sibling joins.

## Definition
```c
static JsonTablePlan *transformJsonTableNestedColumns(JsonTableParseContext *cxt, List *passingArgs, List *columns)
```

## Detailed Description
This static function processes NESTED COLUMNS clauses within JSON_TABLE specifications by recursively transforming nested column definitions into executable plans. When multiple NESTED COLUMNS clauses exist, it creates a "sibling join" plan that effectively performs a UNION operation on the rows produced by each nested plan.

The function iterates through the provided columns list, identifies nested column types, and generates appropriate path names when not explicitly specified. It then recursively calls transformJsonTableColumns to process the nested structure and combines multiple nested plans using makeJsonTableSiblingJoin.

The sibling join semantics ensure that rows from different nested column paths are properly combined according to SQL/JSON standard requirements for JSON_TABLE operations.

## Parameters / Member Variables
- `cxt`: JsonTableParseContext containing parsing state and configuration
- `passingArgs`: List of arguments to be passed to nested JSON functions
- `columns`: List of JsonTableColumn structures that may contain nested column definitions

## Dependencies
- Functions called/Symbols referenced:
  - castNode: Safely casts nodes to specific types with type checking
  - lfirst: Gets the first element from a list cell
  - [generateJsonTablePathName](../g/generateJsonTablePathName.md): Generates unique path names for nested columns
  - [transformJsonTableColumns](transformJsonTableColumns.md): Recursively processes column definitions
  - [makeJsonTableSiblingJoin](../m/makeJsonTableSiblingJoin.md): Creates sibling join plans for combining nested results
  - JTC_NESTED: Constant identifying nested column types

- Called from (representative examples):
  - [transformJsonTableColumns](transformJsonTableColumns.md): Main column processing function that handles nested structures

## Notes and Other Information
- The function implements lazy evaluation by only processing columns of type JTC_NESTED
- Automatic path name generation ensures proper identification of nested column contexts
- The sibling join pattern allows multiple independent nested column specifications to coexist
- Recursive structure enables arbitrary depth of JSON table nesting as per SQL/JSON standards