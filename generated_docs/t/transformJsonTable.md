# transformJsonTable

## Location
[src/backend/parser/parse_jsontable.c:76-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_jsontable.c#L76-L172)

## Overview
Transforms a raw JsonTable node into a TableFunc node, handling the transformation of JSON_TABLE expressions and column specifications for PostgreSQL's JSON table functionality.

## Definition

```c
ParseNamespaceItem *
transformJsonTable(ParseState *pstate, JsonTable *jt)
```
## Detailed Description
The transformJsonTable function is the main entry point for processing JSON_TABLE expressions in PostgreSQL. It converts a raw JsonTable AST node into a TableFunc node that can be executed by the PostgreSQL execution engine. The function performs several key operations:

1. Validates the ON ERROR behavior clause, ensuring only valid behaviors (ERROR, EMPTY, or EMPTY ARRAY) are specified
2. Generates path names for unnamed path specifications and checks for duplicate column/path names
3. Transforms the context item expression and path specification into a JsonFuncExpr with JSON_TABLE_OP operation
4. Creates a JsonTablePlan by transforming column specifications and their associated JSON path expressions
5. Sets up lateral reference handling and creates the appropriate range table entry

The function ensures SQL standard compliance and proper handling of lateral references within the JSON_TABLE construct.

## Parameters / Member Variables
- : ParseState context containing parsing information and namespace
- : JsonTable AST node containing the raw JSON_TABLE specification including context item, path specification, columns, and error handling clauses

## Dependencies
- Functions called/Symbols referenced:
  - [generateJsonTablePathName](../g/generateJsonTablePathName.md)
  - [CheckDuplicateColumnOrPathNames](../C/CheckDuplicateColumnOrPathNames.md)
  - makeNode
  - [transformExpr](transformExpr.md)
  - [transformJsonTableColumns](transformJsonTableColumns.md)
  - copyObject
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [addRangeTableEntryForTableFunc](../a/addRangeTableEntryForTableFunc.md)
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- The function temporarily enables lateral reference resolution (p_lateral_active = true) during transformation to comply with SQL specification requirements
- Only specific ON ERROR behaviors are allowed at the top level: ERROR, EMPTY, or EMPTY ARRAY
- The function creates a dummy JSON_TABLE_OP JsonExpr to represent the top-level context item and path specification
- PASSING arguments are duplicated in both the JsonExpr and TableFunc nodes for separate evaluation contexts
- The resulting ParseNamespaceItem is marked as lateral if explicitly specified or if lateral cross-references are detected