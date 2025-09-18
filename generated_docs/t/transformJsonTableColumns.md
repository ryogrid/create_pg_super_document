# transformJsonTableColumns

## Location
[src/backend/parser/parse_jsontable.c:251-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_jsontable.c#L251-L376)

## Overview
Creates a JsonTablePlan and transforms JSON_TABLE column specifications into their corresponding expression nodes and metadata for execution by the PostgreSQL engine.

## Definition


## Detailed Description
This function is responsible for the core transformation of JSON_TABLE column specifications into executable expressions. It processes each column in the provided list and performs several critical operations:

1. **Column Type Processing**: Handles different column types (FOR ORDINALITY, REGULAR, FORMATTED, EXISTS, NESTED) with specific logic for each
2. **Type Inference and Conversion**: Determines appropriate PostgreSQL data types for each column, with automatic promotion from REGULAR to FORMATTED for complex types
3. **Expression Generation**: Creates JsonFuncExpr nodes for data-extracting columns and transforms them into executable expressions
4. **Metadata Collection**: Builds lists of column names, types, type modifiers, and collations for the TableFunc
5. **Ordinality Validation**: Ensures only one FOR ORDINALITY column exists per JSON_TABLE
6. **Nested Column Handling**: Recursively processes nested column structures and creates appropriate scan plans

The function integrates with the broader JSON_TABLE execution framework by creating JsonTablePathScan plans that can be executed during query runtime.

## Parameters / Member Variables
- : JsonTableParseContext containing parsing state, including the current JsonTable and TableFunc being processed
- : List of JsonTableColumn nodes representing the column specifications to transform
- : List of PASSING clause arguments that provide context values for JSON path expressions
- : JsonTablePathSpec defining the path specification for this level of columns

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md) (type resolution)
  - [transformJsonTableColumn](transformJsonTableColumn.md) (individual column transformation)
  - [transformExpr](transformExpr.md) (expression transformation)
  - [assign_expr_collations](../a/assign_expr_collations.md) (collation assignment)
  - [transformJsonTableNestedColumns](transformJsonTableNestedColumns.md) (recursive nested processing)
  - [makeJsonTablePathScan](../m/makeJsonTablePathScan.md) (scan plan creation)
  - [isCompositeType](../i/isCompositeType.md) (type checking)
  - exprType, exprTypmod, exprCollation (expression metadata)
- Called from (representative examples):
  - [transformJsonTable](transformJsonTable.md) (root level processing)
  - [transformJsonTableNestedColumns](transformJsonTableNestedColumns.md) (recursive nested processing)

## Notes and Other Information
- This is a static function, only accessible within the parse_jsontable.c module
- Automatically promotes REGULAR columns to FORMATTED when dealing with composite types or non-default wrapper/quotes behavior
- FOR ORDINALITY columns are assigned INT4OID type and receive special handling during execution
- The function maintains column ranges (colMin, colMax) to organize columns by their scan level
- Nested columns (JTC_NESTED) are skipped in the main loop and processed separately through transformJsonTableNestedColumns
- Error handling includes validation for multiple FOR ORDINALITY columns and unknown column types
- The resulting JsonTablePathScan integrates with PostgreSQL's execution planning system