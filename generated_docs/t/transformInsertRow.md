# transformInsertRow

## Location
src/backend/parser/analyze.c: 1008 - 1117

## Overview
Prepares a single INSERT row for assignment to the target table by transforming expressions and handling column assignments with optional indirection stripping.

## Definition
```c
List *
transformInsertRow(ParseState *pstate, List *exprlist,
                   List *stmtcols, List *icolumns, List *attrnos,
                   bool strip_indirection)
```

## Detailed Description
This function takes a list of expressions representing values for a single INSERT row and transforms them for assignment to target table columns. It performs several key operations:

1. **Length validation** - Ensures the expression list length is compatible with target columns
2. **Expression transformation** - Calls transformAssignedExpr for each value-column pair to handle type coercion, defaults, and indirection
3. **Indirection stripping** - Optionally removes field/array assignment nodes when processing multiple VALUES rows

The function handles various INSERT scenarios including direct VALUES, SELECT results, and expressions with field/array assignments. It provides detailed error messages for common mistakes like using parentheses to create RowExpr instead of separate columns.

Key design aspects:
- Validates expression count against target columns with helpful error messages  
- Supports partial column lists (remaining columns get defaults)
- Handles complex assignment expressions with indirection
- Can strip indirection nodes when building VALUES RTEs

## Parameters / Member Variables
- `pstate`: Parse state containing context and error position information
- `exprlist`: List of expressions to be assigned (from VALUES, SELECT, etc.)
- `stmtcols`: Original column specification from INSERT statement (used for error checking)
- `icolumns`: Effective target columns (list of ResTarget nodes)  
- `attrnos`: Integer attribute numbers corresponding to target columns
- `strip_indirection`: If true, removes FieldStore/SubscriptingRef nodes from expressions

## Dependencies
- Functions called/Symbols referenced:
  - [transformAssignedExpr](transformAssignedExpr.md) (main expression transformation with indirection handling)
  - [count_rowexpr_columns](../c/count_rowexpr_columns.md) (for detecting RowExpr usage in error cases)
  - [exprLocation](../e/exprLocation.md) (for error position reporting)
  - [list_nth](../l/list_nth.md) (for accessing list elements during validation)

- Called from (representative examples):
  - [transformInsertStmt](transformInsertStmt.md) (multiple times for different INSERT variants)
  - [transformMergeStmt](transformMergeStmt.md) (for MERGE statement processing)

## Notes and Other Information
- Critical for proper type coercion and default value handling in INSERT operations
- Provides enhanced error messages to help users identify common syntax mistakes
- The strip_indirection parameter is used when building VALUES RTEs where indirection must be applied later
- Handles complex assignment expressions including field updates and array element assignments
- Part of the larger INSERT statement transformation pipeline