# ExpandRowReference

## Location
src/backend/parser/parse_target.c: 1423 - 1518

## Overview
Transforms a star expression (.*) applied to an arbitrary expression of composite type into a list of individual field expressions or target list entries.

## Definition


## Detailed Description
ExpandRowReference handles the expansion of star expressions when applied to complex composite-type expressions that are not simple table references. Unlike ExpandSingleTable which deals with simple table references, this function handles arbitrary expressions that evaluate to composite types.

The function operates in two main modes:
1. **Optimized path**: When the expression is a whole-row Var (varattno == InvalidAttrNumber), it delegates to ExpandSingleTable for efficient processing
2. **General path**: For arbitrary composite expressions, it creates multiple FieldSelect nodes by copying the original expression and selecting individual fields

For RECORD type variables, the function uses expandRecordVariable to determine the actual tuple structure, while other composite types use get_expr_result_tupdesc directly.

The function generates FieldSelect expressions for each non-dropped attribute, properly handling type information, type modifiers, and collation settings.

## Parameters / Member Variables
- : Parse state containing context information for the current parsing operation
- : The composite-type expression to be expanded (left side of .*)
- : Boolean flag determining whether to create TargetEntry structures (true) or simple FieldSelect expressions (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ExpandSingleTable](ExpandSingleTable.md)
  - [GetNSItemByRangeTablePosn](../G/GetNSItemByRangeTablePosn.md)
  - [expandRecordVariable](../e/expandRecordVariable.md)
  - [get_expr_result_tupdesc](../g/get_expr_result_tupdesc.md)
  - makeNode
  - copyObject
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - TupleDescAttr
  - InvalidAttrNumber
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - FieldSelect
- Called from (representative examples):
  - [ExpandIndirectionStar](ExpandIndirectionStar.md)

## Notes and Other Information
- This is a static function within parse_target.c for internal target list processing
- The function includes an optimization for whole-row Vars that delegates to the more efficient ExpandSingleTable
- For complex expressions, the implementation creates multiple copies of the original expression, which can be inefficient for computationally expensive expressions
- Special handling is provided for RECORD type variables, which require runtime type resolution
- The function properly handles dropped columns by skipping them during expansion
- Permission checking is handled differently compared to ExpandSingleTable - for whole-row Vars, both table-level and column-level permissions may be marked