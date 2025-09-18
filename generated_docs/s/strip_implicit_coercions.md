# strip_implicit_coercions

## Location
src/backend/nodes/nodeFuncs.c: 700 - 757

## Overview
Removes implicit coercions at the top level of an expression tree without modifying or copying the input, returning a pointer to the appropriate sub-expression.

## Definition


## Detailed Description
This function recursively traverses down expression trees to remove implicit type coercions that were inserted by PostgreSQL's type system. It handles various types of coercion nodes including function calls, relabel operations, I/O-based coercions, array coercions, row type conversions, and domain coercions. The function only removes coercions marked with  format, leaving explicit coercions intact. It returns a pointer to a location within the original tree rather than creating copies, making it efficient for cases where the original structure needs to be preserved.

## Parameters / Member Variables
- : The root node of the expression tree from which to strip implicit coercions. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - linitial (macro for accessing first list element)
  - FuncExpr (function call expression node)
  - RelabelType (type relabeling node)
  - CoerceViaIO (I/O-based coercion node)
  - ArrayCoerceExpr (array coercion expression)
  - ConvertRowtypeExpr (row type conversion expression)
  - CoerceToDomain (domain coercion node)
  - COERCE_IMPLICIT_CAST (coercion format constant)

- Called from (representative examples):
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md) (table alteration)
  - [findTargetlistEntrySQL99](../f/findTargetlistEntrySQL99.md) (SQL99 target list parsing)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (query rewriting)
  - [get_update_query_targetlist_def](../g/get_update_query_targetlist_def.md) (rule utilities)
  - get_rule_expr (rule expression formatting)

## Notes and Other Information
- The function does not modify the input expression tree, making it safe to use in contexts where the original structure must be preserved
- RowExpr nodes are returned unchanged even if marked as implicit coercions, as there's no meaningful way to strip them
- The function is recursive, continuing to strip nested implicit coercions until it reaches a non-coercion node
- This is commonly used in query planning and rewriting phases where the actual underlying expressions are needed without the coercion wrapper nodes