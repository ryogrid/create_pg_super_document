# get_call_expr_argtype

## Location
[src/backend/utils/fmgr/fmgr.c:1929-1974](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1929-L1974)

## Overview
Retrieves the actual type OID of a specific function argument from a calling expression tree, enabling type checking and polymorphic function resolution without requiring FmgrInfo.

## Definition


## Detailed Description
This function extracts the actual data type OID of a function argument at a specified position by analyzing the calling expression tree directly. Unlike similar functions that work with FmgrInfo structures, this function operates on the expression tree itself, making it useful during query planning and optimization phases when the actual function call structure is being analyzed.

The function supports various types of expression nodes including function calls, operators, distinct expressions, scalar array operations, null-if expressions, and window functions. It includes special handling for ScalarArrayOpExpr where the second argument (argnum == 1) requires extracting the element type from an array type.

## Parameters / Member Variables
- : The expression node representing a function call or operator expression to analyze
- : Zero-based index of the argument whose type should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - exprType
  - [list_nth](../l/list_nth.md)
  - list_length
  - [get_base_element_type](get_base_element_type.md)
  - IsA (macro)
- Expression node types referenced:
  - FuncExpr
  - OpExpr
  - DistinctExpr
  - ScalarArrayOpExpr
  - NullIfExpr
  - WindowFunc
- Called from (representative examples):
  - [prepare_sql_fn_parse_info](../p/prepare_sql_fn_parse_info.md)
  - [get_fn_expr_argtype](get_fn_expr_argtype.md)
  - [resolve_polymorphic_tupdesc](../r/resolve_polymorphic_tupdesc.md)
  - [resolve_polymorphic_argtypes](../r/resolve_polymorphic_argtypes.md)

## Notes and Other Information
- Returns InvalidOid if the expression is NULL, unsupported type, or argnum is out of bounds
- Special case handling for ScalarArrayOpExpr where the second argument gets the array's element type rather than the array type itself
- This function is crucial for polymorphic function resolution where argument types must be determined from the calling context
- Primarily used during query planning and type resolution phases of query processing