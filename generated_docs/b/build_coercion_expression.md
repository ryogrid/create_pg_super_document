# build_coercion_expression

## Location
src/backend/parser/parse_coerce.c: 839 - 1011

## Overview
This function constructs an expression tree for applying a pg_cast entry, supporting both type coercion and length coercion operations.

## Definition


## Detailed Description
The build_coercion_expression function is a central component of PostgreSQL's type coercion system. It builds appropriate expression nodes based on the specified coercion path type, creating different node structures depending on how the coercion should be performed.

The function handles three main coercion path types:
1. COERCION_PATH_FUNC: Creates a FuncExpr node that calls a specific coercion function
2. COERCION_PATH_ARRAYCOERCE: Creates an ArrayCoerceExpr node for array element-wise coercion
3. COERCION_PATH_COERCEVIAIO: Creates a CoerceViaIO node for text-based coercion

For function-based coercion, it validates the coercion function and constructs appropriate arguments including optional typmod and explicit coercion parameters. For array coercion, it recursively coerces individual elements using a CaseTestExpr placeholder.

## Parameters / Member Variables
- : The input expression node to be coerced
- : The type of coercion path to use (FUNC, ARRAYCOERCE, or COERCEVIAIO)
- : OID of the coercion function (valid only for COERCION_PATH_FUNC)
- : OID of the target data type
- : Type modifier for the target type
- : Coercion context indicating whether coercion is implicit, assignment, or explicit
- : Coercion format controlling display behavior
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (system catalog access)
  - [makeConst](../m/makeConst.md), makeFuncExpr, makeNode (node construction)
  - exprType, exprTypmod (expression type utilities)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md), get_element_type (type utilities)
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (recursive coercion)
  - list_make1, lappend (list operations)
- Called from:
  - [coerce_type](../c/coerce_type.md)
  - [coerce_type_typmod](../c/coerce_type_typmod.md)

## Notes and Other Information
- This is a static function, only accessible within parse_coerce.c
- Validates coercion functions to ensure they have proper signatures (1-3 arguments)
- For array coercion, uses CaseTestExpr as a placeholder for individual array elements
- The function assumes that domain types will be handled by coerce_to_domain in a higher-level call
- Supports passing typmod and explicit coercion flags to coercion functions when needed
- Error handling includes cache lookup failures and unsupported path types