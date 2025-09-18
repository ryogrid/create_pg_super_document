# make_op

## Location
src/backend/parser/parse_oper.c: 660 - 769

## Overview
The  function constructs operator expressions in PostgreSQL's parser, handling type compatibility, operator resolution, and building the final expression tree.

## Definition


## Detailed Description
This function is the primary entry point for constructing operator expressions during parsing. It handles both unary (prefix) and binary operators by analyzing the provided operand nodes and resolving the appropriate operator from the system catalogs. The function performs comprehensive type checking, ensures operator compatibility, handles type coercion when necessary, and constructs the final OpExpr node.

The function distinguishes between prefix operators (when ltree is NULL) and binary operators, calling the appropriate resolution functions (left_oper or oper). It validates that the operator is not a shell operator and performs polymorphic type resolution to ensure type consistency. The function also handles set-returning function validation and placement checking.

## Parameters / Member Variables
- : ParseState for context and error reporting
- : List containing the operator name components
- : Left operand expression node (NULL for prefix operators)
- : Right operand expression node (required)
- : Copy of pstate->p_last_srf for nested set-returning function detection
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - exprType
  - [left_oper](../l/left_oper.md)
  - [oper](../o/oper.md)
  - [op_signature_string](../o/op_signature_string.md)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md)
  - [make_fn_arguments](make_fn_arguments.md)
  - makeNode
  - [oprid](../o/oprid.md)
  - [get_func_retset](../g/get_func_retset.md)
  - [check_srf_call_placement](../c/check_srf_call_placement.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [transformAExprOp](../t/transformAExprOp.md)
  - transformAExprNullIf
  - transformAExprIn
  - [make_row_comparison_op](make_row_comparison_op.md)
  - [make_distinct_op](make_distinct_op.md)

## Notes and Other Information
- Returns an Expr node (specifically an OpExpr) representing the operator expression
- Does not support postfix operators - will raise an error if rtree is NULL
- Validates that operators are not shell operators (incomplete operator definitions)
- Handles polymorphic type resolution for generic operators
- Performs automatic type coercion when necessary through make_fn_arguments
- Tracks set-returning functions for proper placement validation
- The opcollid and inputcollid fields are set later by parse_collate.c
- Must release the syscache entry for the operator tuple when done