# coerce_to_target_type

## Location
src/backend/parser/parse_coerce.c: 78 - 156

## Overview
Converts an expression to a target type and typmod, serving as the general-purpose entry point for arbitrary type coercion operations in PostgreSQL's parser.

## Definition


## Detailed Description
This function provides a comprehensive type coercion mechanism that attempts to convert an input expression from its current type to a desired target type and typmod. Unlike direct calls to component coercion functions, this function handles the complete coercion pipeline including:

1. **Feasibility Check**: First verifies if the coercion is possible using 
2. **CollateExpr Handling**: Intelligently manages CollateExpr nodes by stripping them before coercion and reinstalling them afterward if the target type is collatable
3. **Type Coercion**: Performs the actual type conversion using 
4. **Typmod Coercion**: Applies additional length/precision coercion using  for fixed-length types

The function returns NULL rather than throwing errors directly, allowing callers to generate custom error messages with appropriate context information.

## Parameters / Member Variables
- : Parse state context (can be NULL, see coerce_type)
- : Input expression tree (already transformed by transformExpr)
- : Current result type of the input expression
- : Desired result type for the coercion
- : Desired result typmod for the coercion
- : Coercion context indicating the circumstances of the coercion
- : Coercion format controlling how the coercion is displayed
- : Parse location of the coercion request, or -1 if unknown/implicit

## Dependencies
- Functions called/Symbols referenced:
  - can_coerce_type
  - coerce_type
  - coerce_type_typmod
  - type_is_collatable
  - CollateExpr (node type)
  - CoercionContext (enum)
  - CoercionForm (enum)
- Called from (representative examples):
  - transformTypeCast
  - transformAssignedExpr
  - coerce_to_boolean
  - build_coercion_expression
  - ATExecAlterColumnType

## Notes and Other Information
- This is the recommended entry point for type coercion operations; direct use of component functions should be limited to special cases
- The function carefully manages CollateExpr nodes to preserve collation information through the coercion process
- For fixed-length types requiring both type and length coercion, the inner coercion node is forced to implicit display form
- Returns NULL on coercion failure rather than reporting errors, enabling context-specific error handling by callers
- Located in src/backend/parser/parse_coerce.c:78-156