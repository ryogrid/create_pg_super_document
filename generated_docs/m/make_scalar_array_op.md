# make_scalar_array_op

## Location
src/backend/parser/parse_oper.c: 770 - 936

## Overview
The  function builds expression trees for "scalar op ANY/ALL (array)" constructs in PostgreSQL's parser, handling type resolution and validation for array operations.

## Definition


## Detailed Description
This function constructs ScalarArrayOpExpr nodes for SQL constructs like "value = ANY(array)" or "value <> ALL(array)". It performs comprehensive type checking to ensure the right-hand side is an array type and extracts the element type for operator resolution. The function validates that the operator returns a boolean result and doesn't return a set, as required for array operations.

The function handles polymorphic operators carefully, ensuring type consistency between the scalar value, array elements, and operator requirements. It also manages type coercion as needed and constructs the final expression node with appropriate operator and function identifiers.

## Parameters / Member Variables
- : ParseState for context and error reporting
- : List containing the operator name components
- : Boolean flag indicating ANY (true) vs ALL (false) semantics
- : Left operand expression node (the scalar value)
- : Right operand expression node (the array)
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - exprType
  - get_base_element_type
  - oper
  - op_signature_string
  - enforce_generic_type_consistency
  - get_func_retset
  - IsPolymorphicType
  - get_array_type
  - make_fn_arguments
  - makeNode
  - oprid
  - ReleaseSysCache
- Called from (representative examples):
  - transformAExprOpAny
  - transformAExprOpAll
  - transformAExprIn

## Notes and Other Information
- Returns a ScalarArrayOpExpr node representing the scalar-array operation
- Requires the right-hand side to be an array type, raising an error otherwise
- Validates that the operator returns boolean and doesn't return a set
- Handles polymorphic operators by adjusting array types as needed
- The useOr parameter determines ANY vs ALL semantics for the operation
- The inputcollid field is set later by parse_collate.c
- Uses UNKNOWNOID handling for untyped literals on the right-hand side
- Performs automatic type coercion through make_fn_arguments when necessary
- The hashfuncid and negfuncid fields are initialized to InvalidOid and may be set later for optimization