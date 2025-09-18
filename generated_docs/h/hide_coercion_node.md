# hide_coercion_node

## Location
src/backend/parser/parse_coerce.c: 811 - 838

## Overview
This function marks a coercion node as IMPLICIT to control its display behavior, ensuring it won't be shown by ruleutils.c when generating SQL output.

## Definition


## Detailed Description
The hide_coercion_node function is used to modify the display format of various coercion nodes by setting their format field to COERCE_IMPLICIT_CAST. This is particularly useful when PostgreSQL generates nested coercion nodes to implement what is logically a single conversion operation. By marking inner nodes as implicit, they become invisible in query plan output and rule display, simplifying the visual representation without changing the actual semantics of the operations.

The function handles multiple node types that contain CoercionForm fields and sets the appropriate format field for each type. If an unsupported node type is passed, it raises an error.

## Parameters / Member Variables
- : A pointer to the Node structure to be marked as implicit. Must be one of the supported coercion node types with a CoercionForm field.

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - nodeTag (for error reporting)
  - elog (for error logging)
  - COERCE_IMPLICIT_CAST (constant)
- [Node](../N/Node.md) types handled:
  - FuncExpr
  - RelabelType
  - CoerceViaIO
  - ArrayCoerceExpr
  - ConvertRowtypeExpr
  - RowExpr
  - CoerceToDomain
- Called from:
  - [coerce_to_domain](../c/coerce_to_domain.md)
  - [coerce_type_typmod](../c/coerce_type_typmod.md)

## Notes and Other Information
- This is a static function, only accessible within parse_coerce.c
- The function only changes display behavior, not the semantic meaning of coercion operations
- Used internally when building complex coercion chains where intermediate steps should be hidden from user view
- Calling this function on a node without a CoercionForm field will result in a runtime error