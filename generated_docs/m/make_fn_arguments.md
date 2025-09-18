# make_fn_arguments

## Location
src/backend/parser/parse_func.c: 1825 - 1880

## Overview
Adds necessary type casting to function argument expressions when actual argument types don't match the declared function parameter types.

## Definition


## Detailed Description
The  function is responsible for ensuring type compatibility between actual function arguments and the declared parameter types of a function. When the actual argument types don't match the declared types, this function adds appropriate type coercion nodes to the expression tree using implicit casting. The function modifies the argument list in-place, making it ready for function execution.

The function handles both regular expressions and NamedArgExpr nodes specially - when encountering a NamedArgExpr, it coerces the inner expression while preserving the NamedArgExpr wrapper at the top level of the argument list.

## Parameters / Member Variables
- : Parse state context for the current parsing operation (can be NULL if no special unknown-Param processing is needed)
- : List of actual argument expressions passed to the function (modified in-place)
- : Array of OIDs representing the actual types of the arguments
- : Array of OIDs representing the expected parameter types of the function

## Dependencies
- Functions called/Symbols referenced:
  - coerce_type
  - NamedArgExpr (struct)
  - COERCION_IMPLICIT (constant)
  - COERCE_IMPLICIT_CAST (constant)
- Called from (representative examples):
  - ParseFuncOrColumn
  - make_op
  - make_scalar_array_op
  - recheck_cast_function_args

## Notes and Other Information
- The function assumes that type casting compatibility has already been verified by the caller
- The argument list is modified in-place, so the original list structure is altered
- Special handling for NamedArgExpr ensures that named arguments maintain their structure while having their inner expressions coerced
- Uses implicit coercion with COERCE_IMPLICIT_CAST format, indicating automatic type casting rather than explicit user-requested casting