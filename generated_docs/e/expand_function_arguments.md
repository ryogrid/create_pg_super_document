# expand_function_arguments

## Location
[src/backend/optimizer/util/clauses.c:4175-4255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4175-L4255)

## Overview
Converts named-notation function arguments to positional notation and inserts default argument values as needed during function call processing.

## Definition


## Detailed Description
This function processes function argument lists to handle two main scenarios:

1. **Named argument conversion**: When arguments are provided using named notation (e.g., func(param2 => value)), they are reordered to match the function's parameter positions
2. **Default argument insertion**: When fewer arguments are provided than the function expects, missing arguments are filled in with their default values

The function can operate in two modes based on the include_out_arguments parameter:
- When true, it considers OUT parameters in addition to IN parameters using the proallargtypes array
- When false, it only considers IN parameters using the proargtypes array

The function preserves the original argument list when no changes are needed and creates a copy only when modifications are required.

## Parameters / Member Variables
- : Input list of function arguments to process
- : Whether to include OUT arguments in processing 
- : Expected result type of the function call for sanity checking
- : The function's pg_proc tuple containing metadata

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - DatumGetArrayTypeP
  - [reorder_function_arguments](../r/reorder_function_arguments.md)
  - [add_function_defaults](../a/add_function_defaults.md)
  - [recheck_cast_function_args](../r/recheck_cast_function_args.md)
  - NamedArgExpr
- Called from (representative examples):
  - [simplify_function](../s/simplify_function.md)
  - [eval_const_expressions_mutator](eval_const_expressions_mutator.md)
  - [transformCallStmt](../t/transformCallStmt.md)

## Notes and Other Information
- The function handles both function calls and operator calls (though named arguments should never occur for operators)
- It performs expensive proallargtypes array access only when include_out_arguments is true
- Input argument lists are never modified in-place; copies are created when changes are needed
- The function validates array structure for proallargtypes to ensure it's a proper 1-D OID array
- Argument type checking and casting is performed after argument reordering or default insertion