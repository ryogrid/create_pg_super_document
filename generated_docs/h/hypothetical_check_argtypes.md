# hypothetical_check_argtypes

## Location
src/backend/utils/adt/orderedsetaggs.c: 1142 - 1170

## Overview
A security validation function that verifies argument type consistency for hypothetical-set aggregate functions in PostgreSQL.

## Definition
```c
static void hypothetical_check_argtypes(FunctionCallInfo fcinfo, int nargs, TupleDesc tupdesc)
```

## Detailed Description
This internal function performs critical security checks for hypothetical-set aggregate functions like `rank()`, `dense_rank()`, etc. It ensures that the direct arguments passed to the hypothetical-set function match the types of the corresponding aggregated columns in the dataset.

The function validates two key aspects: first, it confirms that the tuple descriptor includes the expected int4 flag column used by the hypothetical-set framework; second, it verifies that each direct argument type matches the corresponding aggregated column type. These checks prevent type confusion attacks and ensure the integrity of the aggregate operation.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing argument details
- `nargs`: Number of direct arguments to the hypothetical-set function
- `tupdesc`: Tuple descriptor describing the structure of aggregated data

## Dependencies
- Functions called/Symbols referenced:
  - `[FunctionCallInfo](../F/FunctionCallInfo.md)`: PostgreSQL function call information structure
  - `[get_fn_expr_argtype](../g/get_fn_expr_argtype.md)`: Retrieves the type of a function argument from call info
  - `TupleDescAttr`: Macro to access tuple descriptor attributes
  - `INT4OID`: Object identifier for int4 data type
  - `elog`: PostgreSQL error logging function
- Called from (representative examples):
  - `[hypothetical_rank_common](hypothetical_rank_common.md)`: Common implementation for rank-based hypothetical-set functions
  - `[hypothetical_dense_rank_final](hypothetical_dense_rank_final.md)`: Final function for dense_rank() hypothetical-set aggregate

## Notes and Other Information
- This is a security-critical function that prevents malicious manipulation of aggregate function arguments
- The checks are intentionally strict and will throw errors rather than attempt graceful recovery
- The function expects that hypothetical-set functions always include an int4 flag column as the last column
- Error messages are deliberately terse since these errors should only occur due to system catalog corruption or malicious activity
- The validation ensures that direct arguments can be safely compared with aggregated values of the same type
- This function is part of PostgreSQL's defense against type confusion vulnerabilities in aggregate functions