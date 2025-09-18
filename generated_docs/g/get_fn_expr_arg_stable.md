# get_fn_expr_arg_stable

## Location
src/backend/utils/fmgr/fmgr.c: 1975 - 1993

## Overview
Determines whether a specific function argument is constant for the duration of a query by examining the FmgrInfo structure's expression node.

## Definition


## Detailed Description
This function analyzes whether a particular argument to a function call remains constant throughout query execution. It serves as a wrapper around get_call_expr_arg_stable, providing a convenient interface when working with FmgrInfo structures that contain expression trees. The function is essential for optimization decisions, allowing the query planner and executor to determine if expensive computations can be cached or if certain optimizations are safe to apply.

The function checks for the availability of both FmgrInfo and its associated fn_expr before delegating to the underlying expression analysis function.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing function metadata and expression tree
- : Zero-based index of the argument to check for stability

## Dependencies
- Functions called/Symbols referenced:
  - get_call_expr_arg_stable
- Called from (representative examples):
  - pg_input_is_valid_common
  - leadlag_common
  - window_nth_value
  - extract_variadic_args

## Notes and Other Information
- Returns false if FmgrInfo is NULL or if fn_expr has not been initialized
- This function is commonly used in window functions and validation routines where argument stability affects optimization strategies
- The stability check is crucial for determining whether argument values can be pre-computed or cached during query execution
- Part of PostgreSQL's function manager infrastructure that supports runtime optimization based on argument characteristics