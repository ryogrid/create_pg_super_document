# get_fn_expr_arg_stable

## Location
[src/backend/utils/fmgr/fmgr.c:1975-1993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1975-L1993)

## Overview
Determines whether a specific function argument is constant for the duration of a query by examining the FmgrInfo structure's expression node.

## Definition

```c
bool
get_fn_expr_arg_stable(FmgrInfo *flinfo, int argnum)
```
## Detailed Description
This function analyzes whether a particular argument to a function call remains constant throughout query execution. It serves as a wrapper around get_call_expr_arg_stable, providing a convenient interface when working with FmgrInfo structures that contain expression trees. The function is essential for optimization decisions, allowing the query planner and executor to determine if expensive computations can be cached or if certain optimizations are safe to apply.

The function checks for the availability of both FmgrInfo and its associated fn_expr before delegating to the underlying expression analysis function.

## Parameters / Member Variables
- `*flinfo`: Pointer to FmgrInfo structure containing function metadata and expression tree
- `argnum`: Zero-based index of the argument to check for stability
## Dependencies
- Functions called/Symbols referenced:
  - [get_call_expr_arg_stable](get_call_expr_arg_stable.md)
- Called from (representative examples):
  - [pg_input_is_valid_common](../p/pg_input_is_valid_common.md)
  - [leadlag_common](../l/leadlag_common.md)
  - [window_nth_value](../w/window_nth_value.md)
  - [extract_variadic_args](../e/extract_variadic_args.md)

## Notes and Other Information
- Returns false if FmgrInfo is NULL or if fn_expr has not been initialized
- This function is commonly used in window functions and validation routines where argument stability affects optimization strategies
- The stability check is crucial for determining whether argument values can be pre-computed or cached during query execution
- Part of PostgreSQL's function manager infrastructure that supports runtime optimization based on argument characteristics

## Simplified Source

```c
bool get_fn_expr_arg_stable(FmgrInfo *flinfo, int argnum) {
    // Check if function info and expression are available
    if (!flinfo || !flinfo->fn_expr)
        return false;

    // Delegate to expression-specific stability check
    return get_call_expr_arg_stable(flinfo->fn_expr, argnum);
}
```