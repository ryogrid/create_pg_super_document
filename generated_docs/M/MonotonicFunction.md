# MonotonicFunction

## Location
src/include/nodes/plannodes.h: 1591 - 1593

## Overview
A bitflag enumeration that allows the PostgreSQL planner to track monotonic properties of functions, particularly window functions, for query optimization purposes.

## Definition


## Detailed Description
MonotonicFunction is used to characterize the monotonic behavior of functions, enabling the planner to make optimization decisions based on function properties. A monotonically increasing function is one where subsequent calls cannot yield lower values than previous calls, while a monotonically decreasing function cannot yield higher values on subsequent calls. Functions that are both monotonically increasing and decreasing must return the same value on each call.

This information is primarily used for window function optimization, where knowledge about monotonic properties allows the optimizer to transform certain window function comparisons into more efficient forms, potentially enabling the use of run conditions that can terminate window processing early.

## Parameters / Member Variables
- : The function has no monotonic properties (default)
- : The function is monotonically increasing (values never decrease)
- : The function is monotonically decreasing (values never increase)  
- : The function is both increasing and decreasing (constant function)

## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Used by:
  - SupportRequestWFuncMonotonic struct (as monotonic member)
  - Various window function support functions (row_number, dense_rank, etc.)
  - [Query](../Q/Query.md) planner optimization logic in allpaths.c
  - [int8inc_support](../i/int8inc_support.md) function for increment operations

## Notes and Other Information
- Designed as bitflags to allow combination of INCREASING and DECREASING properties
- Primarily used for window function optimization but applicable to other function types
- Window functions like row_number(), rank(), dense_rank() are marked as MONOTONICFUNC_INCREASING
- The planner uses these properties to optimize window function predicates by converting them to run conditions
- Functions that are MONOTONICFUNC_BOTH are effectively constant functions
- Support functions can query and set these properties through the support function interface
- Enables early termination optimizations in window processing when monotonic functions are compared with constants