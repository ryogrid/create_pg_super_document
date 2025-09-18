# WindowStatePerFunc

## Location
[src/include/nodes/execnodes.h:2544-2544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2544-L2544)

## Overview
WindowStatePerFunc is a pointer type representing per-function working state for each window function and window aggregate handled by a WindowAgg execution node.

## Definition


## Detailed Description
WindowStatePerFunc is a pointer to WindowStatePerFuncData structure that maintains execution state and metadata for individual window functions within a WindowAgg node. Each window function or window aggregate processed by the node has its own WindowStatePerFunc instance that tracks function-specific information including the function's metadata, argument details, result type information, and window object context. This structure serves as the bridge between the window function expressions and their runtime execution state, handling both regular window functions and plain aggregate functions used in window contexts.

## Parameters / Member Variables
The underlying WindowStatePerFuncData structure contains:
- : Pointer to WindowFuncExprState containing expression state for this window function
- : Pointer to WindowFunc node containing the original function expression
- : Number of arguments passed to the window function
- : FmgrInfo structure containing function manager lookup data for the window function
- : OID of the collation derived for the window function
- : Length of the result type for memory management operations
- : Boolean indicating if result type is passed by value or reference
- : Boolean flag indicating if this is a plain aggregate function used as window function
- : Index into WindowStatePerAggData array if this is a plain aggregate function
- : WindowObject providing API context for window function execution

## Dependencies
- Functions called/Symbols referenced:
  - [WindowStatePerFuncData](WindowStatePerFuncData.md)
  - [WindowFuncExprState](WindowFuncExprState.md)
  - WindowFunc
  - [FmgrInfo](../F/FmgrInfo.md)
  - WindowObject
- Called from (representative examples):
  - [initialize_windowaggregate](../i/initialize_windowaggregate.md)
  - [advance_windowaggregate](../a/advance_windowaggregate.md)
  - [finalize_windowaggregate](../f/finalize_windowaggregate.md)
  - [eval_windowfunction](../e/eval_windowfunction.md)
  - [begin_partition](../b/begin_partition.md)
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md)

## Notes and Other Information
This structure is essential for PostgreSQL's window function implementation, providing the necessary state management for complex window operations. The distinction between plain aggregates and true window functions is handled through the plain_agg flag, allowing the system to optimize execution for aggregate functions that don't need full window function capabilities. The WindowObject member provides the API interface that window functions use to access frame data and perform window-specific operations.