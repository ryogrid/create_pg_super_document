# WindowStatePerAgg

## Location
[src/include/nodes/execnodes.h:2545-2549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2545-L2549)

## Overview
WindowStatePerAgg is a pointer type that maintains execution state and cached data for plain aggregate functions used as window functions in PostgreSQL's window aggregation implementation.

## Definition


## Detailed Description
WindowStatePerAgg is a pointer to WindowStatePerAggData structure that manages the execution state for plain aggregate functions when they are used in window function contexts. This structure maintains all necessary information for incremental aggregate computation including transition functions, intermediate state values, type information, and memory management. It supports both forward and inverse transition functions for efficient sliding window computation, caching results for current frame boundaries, and managing the transition state throughout the window evaluation process.

## Parameters / Member Variables
The underlying WindowStatePerAggData structure contains:
- : OID of the forward transition function
- : OID of the inverse transition function (may be InvalidOid if not available)
- : OID of the final function (may be InvalidOid for aggregates without final function)
- : FmgrInfo structure for forward transition function lookup data
- : FmgrInfo structure for inverse transition function lookup data
- : FmgrInfo structure for final function lookup data
- : Number of arguments to pass to the final function
- : Initial value from pg_aggregate catalog entry
- : Boolean indicating if initial value is NULL
- : Cached result value for current frame boundaries
- : Boolean indicating if cached result value is NULL
- : Length of input data type for memory operations
- : Length of result data type for memory operations
- : Length of transition data type for memory operations
- : Boolean indicating if input type is passed by value
- : Boolean indicating if result type is passed by value
- : Boolean indicating if transition type is passed by value
- : Index of associated WindowStatePerFuncData structure
- : Memory context for transition value and subsidiary data
- : Current transition value being maintained
- : Boolean indicating if current transition value is NULL
- : Number of rows currently aggregated in transition value
- : Boolean flag indicating if aggregate needs restart in current evaluation cycle

## Dependencies
- Functions called/Symbols referenced:
  - [WindowStatePerAggData](WindowStatePerAggData.md)
  - [FmgrInfo](../F/FmgrInfo.md)
  - [MemoryContext](../M/MemoryContext.md)
  - Datum
- Called from (representative examples):
  - [initialize_windowaggregate](../i/initialize_windowaggregate.md)
  - [advance_windowaggregate](../a/advance_windowaggregate.md)
  - [finalize_windowaggregate](../f/finalize_windowaggregate.md)
  - [eval_windowaggregates](../e/eval_windowaggregates.md)
  - initialize_peragg
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md)

## Notes and Other Information
This structure is specifically designed for plain aggregate functions that are used in window contexts, providing optimization opportunities through inverse transition functions when available. The cached result values and transition state management enable efficient computation over sliding windows without recalculating the entire aggregate for each frame position. The structure supports both simple aggregates and complex ones requiring final functions, with proper memory management through dedicated aggregate contexts.