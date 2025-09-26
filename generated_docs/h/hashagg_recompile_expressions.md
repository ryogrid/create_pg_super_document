# hashagg_recompile_expressions

## Location
[src/backend/executor/nodeAgg.c:1741-1797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1741-L1797)

## Overview
Dynamically recompiles and caches aggregate transition expressions for hash aggregation operations based on current execution context, optimizing performance for different phases of hash aggregation processing.

## Definition
```c
static void hashagg_recompile_expressions(AggState *aggstate, bool minslot, bool nullcheck)
```

## Detailed Description
This function manages the compilation and caching of aggregate transition expressions for hash aggregation. It adapts the compiled expressions based on the current execution state to optimize performance:

1. **Context-Aware Compilation**: Compiles different versions of expressions depending on whether hash aggregation has spilled to disk and whether it's reading from the outer plan or tape
2. **Performance Optimization**: Avoids recompilation by caching compiled expressions in a 2x2 matrix based on `minslot` and `nullcheck` parameters
3. **Null Safety**: Adds null pointer checks when needed (after spilling begins, AggStatePerGroup arrays may be NULL)
4. **Slot Type Adaptation**: Changes outer slot type to minimal tuple slot when reading spilled data from tape

The function supports both AGG_HASHED and AGG_MIXED strategies, selecting the appropriate phase and managing the transition between reading from outer plan vs. spilled data.

## Parameters / Member Variables
- `aggstate`: The aggregate state containing phase information and cached expressions
- `minslot`: Boolean indicating whether to use minimal tuple slots (true when processing spilled batches)
- `nullcheck`: Boolean indicating whether to include null pointer checks in the compiled expression

## Dependencies
- Functions called/Symbols referenced:
  - [AggState](../A/AggState.md)
  - [AggStatePerPhase](../A/AggStatePerPhase.md)
  - AGG_HASHED
  - AGG_MIXED
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md)
  - [ExecBuildAggTrans](../E/ExecBuildAggTrans.md)
- Called from (representative examples):
  - [hash_agg_enter_spill_mode](hash_agg_enter_spill_mode.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)
  - [ExecReScanAgg](../E/ExecReScanAgg.md)

## Notes and Other Information
- Uses a 2x2 cache matrix (evaltrans_cache[i][j]) to store compiled expressions for different execution contexts
- Critical for performance as expression compilation is expensive and this avoids redundant recompilation
- Temporarily modifies the outer plan's slot operations during compilation when processing spilled data
- The function ensures that the correct expression variant is used based on the current aggregation phase and data source