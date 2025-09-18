# advance_aggregates

## Location
src/backend/executor/nodeAgg.c: 816 - 847

## Overview
Advances all aggregate transition states for one input tuple by evaluating transition expressions for both sorted and hashed aggregation modes.

## Definition
```c
static void advance_aggregates(AggState *aggstate)
```

## Detailed Description
This function serves as the main entry point for advancing aggregate computation for a single input tuple. It processes transition states for both sorted aggregation and hashed aggregation simultaneously to avoid redundant evaluation of input expressions. The function operates by evaluating the transition expression stored in the current aggregation phase, which internally handles the logic for updating all relevant aggregate states.

The input tuple is expected to be stored in tmpcontext->ecxt_outertuple before calling this function, making it accessible to expression evaluation. The function uses ExecEvalExprSwitchContext to evaluate the transition expressions in the appropriate memory context.

## Parameters / Member Variables
- `aggstate`: The main aggregate state containing all aggregation context, including the current phase information and temporary context for expression evaluation

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalExprSwitchContext
- Data types used:
  - [AggState](../A/AggState.md)
- Called from (representative examples):
  - [agg_retrieve_direct](agg_retrieve_direct.md)
  - [agg_fill_hash_table](agg_fill_hash_table.md)
  - [agg_refill_hash_table](agg_refill_hash_table.md)

## Notes and Other Information
- The function expects to be called when CurrentMemoryContext is the per-query context
- Input tuple must be pre-loaded in tmpcontext->ecxt_outertuple before calling
- Handles both sorted and hashed aggregation modes in a single pass for efficiency
- The actual transition state updates are delegated to the expression evaluation system through aggstate->phase->evaltrans
- Uses a dummy null flag variable since the return value of the transition expression evaluation is not needed at this level