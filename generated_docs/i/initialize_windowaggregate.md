# initialize_windowaggregate

## Location
[src/backend/executor/nodeWindowAgg.c:207-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L207-L241)

## Overview
Initializes the state of a window aggregate function, resetting its transition value to the initial state and preparing it for a new computation cycle.

## Definition

```c
static void
initialize_windowaggregate(WindowAggState *winstate,
						   WindowStatePerFunc perfuncstate,
						   WindowStatePerAgg peraggstate)
```
## Detailed Description
This function is parallel to  in nodeAgg.c and is responsible for initializing window aggregate state for a new computation. It handles memory context management carefully, only resetting private aggregate contexts while leaving shared contexts for the caller to manage. The function sets up the transition value either as NULL (if the initial value is NULL) or as a proper copy of the initial value in the appropriate memory context. It also resets counters and result values to prepare for new aggregate computation.

## Parameters / Member Variables
- : The overall window aggregate execution state
- : Per-function state information (currently unused in this function)  
- : Per-aggregate state containing transition values, memory contexts, and initialization data

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [datumCopy](../d/datumCopy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [advance_windowaggregate_base](../a/advance_windowaggregate_base.md)
  - [eval_windowaggregates](../e/eval_windowaggregates.md)

## Notes and Other Information
- The function carefully manages memory contexts, only resetting private contexts to avoid interfering with other aggregates that might share the same context
- When the initial value is not NULL, it creates a proper copy using datumCopy in the aggregate's memory context
- All result state is reset to initial conditions (transValueCount=0, resultValue=0, resultValueIsNull=true)
- This is a critical function in the window aggregate execution pipeline, ensuring clean state initialization