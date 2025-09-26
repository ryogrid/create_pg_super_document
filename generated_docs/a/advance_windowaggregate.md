# advance_windowaggregate

## Location
[src/backend/executor/nodeWindowAgg.c:242-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L242-L418)

## Overview
Advances a window aggregate function by processing one input tuple, updating the aggregate's transition value according to the aggregate's transition function.

## Definition

```c
static void
advance_windowaggregate(WindowAggState *winstate,
						WindowStatePerFunc perfuncstate,
						WindowStatePerAgg peraggstate)
```
## Detailed Description
This function is parallel to  in nodeAgg.c and handles the core logic for advancing window aggregate computations. It evaluates the aggregate's arguments from the current tuple, handles filtering conditions, manages strict transition functions, and carefully manages memory contexts and data copying. The function includes special handling for moving aggregates by tracking transition value counts and ensuring moving-aggregate transition functions don't return NULL. It also optimizes memory management by detecting when transition functions return pointers to their inputs or expanded objects already in the correct context.

## Parameters / Member Variables
- : The overall window aggregate execution state containing temporary contexts and current aggregate context
- : Per-function state containing the window function expression state, number of arguments, and collation information
- : Per-aggregate state containing transition function info, current transition values, and memory context

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [datumCopy](../d/datumCopy.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - DatumIsReadWriteExpandedObject
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - [MemoryContextGetParent](../M/MemoryContextGetParent.md)
  - [DeleteExpandedObject](../D/DeleteExpandedObject.md)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md)

## Notes and Other Information
- Handles FILTER clauses by evaluating the filter expression and skipping filtered-out tuples
- For strict transition functions, skips processing when any argument is NULL
- Special logic for strict functions with NULL initial values: uses first non-NULL input as initial state  
- Tracks  to support inverse transition functions in moving window aggregates
- Moving-aggregate transition functions must not return NULL (enforced with error)
- Sophisticated memory management including detection of expanded objects already in the correct memory context
- Sets  during transition function calls to support AggCheckCallContext