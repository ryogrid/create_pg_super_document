# ExecCreateScanSlotFromOuterPlan

## Location
[src/backend/executor/execUtils.c:659-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L659-L683)

## Overview
Creates and initializes a scan tuple slot for a scan node by deriving the tuple descriptor from its outer plan's result type.

## Definition
```c
void ExecCreateScanSlotFromOuterPlan(EState *estate,
                                   ScanState *scanstate,
                                   const TupleTableSlotOps *tts_ops)
```

## Detailed Description
ExecCreateScanSlotFromOuterPlan is a utility function that initializes a scan tuple slot for nodes that derive their tuple format from an outer (child) plan. This function is commonly used by executor nodes that process tuples from a child node, such as aggregation, sorting, and windowing operations.

The function works by first obtaining the outer plan state from the scan state, then retrieving the result tuple descriptor from that outer plan using ExecGetResultType. Finally, it calls ExecInitScanTupleSlot to create and initialize the scan tuple slot with the derived tuple descriptor and specified slot operations.

This approach ensures that the scan slot is properly configured to handle tuples that match the output format of the child plan, maintaining type consistency throughout the execution tree.

## Parameters / Member Variables
- `estate`: Execution state containing per-query context and memory management information
- `scanstate`: Pointer to the ScanState structure that will receive the initialized scan tuple slot
- `tts_ops`: TupleTableSlotOps structure specifying the operations and implementation type for the tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [ExecGetResultType](ExecGetResultType.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md) (struct type)
  - [ScanState](../S/ScanState.md) (struct type)
- Called from (representative examples):
  - [ExecInitAgg](ExecInitAgg.md)
  - [ExecInitGroup](ExecInitGroup.md)
  - [ExecInitIncrementalSort](ExecInitIncrementalSort.md)
  - [ExecInitMaterial](ExecInitMaterial.md)
  - [ExecInitMemoize](ExecInitMemoize.md)
  - [ExecInitSort](ExecInitSort.md)
  - [ExecInitWindowAgg](ExecInitWindowAgg.md)

## Notes and Other Information
- This function is essential for nodes that process tuples from child plans and need their scan slots to match the child's output format
- The function provides a standardized way to inherit tuple descriptors from outer plans
- Located in src/backend/executor/execUtils.c:659-683
- Widely used across various executor node types that operate on child plan outputs
- The tuple slot operations parameter allows for different slot implementations (heap, minimal, virtual, etc.)