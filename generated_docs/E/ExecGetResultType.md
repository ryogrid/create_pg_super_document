# ExecGetResultType

## Location
[src/backend/executor/execUtils.c:493-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L493-L501)

## Overview
Returns the result tuple descriptor for a given plan state, providing type information about the tuples that the executor plan node will produce.

## Definition

```c
TupleDesc
ExecGetResultType(PlanState *planstate)
```
## Detailed Description
ExecGetResultType is a simple accessor function that retrieves the result tuple descriptor (TupleDesc) from a plan state node. This function provides a standardized way to access the ps_ResultTupleDesc field, which contains the schema information describing the structure and types of tuples that will be produced by the execution plan node.

The function is commonly used throughout the PostgreSQL executor to determine the expected output format of plan nodes, enabling proper slot initialization, type checking, and result handling in various executor operations.

## Parameters / Member Variables
- `*planstate`: Pointer to the PlanState structure containing the execution state for a plan node
## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md) (indirectly through related operations)
- Called from (representative examples):
  - [ExecComputeSlotInfo](ExecComputeSlotInfo.md) (execExpr.c:2930, 2935, 2950, 2955)
  - [InitPlan](../I/InitPlan.md) (execMain.c:964)
  - [ExecCreateScanSlotFromOuterPlan](ExecCreateScanSlotFromOuterPlan.md) (execUtils.c:667)
  - [initialize_phase](../i/initialize_phase.md) (nodeAgg.c:522)
  - Various node initialization functions across different executor nodes

## Notes and Other Information
- This is a simple getter function with minimal overhead
- The returned TupleDesc should not be modified by the caller
- Used extensively throughout the executor for type information propagation
- Essential for proper slot management and tuple handling in PostgreSQL's execution engine

## Simplified Source

```c
TupleDesc
ExecGetResultType(PlanState *planstate)
{
    return planstate->ps_ResultTupleDesc;
}
```