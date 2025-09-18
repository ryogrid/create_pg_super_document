# ExecConditionalAssignProjectionInfo

## Location
src/backend/executor/execUtils.c: 558 - 584

## Overview
Conditionally assigns projection information to a plan state node, optimizing performance by avoiding projection setup when the target list matches the input tuple descriptor exactly.

## Definition
```c
void ExecConditionalAssignProjectionInfo(PlanState *planstate, TupleDesc inputDesc, int varno)
```

## Detailed Description
ExecConditionalAssignProjectionInfo is an optimization-focused variant of ExecAssignProjectionInfo that avoids unnecessary projection setup when no actual projection is needed. The function checks whether the plan's target list exactly matches the input tuple descriptor using tlist_matches_tupdesc().

When the target list matches the input:
- Sets ps_ProjInfo to NULL (no projection needed)
- Copies scan slot operations to result slot operations for efficiency
- This allows direct tuple pass-through without projection overhead

When projection is needed:
- Ensures a result tuple slot exists (creating one with virtual operations if needed)
- Calls ExecAssignProjectionInfo to set up full projection infrastructure

This optimization is particularly valuable for scan nodes where the output often matches the input tuple structure exactly.

## Parameters / Member Variables
- `planstate`: Pointer to the PlanState structure that will receive the projection information
- `inputDesc`: Tuple descriptor for input tuples that will be compared against the target list
- `varno`: Variable number used for matching target list entries against input columns

## Dependencies
- Functions called/Symbols referenced:
  - [tlist_matches_tupdesc](../t/tlist_matches_tupdesc.md) (checks if target list matches input descriptor)
  - [ExecInitResultSlot](ExecInitResultSlot.md) (initializes result slot if needed)
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md) (sets up projection when needed)
  - TTSOpsVirtual (virtual tuple table slot operations)
- Called from (representative examples):
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md) (execScan.c:275)
  - [ExecAssignScanProjectionInfoWithVarno](ExecAssignScanProjectionInfoWithVarno.md) (execScan.c:287)
  - [ExecInitGather](ExecInitGather.md) (nodeGather.c:102)
  - [ExecInitGatherMerge](ExecInitGatherMerge.md) (nodeGatherMerge.c:127)

## Notes and Other Information
- Key optimization that eliminates projection overhead when target list matches input exactly
- Particularly important for scan nodes where projection is often unnecessary
- Sets up result slot operations to match scan slot operations when no projection is needed
- Ensures proper fallback to full projection setup when transformation is required
- The varno parameter helps identify which input columns correspond to target list entries